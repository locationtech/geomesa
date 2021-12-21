/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeParser.{GeoMesaAvroDateFormat, GeoMesaAvroGeomFormat, GeoMesaAvroVisibilityField}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.{InputStream, OutputStream}
import java.net.URL
import java.util.Date
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object ConfluentFeatureSerializer extends LazyLogging {

  def builder(sft: SimpleFeatureType, schemaRegistryUrl: URL): Builder = new Builder(sft, schemaRegistryUrl)

  class Builder private [ConfluentFeatureSerializer] (sft: SimpleFeatureType, schemaRegistryUrl: URL)
      extends SimpleFeatureSerializer.Builder[Builder] {
    override def build(): ConfluentFeatureSerializer = {
      val client = new CachedSchemaRegistryClient(schemaRegistryUrl.toExternalForm, 100)
      new ConfluentFeatureSerializer(sft, client, options.toSet)
    }
  }
}

class ConfluentFeatureSerializer(
    sft: SimpleFeatureType,
    schemaRegistryClient: SchemaRegistryClient,
    val options: Set[SerializationOption] = Set.empty
) extends SimpleFeatureSerializer with LazyLogging {

  private val kafkaAvroDeserializer = new ThreadLocal[KafkaAvroDeserializer]() {
    override def initialValue(): KafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient)
  }

  private val featureReader = new ThreadLocal[ConfluentFeatureReader]() {
    override def initialValue(): ConfluentFeatureReader = {
      val schemaId = Option(sft.getUserData.get(ConfluentMetadata.SchemaIdKey))
        .map(_.asInstanceOf[String].toInt).getOrElse {
          throw new IllegalStateException(s"Cannot create ConfluentFeatureSerializer because SimpleFeatureType " +
            s"'${sft.getTypeName}' does not have schema id at key '${ConfluentMetadata.SchemaIdKey}'")
        }
      val schema = schemaRegistryClient.getById(schemaId)

      new ConfluentFeatureReader(sft, schema)
    }
  }

  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature = {
    val record = kafkaAvroDeserializer.get.deserialize("", bytes).asInstanceOf[GenericRecord]

    val feature = featureReader.get.read(id, record)

    // set the feature visibility if it exists
    Option(sft.getUserData.get(GeoMesaAvroVisibilityField.KEY)).map(_.asInstanceOf[String]).foreach { fieldName =>
      try {
        SecurityUtils.setFeatureVisibility(feature, record.get(fieldName).toString)
      } catch {
        case NonFatal(ex) => throw new IllegalArgumentException(s"Error setting feature visibility using" +
          s"field '$fieldName': ${ex.getMessage}")
      }
    }

    feature
  }

  // implement the following if we need them

  override def deserialize(in: InputStream): SimpleFeature = throw new NotImplementedError()

  override def deserialize(bytes: Array[Byte]): SimpleFeature = throw new NotImplementedError()

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    throw new NotImplementedError()

  override def deserialize(id: String, in: InputStream): SimpleFeature =
    throw new NotImplementedError()

  override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    throw new NotImplementedError()

  override def serialize(feature: SimpleFeature): Array[Byte] = {
    // Strategy:  We have the SFT.
    //  1. Determine Avro schema.
    //  2. Build Avro record
    //  3. serialize with the io.confluent.kafka.serializers.KafkaAvroSerializer

    // TODO:  Cribbed code / refactor
    val schemaId = Option(sft.getUserData.get(ConfluentMetadata.SchemaIdKey))
      .map(_.asInstanceOf[String].toInt).getOrElse {
      throw new IllegalStateException(s"Cannot create ConfluentFeatureSerializer because SimpleFeatureType " +
        s"'${sft.getTypeName}' does not have schema id at key '${ConfluentMetadata.SchemaIdKey}'")
    }
    val schema: Schema = schemaRegistryClient.getById(schemaId)

    val record: GenericData.Record = new GenericData.Record(schema)
    schema.getFields.asScala.foreach { field =>
      val name = field.name()
      val ad = sft.getDescriptor(name)

      if (ad != null) {
        println(s"Handling $name with type ${ad.getType.getBinding}")
        if (ad.getType.getBinding.isAssignableFrom(classOf[Geometry]) ||
          classOf[Geometry].isAssignableFrom(ad.getType.getBinding)) {
          println("Doing geometry handling!")
          // Handling the geometry field!
          // TODO:  Add handling for WKB by reading the metadata
          val geom = feature.getAttribute(name).asInstanceOf[Geometry]
          record.put(name, WKTUtils.write(geom))
        } else if (ad.getType.getBinding.isAssignableFrom(classOf[java.util.Date]) ||
          classOf[java.util.Date].isAssignableFrom(ad.getType.getBinding)) {
          val date: Date = feature.getAttribute(name).asInstanceOf[java.util.Date]
          val formatter: DateTimeFormatter = ISODateTimeFormat.dateTime()
          record.put(name, formatter.print(date.getTime))  // JNH: This may not be formatted properly
        } else {
          record.put(name, feature.getAttribute(name))
        }
      } else {
        // TODO: Fix this!
        // This is to handle "visibility" / missing fields
        record.put(name, "")
      }
    }

//    sft.getAttributeDescriptors.asScala.foreach { ad =>
//      val name = ad.getLocalName
//      println(s"Handling $name with type ${ad.getType.getBinding}")
//      if (ad.getType.getBinding.isAssignableFrom(classOf[Geometry]) ||
//        classOf[Geometry].isAssignableFrom(ad.getType.getBinding)) {
//        println("Doing geometry handling!")
//        // Handling the geometry field!
//        // TODO:  Add handling for WKB by reading the metadata
//        val geom = feature.getAttribute(name).asInstanceOf[Geometry]
//        record.put(name, WKTUtils.write(geom))
//      } else if (ad.getType.getBinding.isAssignableFrom(classOf[java.util.Date]) ||
//        classOf[java.util.Date].isAssignableFrom(ad.getType.getBinding)) {
//        val date: Date = feature.getAttribute(name).asInstanceOf[java.util.Date]
//        val parser: DateTimeFormatter = ISODateTimeFormat.dateTimeParser()
//        record.put(name, date.toString)  // JNH: This may not be formatted properly
//      } else {
//        record.put(name, feature.getAttribute(name))
//      }
//    }

    val kas = new KafkaAvroSerializer(schemaRegistryClient)
    // TODO: Wire through topic instead of "confluent-kds-test"
    kas.serialize("confluent-kds-test", record)
  }

  override def serialize(feature: SimpleFeature, out: OutputStream): Unit =
    throw new NotImplementedError("ConfluentSerializer is read-only")

  // precompute the deserializer for each field in the SFT to simplify the actual deserialization
  private class ConfluentFeatureReader(sft: SimpleFeatureType, schema: Schema) {

    def read(id: String, record: GenericRecord): SimpleFeature = {
      val attributes = fieldReaders.map { fieldReader =>
        try {
          Option(record.get(fieldReader.fieldName)).map(fieldReader.reader.apply).orNull
        } catch {
          case NonFatal(ex) =>
            throw ConfluentFeatureReader.DeserializationException(fieldReader.fieldName, fieldReader.clazz, ex)
        }
      }

      ScalaSimpleFeature.create(sft, id, attributes: _*)
    }

    private val fieldReaders: Seq[ConfluentFeatureReader.FieldReader[_]] = {
      sft.getAttributeDescriptors.asScala.map { descriptor =>
        val fieldName = descriptor.getLocalName

        val reader =
          if (classOf[Geometry].isAssignableFrom(descriptor.getType.getBinding)) {
            GeoMesaAvroGeomFormat.getFieldReader(schema, fieldName)
          } else if (classOf[Date].isAssignableFrom(descriptor.getType.getBinding)) {
            GeoMesaAvroDateFormat.getFieldReader(schema, fieldName)
          } else {
            value: AnyRef => value
          }

        ConfluentFeatureReader.FieldReader(fieldName, reader, descriptor.getType.getBinding)
      }
    }
  }

  private object ConfluentFeatureReader {

    private case class FieldReader[T](fieldName: String, reader: AnyRef => T, clazz: Class[_])

    final case class DeserializationException(fieldName: String, clazz: Class[_], t: Throwable)
      extends RuntimeException(s"Cannot deserialize field '$fieldName' into a '${clazz.getName}': ${t.getMessage}")
  }
}
