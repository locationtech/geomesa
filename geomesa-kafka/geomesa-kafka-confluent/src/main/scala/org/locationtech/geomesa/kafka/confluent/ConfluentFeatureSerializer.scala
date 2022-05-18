/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeParser.{GeoMesaAvroDateFormat, GeoMesaAvroGeomFormat, GeoMesaAvroVisibilityField}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import java.io.{InputStream, OutputStream}
import java.net.URL
import java.util.Date
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object ConfluentFeatureSerializer {

  def builder(sft: SimpleFeatureType, schemaRegistryUrl: URL, schemaOverride: Option[Schema] = None): Builder =
    new Builder(sft, schemaRegistryUrl, schemaOverride)

  class Builder private[ConfluentFeatureSerializer](
    sft: SimpleFeatureType,
    schemaRegistryUrl: URL,
    schemaOverride: Option[Schema] = None
  ) extends SimpleFeatureSerializer.Builder[Builder] {
    override def build(): ConfluentFeatureSerializer = {
      val client = new CachedSchemaRegistryClient(schemaRegistryUrl.toExternalForm, 100)
      new ConfluentFeatureSerializer(sft, client, schemaOverride, options.toSet)
    }
  }
}

class ConfluentFeatureSerializer(
    sft: SimpleFeatureType,
    schemaRegistryClient: SchemaRegistryClient,
    schemaOverride: Option[Schema] = None,
    val options: Set[SerializationOption] = Set.empty
  ) extends SimpleFeatureSerializer with LazyLogging {

  private val kafkaAvroDeserializers = new ThreadLocal[KafkaAvroDeserializer]() {
    override def initialValue(): KafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient)
  }

  private val featureReaders = new ThreadLocal[ConfluentFeatureReader]() {
    override def initialValue(): ConfluentFeatureReader = {
      val schema = schemaOverride.getOrElse {
        val schemaId = Option(sft.getUserData.get(ConfluentMetadata.SchemaIdKey))
          .map(_.asInstanceOf[String].toInt).getOrElse {
          throw new IllegalStateException(s"Cannot create ConfluentFeatureSerializer because SimpleFeatureType " +
            s"'${sft.getTypeName}' does not have schema id at key '${ConfluentMetadata.SchemaIdKey}'")
        }
        schemaRegistryClient.getById(schemaId)
      }

      new ConfluentFeatureReader(sft, schema)
    }
  }

  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature = {
    val record = kafkaAvroDeserializers.get.deserialize("", bytes).asInstanceOf[GenericRecord]

    val feature = featureReaders.get.read(id, record)

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

  override def deserialize(bytes: Array[Byte]): SimpleFeature = deserialize("", bytes)

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    deserialize("", bytes, offset, length)

  override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature = {
    val buf = if (offset == 0 && length == bytes.length) { bytes } else {
      val buf = Array.ofDim[Byte](length)
      System.arraycopy(bytes, offset, buf, 0, length)
      buf
    }
    deserialize(id, buf)
  }

  // implement the following if we need them

  override def deserialize(in: InputStream): SimpleFeature = throw new NotImplementedError()

  override def deserialize(id: String, in: InputStream): SimpleFeature =
    throw new NotImplementedError()

  override def serialize(feature: SimpleFeature): Array[Byte] =
    throw new NotImplementedError("ConfluentSerializer is read-only")

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
