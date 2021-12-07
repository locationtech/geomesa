/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import java.io.{InputStream, OutputStream}
import java.net.URL
import java.util.Date
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.{CachedSchemaRegistryClient, SchemaRegistryClient}
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeParser.{GeomesaAvroDateFormat, GeomesaAvroGeomFormat}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object ConfluentFeatureSerializer {

  val VisAttributeName  = "visibilities"

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

  private val visAttributeIndex = sft.indexOf(ConfluentFeatureSerializer.VisAttributeName)

  private val kafkaAvroDeserializer = new ThreadLocal[KafkaAvroDeserializer]() {
    override def initialValue(): KafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient)
  }

  def deserialize(id: String, bytes: Array[Byte], date: Date): SimpleFeature = {
    val record = kafkaAvroDeserializer.get.deserialize("", bytes).asInstanceOf[GenericRecord]

    val attributes = sft.getAttributeDescriptors.asScala.map { descriptor =>
      val fieldName = descriptor.getLocalName
      val userData = descriptor.getUserData
      if (descriptor.getType.getBinding.isAssignableFrom(classOf[Geometry])) {
        Option(userData.get(GeomesaAvroGeomFormat.KEY).asInstanceOf[String]).map { property =>
          GeomesaAvroGeomFormat.deserialize(record, fieldName, property)
        }
      } else if (descriptor.getType.getBinding.isAssignableFrom(classOf[Date])) {
        Option(userData.get(GeomesaAvroDateFormat.KEY).asInstanceOf[String]).map { property =>
          GeomesaAvroDateFormat.deserialize(record, fieldName, property)
        }
      } else {
        record.get(fieldName)
      }
    }

    val feature = ScalaSimpleFeature.create(sft, id, attributes: _*)
    if (visAttributeIndex != -1) {
      SecurityUtils.setFeatureVisibility(feature, feature.getAttribute(visAttributeIndex).asInstanceOf[String])
    }

    feature
  }

  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature = deserialize(id, bytes, null)

  private def readGeometryField(record: GenericRecord, fieldName: String)
                               (read: String => Geometry): Option[Geometry] = {
    try {
      Option(read(record.get(fieldName).toString))
    } catch {
      case NonFatal(_) =>
        logger.error(s"Error parsing WKB from field '$fieldName' with value '${record.get(fieldName)}' " +
          s"for sft '${sft.getTypeName}'")
        None
    }
  }

  // Implement the following if we find we need them

  override def deserialize(in: InputStream): SimpleFeature = throw new NotImplementedError()

  override def deserialize(bytes: Array[Byte]): SimpleFeature = throw new NotImplementedError()

  override def deserialize(bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    throw new NotImplementedError()

  override def deserialize(id: String, in: InputStream): SimpleFeature =
    throw new NotImplementedError()

  override def deserialize(id: String, bytes: Array[Byte], offset: Int, length: Int): SimpleFeature =
    throw new NotImplementedError()

  override def serialize(feature: SimpleFeature): Array[Byte] =
    throw new NotImplementedError("ConfluentSerializer is read-only")

  override def serialize(feature: SimpleFeature, out: OutputStream): Unit =
    throw new NotImplementedError("ConfluentSerializer is read-only")
}

