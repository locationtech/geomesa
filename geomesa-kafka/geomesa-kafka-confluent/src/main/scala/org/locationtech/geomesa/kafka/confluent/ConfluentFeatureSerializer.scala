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
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeParser.{GeomesaAvroDateFormat, GeomesaAvroFeatureVisibility, GeomesaAvroGeomFormat}
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._

object ConfluentFeatureSerializer {

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

  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature = {
    val record = kafkaAvroDeserializer.get.deserialize("", bytes).asInstanceOf[GenericRecord]

    val attributes = sft.getAttributeDescriptors.asScala.map { descriptor =>
      val fieldName = descriptor.getLocalName

      if (classOf[Geometry].isAssignableFrom(descriptor.getType.getBinding)) {
        GeomesaAvroGeomFormat.deserialize(record, fieldName)
      } else if (classOf[Date].isAssignableFrom(descriptor.getType.getBinding)) {
        GeomesaAvroDateFormat.deserialize(record, fieldName)
      } else {
        record.get(fieldName)
      }
    }

    val sf = ScalaSimpleFeature.create(sft, id, attributes: _*)

    // set the feature visibility if it exists
    Option(sft.getUserData.get(GeomesaAvroFeatureVisibility.KEY)).map { fieldName =>
      SecurityUtils.setFeatureVisibility(sf, record.get(fieldName.asInstanceOf[String]).toString)
    }

    sf
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

  override def serialize(feature: SimpleFeature): Array[Byte] =
    throw new NotImplementedError("ConfluentSerializer is read-only")

  override def serialize(feature: SimpleFeature, out: OutputStream): Unit =
    throw new NotImplementedError("ConfluentSerializer is read-only")
}

