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
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.locationtech.geomesa.features.SerializationOption.SerializationOption
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureTypeParser.{GeomesaAvroDateFormat, GeomesaAvroFeatureVisibility, GeomesaAvroGeomFormat, GeomesaAvroProperty}
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

    var visibilityAttributeName: Option[String] = None

    val attributes = sft.getAttributeDescriptors.asScala.map { descriptor =>
      val fieldName = descriptor.getLocalName
      val userData = descriptor.getUserData

      if (classOf[Geometry].isAssignableFrom(descriptor.getType.getBinding)) {
        Option(userData.get(GeomesaAvroGeomFormat.KEY).asInstanceOf[String]).map { format =>
          GeomesaAvroGeomFormat.deserialize(record, fieldName, format)
        }.getOrElse {
          throw GeomesaAvroProperty.MissingPropertyValueException[Geometry](fieldName, GeomesaAvroGeomFormat.KEY)
        }
      } else if (classOf[Date].isAssignableFrom(descriptor.getType.getBinding)) {
        Option(userData.get(GeomesaAvroDateFormat.KEY).asInstanceOf[String]).map { format =>
          GeomesaAvroDateFormat.deserialize(record, fieldName, format)
        }.getOrElse {
          throw GeomesaAvroProperty.MissingPropertyValueException[Date](fieldName, GeomesaAvroDateFormat.KEY)
        }
      } else {
        if (classOf[String].isAssignableFrom(descriptor.getType.getBinding)
            && userData.containsKey(GeomesaAvroFeatureVisibility.KEY)) {
          // if there is more than one visibility attribute, the last one will be used
          visibilityAttributeName = Some(descriptor.getLocalName)
        }
        record.get(fieldName)
      }
    }

    val feature = ScalaSimpleFeature.create(sft, id, attributes: _*)

    visibilityAttributeName.map { name =>
      SecurityUtils.setFeatureVisibility(feature, feature.getAttribute(name).asInstanceOf[String])
    }

    println("Feature: " + feature)

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

  override def serialize(feature: SimpleFeature): Array[Byte] =
    throw new NotImplementedError("ConfluentSerializer is read-only")

  override def serialize(feature: SimpleFeature, out: OutputStream): Unit =
    throw new NotImplementedError("ConfluentSerializer is read-only")
}

