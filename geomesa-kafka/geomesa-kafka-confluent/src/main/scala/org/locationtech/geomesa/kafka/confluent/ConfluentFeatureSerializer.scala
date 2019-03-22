/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.features.{ScalaSimpleFeature, SimpleFeatureSerializer}
import org.locationtech.geomesa.security.SecurityUtils
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.locationtech.jts.geom.Geometry
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

object ConfluentFeatureSerializer {

  val GeomAttributeName = "_geom"
  val DateAttributeName = "_date"
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

  private var geomSrcAttributeName: Option[String] = None

  def deserialize(id: String, bytes: Array[Byte], date: Date): SimpleFeature = {
    val genericRecord = kafkaAvroDeserializer.get.deserialize("", bytes).asInstanceOf[GenericRecord]
    val attrs = sft.getAttributeDescriptors.asScala.map(_.getLocalName).map {
      case ConfluentFeatureSerializer.GeomAttributeName =>
        geomSrcAttributeName match {
          case None =>
            // Here we find a valid geom field in the first record or throw.
            val kv = sft.getAttributeDescriptors.asScala
                .iterator
                .map(_.getLocalName)
                .map(n => n -> readFieldAsWkt(genericRecord, n, logFailure = false))
                .collectFirst { case (k, Some(v)) => k -> v }
                .getOrElse {
                  throw new UnsupportedOperationException("No valid WKT field found in avro data for " +
                      s"ConfluentFeatureSerializer in first record $genericRecord.  Valid Geometry field is required.")
                }
            geomSrcAttributeName = Option(kv._1)
            kv._2

          case Some(name) => readFieldAsWkt(genericRecord, name, logFailure = true).get
        }

      case ConfluentFeatureSerializer.DateAttributeName => date
      case name => genericRecord.get(name)
    }
    val sf = ScalaSimpleFeature.create(sft, id, attrs: _*)
    if (visAttributeIndex != -1) {
      SecurityUtils.setFeatureVisibility(sf, sf.getAttribute(visAttributeIndex).asInstanceOf[String])
    }
    sf
  }

  override def deserialize(id: String, bytes: Array[Byte]): SimpleFeature = deserialize(id, bytes, null)

  private def readFieldAsWkt(record: GenericRecord, fieldName: String, logFailure: Boolean): Option[Geometry] = {
    try { Option(WKTUtils.read(record.get(fieldName).toString)) } catch {
      case NonFatal(t) =>
        if (logFailure) {
          logger.error(s"Error parsing wkt from field $fieldName with value ${record.get(fieldName)} " +
              s"for sft ${sft.getTypeName}")
        }
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

