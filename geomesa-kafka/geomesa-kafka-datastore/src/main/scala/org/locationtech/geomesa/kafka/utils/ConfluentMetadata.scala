/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.utils

import java.util.Date
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Geometry
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.features.confluent.ConfluentFeatureSerializer
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.utils.ConfluentMetadata.{schemaToSft, SUBJECT_POSTFIX}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class ConfluentMetadata(val schemaRegistry: SchemaRegistryClient) extends GeoMesaMetadata[String] with LazyLogging {

  val topicSftCache: LoadingCache[String, String] =
    Caffeine.newBuilder()
      .expireAfterWrite(10, TimeUnit.MINUTES)
      .build(
        new CacheLoader[String, String] {
          def load(topic: String): String = getSftSpecForTopic(topic).orNull
        }
      )

  protected def getSftSpecForTopic(topic: String): Option[String] =
    try {
      val subject = topic + SUBJECT_POSTFIX
      val schemaId = schemaRegistry.getLatestSchemaMetadata(subject).getId
      val sft = schemaToSft(schemaRegistry.getByID(schemaId), topic) // todo: any restrictions on sftName not on topic?
      KafkaDataStore.setTopic(sft, topic)
      Option(SimpleFeatureTypes.encodeType(sft, includeUserData = true))
    } catch {
      case NonFatal(t) =>
        logger.error("Error retrieving schema from confluent registry.  Returning None.", t)
        None
    }

  override def getFeatureTypes: Array[String] = schemaRegistry.getAllSubjects.asScala
                                                              .filter(_.endsWith(SUBJECT_POSTFIX))
                                                              .map(s => s.substring(0, s.lastIndexOf(SUBJECT_POSTFIX)))
                                                              .toArray

  override def read(typeName: String, key: String, cache: Boolean): Option[String] = {
    if (key != GeoMesaMetadata.ATTRIBUTES_KEY) {
      logger.warn(s"Requested read on ConfluentMetadata with unsupported key $key. " +
        s"ConfluentMetadata only supports ${GeoMesaMetadata.ATTRIBUTES_KEY}")
      None
    } else {
      if (!cache) {
        getSftSpecForTopic(typeName)
      } else {
        Option(topicSftCache.get(typeName))
      }
    }
  }

  override def invalidateCache(typeName: String, key: String): Unit = {
    if (key != GeoMesaMetadata.ATTRIBUTES_KEY) {
      logger.warn(s"Requested invalidate cache on ConfluentMetadata with unsupported key $key. " +
        s"ConfluentMetadata only supports ${GeoMesaMetadata.ATTRIBUTES_KEY}")
    } else {
      topicSftCache.invalidate(typeName)
    }
  }

  override def close(): Unit = {}

  override def scan(typeName: String, prefix: String, cache: Boolean): Seq[(String, String)] =
    throw new NotImplementedError(s"ConfluentMetadata only supports ATTRIBUTES_KEY")

  override def insert(typeName: String, key: String, value: String): Unit = {}
  override def insert(typeName: String, kvPairs: Map[String, String]): Unit = {}
  override def remove(typeName: String, key: String): Unit = {}
  override def delete(typeName: String): Unit = {}
}

object ConfluentMetadata extends LazyLogging {
  val SUBJECT_POSTFIX = "-value"

  def schemaToSft(schema: Schema, sftName: String): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder
    builder.setName(sftName)
    builder.setDefaultGeometry(ConfluentFeatureSerializer.geomAttributeName)
    builder.add(ConfluentFeatureSerializer.geomAttributeName, classOf[Geometry])
    builder.add(ConfluentFeatureSerializer.dateAttributeName, classOf[Date])
    schema.getFields.asScala.foreach(addSchemaToBuilder(builder, _))
    builder.buildFeatureType()
  }

  def addSchemaToBuilder(builder: SimpleFeatureTypeBuilder,
                         field: Schema.Field,
                         typeOverride: Option[Schema.Type] = None): Unit = {
    typeOverride.getOrElse(field.schema().getType) match {
      case STRING => builder.add(field.name(), classOf[java.lang.String])
      case BOOLEAN => builder.add(field.name(), classOf[java.lang.Boolean])
      case INT => builder.add(field.name(), classOf[java.lang.Integer])
      case DOUBLE => builder.add(field.name(), classOf[java.lang.Double])
      case LONG => builder.add(field.name(), classOf[java.lang.Long])
      case FLOAT => builder.add(field.name(), classOf[java.lang.Float])
      case BYTES => logger.error("Avro schema requested BYTES, which is not yet supported") //todo: support
      case UNION => field.schema().getTypes.asScala.map(_.getType).find(_ != NULL)
                         .foreach(t => addSchemaToBuilder(builder, field, Option(t))) //todo: support more union types and log any errors better
      case MAP => logger.error("Avro schema requested MAP, which is not yet supported") //todo: support
      case RECORD => logger.error("Avro schema requested RECORD, which is not yet supported") //todo: support
      case ENUM => builder.add(field.name(), classOf[java.lang.String])
      case ARRAY => logger.error("Avro schema requested ARRAY, which is not yet supported") //todo: support
      case FIXED => logger.error("Avro schema requested FIXED, which is not yet supported") //todo: support
      case NULL => logger.error("Avro schema requested NULL, which is not yet supported") //todo: support
      case _ => logger.error(s"Avro schema requested unknown type ${field.schema().getType}")
    }
  }
}
