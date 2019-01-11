/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.utils

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureUtils
import org.locationtech.geomesa.features.confluent.ConfluentFeatureSerializer
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.utils.ConfluentMetadata.SUBJECT_POSTFIX
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class ConfluentMetadata(val schemaRegistry: SchemaRegistryClient) extends GeoMesaMetadata[String] with LazyLogging {

  private val topicSftCache: LoadingCache[String, String] =
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
      val sft = AvroSimpleFeatureUtils.schemaToSft(schemaRegistry.getByID(schemaId),
                                                   topic,
                                                   Some(ConfluentFeatureSerializer.geomAttributeName),
                                                   Some(ConfluentFeatureSerializer.dateAttributeName))
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

  // Currently hard-coded to the default confluent uses (<topic>-value).  See the following documentation:
  //   https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#subject-name-strategy
  val SUBJECT_POSTFIX = "-value"
}
