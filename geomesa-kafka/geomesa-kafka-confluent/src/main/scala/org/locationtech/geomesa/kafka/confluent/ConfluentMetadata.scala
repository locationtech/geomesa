/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureUtils
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class ConfluentMetadata(val schemaRegistry: SchemaRegistryClient) extends GeoMesaMetadata[String] with LazyLogging {

  import ConfluentMetadata.SubjectPostfix

  private val topicSftCache: LoadingCache[String, String] =
    Caffeine.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build(
      new CacheLoader[String, String] {
        override def load(topic: String): String = {
          try {
            val subject = topic + SubjectPostfix
            val schemaId = schemaRegistry.getLatestSchemaMetadata(subject).getId
            val geom = Some(ConfluentFeatureSerializer.GeomAttributeName)
            val dtg = Some(ConfluentFeatureSerializer.DateAttributeName)
            val sft = AvroSimpleFeatureUtils.schemaToSft(schemaRegistry.getByID(schemaId), topic, geom, dtg)
            KafkaDataStore.setTopic(sft, topic)
            SimpleFeatureTypes.encodeType(sft, includeUserData = true)
          } catch {
            case NonFatal(e) => logger.error("Error retrieving schema from confluent registry:", e); null
          }
        }
      }
    )

  override def getFeatureTypes: Array[String] = {
    val types = schemaRegistry.getAllSubjects.asScala.collect {
      case s if s.endsWith(SubjectPostfix) => s.substring(0, s.lastIndexOf(SubjectPostfix))
    }
    types.toArray
  }

  override def read(typeName: String, key: String, cache: Boolean): Option[String] = {
    if (key != GeoMesaMetadata.ATTRIBUTES_KEY) {
      logger.warn(s"Requested read on ConfluentMetadata with unsupported key $key. " +
        s"ConfluentMetadata only supports ${GeoMesaMetadata.ATTRIBUTES_KEY}")
      None
    } else {
      if (!cache) {
        topicSftCache.invalidate(typeName)
      }
      Option(topicSftCache.get(typeName))
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
  override def remove(typeName: String, keys: Seq[String]): Unit = {}
  override def delete(typeName: String): Unit = {}
}

object ConfluentMetadata extends LazyLogging {

  // Currently hard-coded to the default confluent uses (<topic>-value).  See the following documentation:
  //   https://docs.confluent.io/current/schema-registry/docs/serializer-formatter.html#subject-name-strategy
  val SubjectPostfix = "-value"
}
