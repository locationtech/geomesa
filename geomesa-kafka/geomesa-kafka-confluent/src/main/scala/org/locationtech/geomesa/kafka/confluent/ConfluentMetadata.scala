/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.confluent

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.locationtech.geomesa.index.metadata.GeoMesaMetadata
import org.locationtech.geomesa.kafka.confluent.ConfluentMetadata._
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

class ConfluentMetadata(schemaRegistry: SchemaRegistryClient, sftOverrides: Map[String, SimpleFeatureType] = Map.empty)
  extends GeoMesaMetadata[String] with LazyLogging {

  private val topicSftCache: LoadingCache[String, String] = {
    Caffeine.newBuilder().expireAfterWrite(10, TimeUnit.MINUTES).build(
      new CacheLoader[String, String] {
        override def load(topic: String): String = {
          try {
            // use the overridden sft for the topic if it exists, else look it up in the registry
            val sft = sftOverrides.getOrElse(topic, {
              val subject = topic + SubjectPostfix
              val schemaId = schemaRegistry.getLatestSchemaMetadata(subject).getId
              val sft = SchemaParser.schemaToSft(schemaRegistry.getById(schemaId))
              // store the schema id to access the schema when creating the feature serializer
              sft.getUserData.put(SchemaIdKey, schemaId.toString)
              sft
            })
            KafkaDataStore.setTopic(sft, topic)
            SimpleFeatureTypes.encodeType(sft, includeUserData = true)
          } catch {
            case NonFatal(e) => logger.error("Error retrieving schema from confluent registry: ", e); null
          }
        }
      }
    )
  }

  override def getFeatureTypes: Array[String] = {
    schemaRegistry.getAllSubjects.asScala.collect {
      case s: String if s.endsWith(SubjectPostfix) => s.substring(0, s.lastIndexOf(SubjectPostfix))
    }.toArray
  }

  override def read(typeName: String, key: String, cache: Boolean): Option[String] = {
    if (key == GeoMesaMetadata.AttributesKey) {
      if (!cache) {
        topicSftCache.invalidate(typeName)
      }
      Option(topicSftCache.get(typeName))
    } else if (typeName == "migration" && key == "check") {
      // skip metadata migration check since there's nothing to migrate
      Some("true")
    } else {
      logger.warn(
        s"Requested read on ConfluentMetadata with unsupported key $key. " +
            s"ConfluentMetadata only supports ${GeoMesaMetadata.AttributesKey}")
      None
    }
  }

  override def invalidateCache(typeName: String, key: String): Unit = {
    if (key != GeoMesaMetadata.AttributesKey) {
      logger.warn(s"Requested invalidate cache on ConfluentMetadata with unsupported key $key. " +
        s"ConfluentMetadata only supports ${GeoMesaMetadata.AttributesKey}")
    } else {
      topicSftCache.invalidate(typeName)
    }
  }

  override def close(): Unit = {}

  override def scan(typeName: String, prefix: String, cache: Boolean): Seq[(String, String)] =
    throw new NotImplementedError(s"ConfluentMetadata only supports ${GeoMesaMetadata.AttributesKey}")

  override def insert(typeName: String, key: String, value: String): Unit = {}
  override def insert(typeName: String, kvPairs: Map[String, String]): Unit = {}
  override def remove(typeName: String, key: String): Unit = {}
  override def remove(typeName: String, keys: Seq[String]): Unit = {}
  override def delete(typeName: String): Unit = {}
  override def backup(typeName: String): Unit = {}
  override def resetCache(): Unit = {}
}

object ConfluentMetadata {

  // hardcoded to the default confluent uses (<topic>-value)
  val SubjectPostfix = "-value"

  // key in user data where avro schema id is stored
  val SchemaIdKey = "geomesa.avro.schema.id"
}
