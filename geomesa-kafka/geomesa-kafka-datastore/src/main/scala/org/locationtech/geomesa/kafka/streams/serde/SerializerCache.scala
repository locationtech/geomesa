/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.streams.serde

import org.geotools.data.DataStoreFinder
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.streams.HasTopicMetadata
import org.locationtech.geomesa.utils.io.WithClose

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ArrayBuffer

/**
 * Cache for serializers and topic names
 *
 * @param params data store params
 */
class SerializerCache(params: java.util.Map[String, _]) extends HasTopicMetadata {

  private val metadataByTypeName = new ConcurrentHashMap[String, SchemaMetadata]()
  private val serializersByTopic = new ConcurrentHashMap[String, GeoMesaMessageSerializer]()

  private val metadataLoader = new java.util.function.Function[String, SchemaMetadata]() {
    override def apply(typeName: String): SchemaMetadata = loadMetadata(typeName)
  }

  private val serializerLoader = new java.util.function.Function[String, GeoMesaMessageSerializer]() {
    override def apply(topic: String): GeoMesaMessageSerializer = loadSerializer(topic)
  }

  // track last-used serializer so we don't have to look them up by hash each
  // time if we're just reading/writing to one topic (which is the standard use-case)
  @volatile
  private var last: (String, GeoMesaMessageSerializer) = ("", null)

  override def topic(typeName: String): String = metadataByTypeName.computeIfAbsent(typeName, metadataLoader).topic

  override def usesDefaultPartitioning(typeName: String): Boolean =
    metadataByTypeName.computeIfAbsent(typeName, metadataLoader).usesDefaultPartitioning

  /**
   * Gets the serializer associated with a topic
   *
   * @param topic kafka topic name
   * @return
   */
  def serializer(topic: String): GeoMesaMessageSerializer = {
    val (lastTopic, lastSerializer) = last
    if (lastTopic == topic) { lastSerializer } else {
      val serializer = serializersByTopic.computeIfAbsent(topic, serializerLoader)
      // should be thread-safe due to volatile
      last = (topic, serializer)
      serializer
    }
  }

  private def loadMetadata(typeName: String): SchemaMetadata = {
    withDataStore { ds =>
      ds.getSchema(typeName) match {
        case sft => SchemaMetadata(KafkaDataStore.topic(sft), KafkaDataStore.usesDefaultPartitioning(sft))
        case null =>
          throw new IllegalArgumentException(
            s"Schema '$typeName' does not exist in the configured store. " +
                s"Available schemas: ${ds.getTypeNames.mkString(", ")}")
      }
    }
  }

  private def loadSerializer(topic: String): GeoMesaMessageSerializer = {
    withDataStore { ds =>
      val topics = ArrayBuffer.empty[String]
      // order so that we check the most likely ones first
      val typeNames = ds.getTypeNames.partition(_.contains(topic)) match {
        case (left, right) => left ++ right
      }
      var i = 0
      while (i < typeNames.length) {
        val sft = ds.getSchema(typeNames(i))
        KafkaDataStore.topic(sft) match {
          case t if t == topic =>
            val internal = ds.serialization(sft, ds.config.serialization, `lazy` = false).serializer
            return new GeoMesaMessageSerializer(sft, internal)

          case t => topics += t
        }
        i += 1
      }
      throw new IllegalArgumentException(
        s"Topic '$topic' does not exist in the configured store. Available topics: ${topics.mkString(", ")}")
    }
  }

  private def withDataStore[T](fn: KafkaDataStore => T): T = {
    WithClose(DataStoreFinder.getDataStore(params)) {
      case ds: KafkaDataStore => fn(ds)
      case null => throw new IllegalArgumentException("Could not load data store with provided params")
      case ds => throw new IllegalArgumentException(s"Expected a KafkaDataStore but got ${ds.getClass.getName}")
    }
  }

  private case class SchemaMetadata(topic: String, usesDefaultPartitioning: Boolean)
}
