/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.utils

import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.kafka.utils.GeoMessage.{Change, Clear, Delete}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Random
import scala.util.control.NonFatal
import scala.util.hashing.MurmurHash3

/**
  * Serialized `GeoMessage`s
  *
  * Current encoding (version 2), designed to work with kafka log compaction.
  * See https://kafka.apache.org/10/documentation.html#design_compactionbasics
  *
  * change:
  *   key: 1 byte message version, n bytes feature id
  *   value: n bytes for serialized feature (without id)
  *
  * delete:
  *   key: 1 byte message version, n bytes feature id
  *   value: null - this allows for log compaction to delete the feature out
  *
  * clear:
  *   key: 1 byte message version
  *   value: empty
  *
  *
  * Version 1 legacy encoding:
  *
  * change:
  *   key: 1 byte message version, 1 byte for message type ('C'), 8 byte long for epoch millis
  *   value: n bytes for serialized feature (with id)
  *
  * delete:
  *   key: 1 byte message version, 1 byte for message type ('D'), 8 byte long for epoch millis
  *   value: n bytes for feature id
  *
  * clear:
  *   key: 1 byte message version, 1 byte for message type ('X'), 8 byte long for epoch millis
  *   value: empty
  */
object GeoMessageSerializer {

  val Version: Byte = 2

  private val Empty = Array.empty[Byte]

  /**
    * Ensures that updates to a given feature go to the same partition, so that they maintain order
    */
  class GeoMessagePartitioner extends Partitioner {

    override def partition(topic: String,
                           key: Any,
                           keyBytes: Array[Byte],
                           value: Any,
                           valueBytes: Array[Byte],
                           cluster: Cluster): Int = {
      val count = cluster.partitionsForTopic(topic).size

      try {
        // use the feature id if available, otherwise (for clear) use random shard
        if (keyBytes.length > 1) {
          Math.abs(MurmurHash3.bytesHash(keyBytes)) % count
        } else {
          Random.nextInt(count)
        }
      } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(
            s"Unexpected message format: ${Option(keyBytes).map(ByteArrays.toHex).getOrElse("")} " +
                s"${Option(valueBytes).map(ByteArrays.toHex).getOrElse("")}", e)
      }

    }

    override def configure(configs: java.util.Map[String, _]): Unit = {}

    override def close(): Unit = {}
  }
}

/**
  * Serializes `GeoMessage`s
  *
  * @param sft simple feature type being serialized
  */
class GeoMessageSerializer(sft: SimpleFeatureType, lazyDeserialization: Boolean = false) extends LazyLogging {

  private val serializer = {
    val builder = KryoFeatureSerializer.builder(sft).withoutId.withUserData.immutable
    if (lazyDeserialization) { builder.`lazy`.build() } else { builder.build() }
  }

  private lazy val serializerV1 = {
    val builder = KryoFeatureSerializer.builder(sft).withUserData.immutable
    if (lazyDeserialization) { builder.`lazy`.build() } else { builder.build() }
  }

  /**
    * Serializes a message
    *
    * @param msg message
    * @return (serialized key, serialized value)
    */
  def serialize(msg: GeoMessage): (Array[Byte], Array[Byte]) = {
    msg match {
      case m: Change => serialize(m)
      case m: Delete => serialize(m)
      case m: Clear  => serialize(m)
      case _ => throw new IllegalArgumentException(s"Invalid message: '$msg'")
    }
  }

  /**
    * Serializes a change message
    *
    * key: 1 byte message version, n bytes feature id
    * value: n bytes for serialized feature (without id)
    *
    * @param msg msg
    * @return (serialized key, serialized value)
    */
  private def serialize(msg: Change): (Array[Byte], Array[Byte]) = {
    val id = msg.feature.getID.getBytes(StandardCharsets.UTF_8)
    val key = Array.ofDim[Byte](id.length + 1)
    System.arraycopy(id, 0, key, 1, id.length)
    key(0) = GeoMessageSerializer.Version

    (key, serializer.serialize(msg.feature))
  }

  /**
    * Serializes a delete message
    *
    * key: 1 byte message version, n bytes feature id
    * value: null
    *
    * @param msg msg
    * @return (serialized key, serialized value)
    */
  private def serialize(msg: Delete): (Array[Byte], Array[Byte]) = {
    val id = msg.id.getBytes(StandardCharsets.UTF_8)
    val key = Array.ofDim[Byte](id.length + 1)
    System.arraycopy(id, 0, key, 1, id.length)
    key(0) = GeoMessageSerializer.Version

    (key, null)
  }

  /**
    * Serializes a clear message
    *
    * key: 1 byte message version
    * value: 1 byte clear
    *
    * @param msg msg
    * @return (serialized key, serialized value)
    */
  private def serialize(msg: Clear): (Array[Byte], Array[Byte]) =
    (Array(GeoMessageSerializer.Version), GeoMessageSerializer.Empty)

  /**
    * Deserializes a serialized `GeoMessage`
    *
    * @param key the serialized message key
    * @param value the serialized message body
    * @return the deserialized message
    */
  def deserialize(key: Array[Byte], value: Array[Byte]): GeoMessage = {
    try {
      key(0) match {
        case 2 => deserializeV2(key, value)
        case 1 => deserializeV1(key, value)
        case _ => throw new IllegalArgumentException(s"Invalid message version: '${key(0)}'")
      }
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"Unexpected message format: ${Option(key).map(ByteArrays.toHex).getOrElse("")} " +
              s"${Option(value).map(ByteArrays.toHex).getOrElse("")}", e)
    }
  }

  private def deserializeV2(key: Array[Byte], value: Array[Byte]): GeoMessage = {
    if (key.length == 1) { Clear } else {
      val id = new String(key, 1, key.length - 1, StandardCharsets.UTF_8)
      if (value == null) { Delete(id) } else { Change(serializer.deserialize(id, value)) }
    }
  }

  private def deserializeV1(key: Array[Byte], value: Array[Byte]): GeoMessage = {
    key(1).toChar match {
      case 'C' => Change(serializerV1.deserialize(value))
      case 'D' => Delete(new String(value, StandardCharsets.UTF_8))
      case 'X' => Clear
      case m   => throw new IllegalArgumentException(s"Unknown message type: $m" )
    }
  }
}
