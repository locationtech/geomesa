/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.utils

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.time.Instant

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.features.kryo.impl.KryoFeatureDeserialization
import org.locationtech.geomesa.kafka.utils.GeoMessage.{Change, Clear, Delete}
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Random
import scala.util.hashing.MurmurHash3

/**
  * Serialized `GeoMessage`s
  *
  * The following encoding is used:
  *
  * Key: version (1 byte) type (1 byte) timestamp (8 bytes)
  *
  * The current version is 1
  * The type is 'C' for change, 'D' for delete and 'X' for clear
  *
  * Value
  *   Change: serialized simple feature
  *   Delete: simple feature ID as UTF-8 bytes
  *   Clear: empty
  *
  */
object GeoMessageSerializer {

  private val Empty = Array.empty[Byte]

  val Version: Byte = 1

  val ChangeType: Byte = 'C'
  val DeleteType: Byte = 'D'
  val ClearType: Byte  = 'X'

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

      def hash(id: String): Int = Math.abs(MurmurHash3.stringHash(id)) % count

      // use the feature id if available, otherwise (for clear) use random shard
      keyBytes(1) match {
        case ChangeType => hash(deserializeFid(valueBytes))
        case DeleteType => hash(new String(valueBytes, StandardCharsets.UTF_8))
        case _          => Random.nextInt(count)
      }
    }

    override def configure(configs: java.util.Map[String, _]): Unit = {}

    override def close(): Unit = {}

    /**
      * Deserializes just the feature id from a serialized simple feature.
      * Note: feature id starts at position 5
      *
      * @param bytes serialized bytes
      * @return feature id
      */
    private def deserializeFid(bytes: Array[Byte]): String =
      KryoFeatureDeserialization.getInput(bytes, 5, bytes.length - 5).readString()
  }
}

/**
  * Serializes `GeoMessage`s
  *
  * @param sft simple feature type being serialized
  */
class GeoMessageSerializer(sft: SimpleFeatureType) extends LazyLogging {

  import GeoMessageSerializer._

  private val serializer = KryoFeatureSerializer(sft, SerializationOptions.builder.withUserData.immutable.`lazy`.build)

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

  def serialize(msg: Change): (Array[Byte], Array[Byte]) =
    (serializeKey(ChangeType, msg.timestamp), serializer.serialize(msg.feature))

  def serialize(msg: Delete): (Array[Byte], Array[Byte]) =
    (serializeKey(DeleteType, msg.timestamp), msg.id.getBytes(StandardCharsets.UTF_8))

  def serialize(msg: Clear): (Array[Byte], Array[Byte]) = (serializeKey(ClearType, msg.timestamp), Empty)

  /**
    * Deserializes a serialized `GeoMessage`
    *
    * @param key the serialized message key
    * @param value the serialized message body
    * @return the deserialized message
    */
  def deserialize(key: Array[Byte], value: Array[Byte]): GeoMessage = {
    require(key != null && key.length > 0, s"Invalid empty message key")

    val buffer = ByteBuffer.wrap(key)
    val version = buffer.get()

    require(version == 1 && key.length == 10, s"Invalid version/length (expected 1-10): $version-${key.length}")

    val msgType = buffer.get()
    val ts = Instant.ofEpochMilli(buffer.getLong)

    msgType match {
      case ChangeType => Change(ts, serializer.deserialize(value))
      case DeleteType => Delete(ts, new String(value, StandardCharsets.UTF_8))
      case ClearType  => Clear(ts)
      case _ => throw new IllegalArgumentException("Unknown message type: " + msgType.toChar)
    }
  }

  private def serializeKey(msgType: Byte, ts: Instant): Array[Byte] = {
    val bb = ByteBuffer.allocate(10)
    bb.put(Version)
    bb.put(msgType)
    bb.putLong(ts.toEpochMilli)
    bb.array()
  }
}
