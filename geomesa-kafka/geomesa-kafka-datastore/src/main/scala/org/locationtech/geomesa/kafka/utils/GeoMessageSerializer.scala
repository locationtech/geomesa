/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.features.avro.AvroFeatureSerializer
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.features.{SerializationType, SimpleFeatureSerializer}
import org.locationtech.geomesa.kafka.utils.GeoMessage.{Change, Clear, Delete}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Random
import scala.util.control.NonFatal
import scala.util.hashing.MurmurHash3

/**
  * Serialized `GeoMessage`s
  *
  * Current encoding (version 2/3), designed to work with kafka log compaction.
  * See https://kafka.apache.org/10/documentation.html#design_compactionbasics
  *
  * Version 3 uses avro for serialized features, version 2 uses kryo
  *
  * change:
  *   key: n bytes feature id
  *   value: n bytes for serialized feature (without id)
  *   headers: "v" -> serialization version
  *
  * delete:
  *   key: n bytes feature id
  *   value: null - this allows for log compaction to delete the feature out
  *   headers: "v" -> serialization version
  *
  * clear:
  *   key: empty
  *   value: empty
  *   headers: "v" -> serialization version
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

  val KryoVersion: Byte = 2
  val AvroVersion: Byte = 3

  val VersionHeader = "v"

  private val Empty = Array.empty[Byte]

  /**
    * Create a message serializer
    *
    * @param sft simple feature type
    * @param serialization serialization type (avro or kryo)
    * @param `lazy` use lazy deserialization
    * @return
    */
  def apply(
      sft: SimpleFeatureType,
      serialization: SerializationType = SerializationType.KRYO,
      `lazy`: Boolean = false): GeoMessageSerializer = {
    val kryoBuilder = KryoFeatureSerializer.builder(sft).withoutId.withUserData.immutable
    val avroBuilder = AvroFeatureSerializer.builder(sft).withoutId.withUserData.immutable
    val kryoSerializer = if (`lazy`) { kryoBuilder.`lazy`.build() } else { kryoBuilder.build() }
    val avroSerializer = if (`lazy`) { avroBuilder.`lazy`.build() } else { avroBuilder.build() }
    val (serializer, version) = serialization match {
      case SerializationType.KRYO => (kryoSerializer, KryoVersion)
      case SerializationType.AVRO => (avroSerializer, AvroVersion)
      case _ => throw new NotImplementedError(s"Unhandled serialization type '$serialization'")
    }
    new GeoMessageSerializer(sft, serializer, kryoSerializer, avroSerializer, version)
  }

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
        if (keyBytes.length > 0) {
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

  class GeoMessageSerializerFactory {
    def apply(sft: SimpleFeatureType, serialization: SerializationType, `lazy`: Boolean): GeoMessageSerializer =
      GeoMessageSerializer.apply(sft, serialization, `lazy`)
  }
}

/**
  * Serializes `GeoMessage`s
  *
  * @param sft simple feature type being serialized
  * @param serializer serializer used for reading or writing messages
  * @param kryo kryo serializer used for deserializing kryo messages
  * @param avro avro serializer used for deserializing avro messages
  * @param version version byte corresponding to the serializer type
  */
class GeoMessageSerializer(sft: SimpleFeatureType,
                           val serializer: SimpleFeatureSerializer,
                           kryo: KryoFeatureSerializer,
                           avro: AvroFeatureSerializer,
                           version: Byte) extends LazyLogging {

  private val headers = Map(GeoMessageSerializer.VersionHeader -> Array(version))

  private lazy val serializerV1 = KryoFeatureSerializer.builder(sft).withUserData.immutable.build()

  /**
    * Serializes a message
    *
    * @param msg message
    * @return (serialized key, serialized value)
    */
  def serialize(msg: GeoMessage): (Array[Byte], Array[Byte], Map[String, Array[Byte]]) = {
    msg match {
      case m: Change => serialize(m)
      case m: Delete => serialize(m)
      case m: Clear  => serialize(m)
      case _ => throw new IllegalArgumentException(s"Invalid message: '$msg'")
    }
  }

  /**
    * Deserializes a serialized `GeoMessage`
    *
    * @param key the serialized message key
    * @param value the serialized message body
    * @param timestamp the kafka message timestamp
    * @return the deserialized message
    */
  def deserialize(
      key: Array[Byte],
      value: Array[Byte],
      headers: Map[String, Array[Byte]] = Map.empty,
      timestamp: Long = System.currentTimeMillis()): GeoMessage = {
    try {
      headers.get(GeoMessageSerializer.VersionHeader) match {
        case Some(h) if h.length == 1 && h(0) == GeoMessageSerializer.KryoVersion => deserialize(key, value, kryo)
        case Some(h) if h.length == 1 && h(0) == GeoMessageSerializer.AvroVersion => deserialize(key, value, avro)
        case _ => tryDeserializeVersions(key, value)
      }
    } catch {
      case NonFatal(e) =>
        throw new IllegalArgumentException(
          s"Unexpected message format: ${Option(key).map(ByteArrays.toHex).getOrElse("")} " +
              s"${Option(value).map(ByteArrays.toHex).getOrElse("")}", e)
    }
  }

  /**
    * Serializes a change message
    *
    * key: n bytes feature id
    * value: n bytes for serialized feature (without id)
    * headers: "v" -> 1 byte message version
    *
    * @param msg msg
    * @return (serialized key, serialized value, headers)
    */
  private def serialize(msg: Change): (Array[Byte], Array[Byte], Map[String, Array[Byte]]) =
    (msg.feature.getID.getBytes(StandardCharsets.UTF_8), serializer.serialize(msg.feature), headers)

  /**
    * Serializes a delete message
    *
    * key: n bytes feature id
    * value: null
    * headers: "v" -> 1 byte message version
    *
    * @param msg msg
    * @return (serialized key, serialized value, headers)
    */
  private def serialize(msg: Delete): (Array[Byte], Array[Byte], Map[String, Array[Byte]]) =
    (msg.id.getBytes(StandardCharsets.UTF_8), null, headers)

  /**
    * Serializes a clear message
    *
    * key: empty
    * value: empty
    * headers: "v" -> 1 byte message version
    *
    * @param msg msg
    * @return (serialized key, serialized value, headers)
    */
  private def serialize(msg: Clear): (Array[Byte], Array[Byte], Map[String, Array[Byte]]) =
    (GeoMessageSerializer.Empty, GeoMessageSerializer.Empty, headers)

  /**
    * Deserialize a message using the appropriate serializer
    *
    * @param key message key
    * @param value message value
    * @param deserializer deserializer appropriate for the message encoding
    * @return
    */
  private def deserialize(key: Array[Byte], value: Array[Byte], deserializer: SimpleFeatureSerializer): GeoMessage = {
    if (key.isEmpty) { Clear } else {
      val id = new String(key, StandardCharsets.UTF_8)
      if (value == null) { Delete(id) } else { Change(deserializer.deserialize(id, value)) }
    }
  }

  /**
    * Used to deserialize messages without headers, which may be caused by:
    *
    * a) version 1 messages
    * b) an older kafka version that doesn't support message headers
    *
    * @param key message key
    * @param value message value
    * @return
    */
  private def tryDeserializeVersions(key: Array[Byte], value: Array[Byte]): GeoMessage = {
    if (key.length == 10 && key(0) == 1 && Seq('C', 'D', 'X').contains(key(1).toChar)) {
      try { deserializeV1(key, value) } catch {
        case NonFatal(e) =>
          try { tryDeserializeTypes(key, value) } catch {
            case NonFatal(suppressed) => e.addSuppressed(suppressed); throw e
          }
      }
    } else {
      tryDeserializeTypes(key, value)
    }
  }

  /**
    * Try to deserialize using both kryo and avro
    *
    * @param key message key
    * @param value message value
    * @return
    */
  private def tryDeserializeTypes(key: Array[Byte], value: Array[Byte]): GeoMessage = {
    try { deserialize(key, value, serializer) } catch {
      case NonFatal(e) =>
        try { deserialize(key, value, if (serializer.eq(kryo)) { avro } else { kryo }) } catch {
          case NonFatal(suppressed) => e.addSuppressed(suppressed); throw e
        }
    }
  }

  /**
    * Deserialize version 1 messages
    *
    * @param key message key
    * @param value message value
    * @return
    */
  private def deserializeV1(key: Array[Byte], value: Array[Byte]): GeoMessage = {
    key(1).toChar match {
      case 'C' => Change(serializerV1.deserialize(value))
      case 'D' => Delete(new String(value, StandardCharsets.UTF_8))
      case 'X' => Clear
      case m   => throw new IllegalArgumentException(s"Unknown message type: $m" )
    }
  }
}
