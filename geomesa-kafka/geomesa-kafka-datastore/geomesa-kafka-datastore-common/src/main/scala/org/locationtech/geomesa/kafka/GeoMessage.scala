/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import org.joda.time.Instant
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.opengis.feature.simple.{SimpleFeatureType, SimpleFeature}

sealed trait GeoMessage {

  def timestamp: Instant
}

object GeoMessage {

  /** @return a new [[Clear]] message with the current time */
  def clear(): Clear = Clear(Instant.now)

  /** @return a new [[Clear]] message with the given feature ``id`` and current time */
  def delete(id: String): Delete = Delete(Instant.now, id)

  /** @return a new [[CreateOrUpdate]] message with the given ``sf`` and current time */
  def createOrUpdate(sf: SimpleFeature): CreateOrUpdate = CreateOrUpdate(Instant.now, sf)
}

/** Create a new simple feature or update an existing [[SimpleFeature]]
  *
  * @param feature the [[SimpleFeature]]
  */
case class CreateOrUpdate(override val timestamp: Instant, feature: SimpleFeature) extends GeoMessage

/** Delete an existing [[SimpleFeature]]
  *
  * @param id the id of the simple feature
 */
case class Delete(override val timestamp: Instant, id: String) extends GeoMessage

/** Delete all [[org.opengis.feature.simple.SimpleFeature]]s
  */
case class Clear(override val timestamp: Instant) extends GeoMessage


/** Encodes [[GeoMessage]]s.  [[Clear]] and [[Delete]] messages are handled directly.  See class
  * [[GeoMessageEncoder]] for encoding [[CreateOrUpdate]] messages.
  *
  * The following encoding is used:
  *
  * Key: version (1 byte) type (1 byte) timestamp (8 bytes)
  *
  * The current version is 1
  * The type is 'C' for create or update, 'D' for delete and 'X' for clear
  *
  * Value
  *   CreateOrUpdate: serialized simple feature
  *   Delete: simple feature ID as UTF-8 bytes
  *   Clear: empty
  *
  */
object GeoMessageEncoder {

  val version: Byte = 1
  val createOrUpdateType: Byte = 'C'
  val deleteType: Byte = 'D'
  val clearType: Byte = 'X'

  private val EMPTY = Array.empty[Byte]
}

/** Encodes [[GeoMessage]]s.
  *
  * @param schema the [[SimpleFeatureType]]; required to serialize [[CreateOrUpdate]] messages
  */
class GeoMessageEncoder(schema: SimpleFeatureType) {

  import GeoMessageEncoder._

  private val serializer = new KryoFeatureSerializer(schema, SerializationOptions.withUserData)

  def encodeKey(msg: GeoMessage): Array[Byte] = {
    val msgType: Byte = msg match {
      case c: CreateOrUpdate => createOrUpdateType
      case d: Delete => deleteType
      case x: Clear => clearType
      case u => throw new IllegalArgumentException(s"Invalid message: '$u'")
    }

    val bb = ByteBuffer.allocate(10)
    bb.put(version)
    bb.put(msgType)
    bb.putLong(msg.timestamp.getMillis)
    bb.array()
  }

  def encodeMessage(msg: GeoMessage): Array[Byte] = msg match {
    case c: CreateOrUpdate =>
      encodeCreateOrUpdateMessage(c)
    case d: Delete =>
      encodeDeleteMessage(d)
    case x: Clear =>
      encodeClearMessage(x)
    case u =>
      throw new IllegalArgumentException(s"Invalid message: '$u'")
  }

  def encodeCreateOrUpdateMessage(msg: CreateOrUpdate): Array[Byte] = serializer.serialize(msg.feature)

  def encodeDeleteMessage(msg: Delete): Array[Byte] = msg.id.getBytes(StandardCharsets.UTF_8)

  def encodeClearMessage(msg: Clear): Array[Byte] = EMPTY
}

/** Decodes an encoded [[GeoMessage]].
  *
  * @param schema the [[SimpleFeatureType]]; required to deserialize [[CreateOrUpdate]] messages
  */
class GeoMessageDecoder(schema: SimpleFeatureType) extends LazyLogging {

  case class MsgKey(version: Byte, msgType: Byte, ts: Instant)

  private val serializer = new KryoFeatureSerializer(schema, SerializationOptions.withUserData)

  /** Decodes an encoded [[GeoMessage]] represented by the given ``key`` and ``msg``.
    *
    * @param key the encoded message key
    * @param msg the encoded message body
    * @return the decoded [[GeoMessage]]
    */
  def decode(key: Array[Byte], msg: Array[Byte]): GeoMessage = {
    val MsgKey(version, msgType, ts) = decodeKey(key)

    if (version == 1) {
      decodeVersion1(msgType, ts, msg)
    } else {
      throw new IllegalArgumentException(s"Unknown serialization version: $version")
    }
  }

  protected def decodeKey(key: Array[Byte]): MsgKey = {

    def keyToString = if (key == null) "null" else s"'${new String(key, StandardCharsets.UTF_8)}'"

    if (key == null || key.length == 0) {
      throw new IllegalArgumentException(
        s"Invalid message key: $keyToString.  Cannot determine serialization version.")
    }

    val buffer = ByteBuffer.wrap(key)
    val version = buffer.get()

    if (version == 1) {
      if (key.length != 10) {
        throw new IllegalArgumentException(
          s"Invalid version 1 message key: $keyToString.  Expecting 10 bytes but found ${key.length}.")
      }

      val msgType = buffer.get()
      val ts = new Instant(buffer.getLong)

      MsgKey(version, msgType, ts)
    } else {
      throw new IllegalArgumentException(
        s"Invalid message key: $keyToString.  Unknown serialization version: $version")
    }
  }

  private def decodeVersion1(msgType: Byte, ts: Instant, msg: Array[Byte]): GeoMessage = msgType match {
    case GeoMessageEncoder.createOrUpdateType =>
      val sf = serializer.deserialize(msg)
      CreateOrUpdate(ts, sf)
    case GeoMessageEncoder.deleteType =>
      val id = new String(msg, StandardCharsets.UTF_8)
      Delete(ts, id)
    case GeoMessageEncoder.clearType =>
      Clear(ts)
    case _ =>
      throw new IllegalArgumentException("Unknown message type: " + msgType.toChar)
  }
}
