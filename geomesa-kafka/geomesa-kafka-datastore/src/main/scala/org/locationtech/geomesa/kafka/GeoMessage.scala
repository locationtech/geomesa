/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.kafka

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.slf4j.Logging
import org.joda.time.Instant
import org.locationtech.geomesa.feature.EncodingOption.EncodingOptions
import org.locationtech.geomesa.feature._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

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
case class CreateOrUpdate(override val timestamp: Instant, feature: SimpleFeature) extends GeoMessage {

  val id = feature.getID
}

/** Delete an existing [[SimpleFeature]]
  *
  * @param id the id of the simple feature
 */
case class Delete(override val timestamp: Instant, id: String) extends GeoMessage

/** Delete all [[SimpleFeature]]s
  */
case class Clear(override val timestamp: Instant) extends GeoMessage


/** Encodes [[GeoMessage]]s.  [[Clear]] and [[Delete]] messages are handled directly.  See class
  * [[GeoMessageEncoder]] for encoding [[CreateOrUpdate]] messages.
  *
  * The following encoding is used:
  *
  * Key: version (1 byte) type (1 byte) timstamp (8 bytes)
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

  def encodeClearMessage(msg: Clear): Array[Byte] = EMPTY

  def encodeDeleteMessage(msg: Delete): Array[Byte] = msg.id.getBytes(StandardCharsets.UTF_8)
}

/** Encodes [[GeoMessage]]s.
  *
  * @param schema the [[SimpleFeatureType]]; required to serialize [[CreateOrUpdate]] messages
  */
class GeoMessageEncoder(schema: SimpleFeatureType) {

  import GeoMessageEncoder._

  val sfEncoder: SimpleFeatureEncoder = new KryoFeatureEncoder(schema, EncodingOptions.withUserData)

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

  def encodeCreateOrUpdateMessage(msg: CreateOrUpdate): Array[Byte] = sfEncoder.encode(msg.feature)
}

/** Decodes an encoded [[GeoMessage]].
  *
  * @param schema the [[SimpleFeatureType]]; required to deserialize [[CreateOrUpdate]] messages
  */
class GeoMessageDecoder(schema: SimpleFeatureType) extends Logging {

  case class MsgKey(version: Byte, msgType: Byte, ts: Instant)

  val sfDecoder: SimpleFeatureDecoder = new KryoFeatureDecoder(schema, EncodingOptions.withUserData)

  def decode(key: Array[Byte], msg: Array[Byte]): GeoMessage = {
    val MsgKey(version, msgType, ts) = decodeKey(key)

    if (version == 1) {
      decodeVersion1(msgType, ts, msg)
    } else {
      throw new IllegalArgumentException(s"Unknown serialization version: $version")
    }
  }

  def decodeKey(key: Array[Byte]): MsgKey = {
    if (key == null) {
      throw new IllegalArgumentException("Invalid null key.")
    }
    if (key.length != 10) {
      throw new IllegalArgumentException(s"Invalid message key.  Expecting 10 bytes but found ${key.length}: "
        + s"${new String(key, StandardCharsets.UTF_8)}")
    }

    val buffer = ByteBuffer.wrap(key)
    val version = buffer.get()
    val msgType = buffer.get()
    val ts = new Instant(buffer.getLong)

    MsgKey(version, msgType, ts)
  }

  def decodeVersion1(msgType: Byte, ts: Instant, msg: Array[Byte]): GeoMessage = msgType match {
    case GeoMessageEncoder.createOrUpdateType =>
      val sf = sfDecoder.decode(msg)
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
