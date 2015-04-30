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
import kafka.message.{Message, MessageAndMetadata}
import kafka.producer.KeyedMessage
import org.joda.time.Instant
import org.locationtech.geomesa.feature.EncodingOption.EncodingOptions
import org.locationtech.geomesa.feature._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

sealed trait KafkaGeoMessage {

  def timestamp: Instant
}

object KafkaGeoMessage {

  def clear(): Clear = Clear(Instant.now)

  def delete(id: String): Delete = Delete(Instant.now, id)

  def createOrUpdate(sf: SimpleFeature): CreateOrUpdate = CreateOrUpdate(Instant.now, sf)
}

/** Create a new simple feature or update an existing [[SimpleFeature]]
  *
  * @param feature the [[SimpleFeature]]
  */
case class CreateOrUpdate(override val timestamp: Instant, feature: SimpleFeature) extends KafkaGeoMessage {

  val id = feature.getID
}

/** Delete an existing [[SimpleFeature]]
  *
  * @param id the id of the simple feature
 */
case class Delete(override val timestamp: Instant, id: String) extends KafkaGeoMessage

/** Delete all [[SimpleFeature]]s
  */
case class Clear(override val timestamp: Instant) extends KafkaGeoMessage


/** Encodes [[KafkaGeoMessage]]s using the following encoding:
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
object KafkaGeoMessageEncoder {

  val version: Byte = 1
  val createOrUpdateType: Byte = 'C'
  val deleteType: Byte = 'D'
  val clearType: Byte = 'X'

  type MSG = KeyedMessage[Array[Byte], Array[Byte]]

  private val EMPTY = Array.empty[Byte]

  def encodeKey(msg: KafkaGeoMessage): Array[Byte] = {

    val msgType: Byte = msg match {
      case c: CreateOrUpdate => createOrUpdateType
      case d: Delete => deleteType
      case x: Clear => clearType
    }

    val bb = ByteBuffer.allocate(10)
    bb.put(version)
    bb.put(msgType)
    bb.putLong(msg.timestamp.getMillis)
    bb.array()
  }

  def encodeClearMessage(topic: String, msg: Clear): MSG = new MSG(topic, encodeKey(msg), EMPTY)

  def encodeDeleteMessage(topic: String, msg: Delete): MSG = {
    val key = encodeKey(msg)
    val value = msg.id.getBytes(StandardCharsets.UTF_8)
    new MSG(topic, key, value)
  }
}

/** For encoding any [[KafkaGeoMessage]].
  *
  * @param schema the [[SimpleFeatureType]]; required to serialize [[CreateOrUpdate]] messages
  */
class KafkaGeoMessageEncoder(val schema: SimpleFeatureType) {

  import KafkaGeoMessageEncoder.{MSG, encodeKey}

  val sfEncoder: SimpleFeatureEncoder = new KryoFeatureEncoder(schema, EncodingOptions.withUserData)

  def encode(topic: String, msg: KafkaGeoMessage): MSG = msg match {
    case c: CreateOrUpdate =>
      encodeCreateOrUpdateMessage(topic, c)
    case d: Delete =>
      encodeDeleteMessage(topic, d)
    case x: Clear =>
      encodeClearMessage(topic, x)
  }

  val encodeClearMessage: (String, Clear) => MSG = KafkaGeoMessageEncoder.encodeClearMessage

  val encodeDeleteMessage: (String, Delete) => MSG = KafkaGeoMessageEncoder.encodeDeleteMessage

  def encodeCreateOrUpdateMessage(topic: String, msg: CreateOrUpdate): MSG = {
    val key = encodeKey(msg)
    val value = sfEncoder.encode(msg.feature)
    new MSG(topic, key, value)
  }
}

class KafkaGeoMessageDecoder(val schema: SimpleFeatureType) extends Logging {

  type MSG = MessageAndMetadata[Array[Byte], Array[Byte]]

  case class MsgKey(version: Byte, msgType: Byte, ts: Instant)

  val sfDecoder: SimpleFeatureDecoder = new KryoFeatureDecoder(schema, EncodingOptions.withUserData)

  def decode(msg: MSG): KafkaGeoMessage = {

    val MsgKey(version, msgType, ts) = decodeKey(msg.key())

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

  def decodeKey(msg: Message): MsgKey = {
    if (!msg.hasKey) {
      decodeKey(null : Array[Byte])
    } else {
      val buffer = msg.key
      val bytes = Array.ofDim[Byte](buffer.remaining())
      buffer.get(bytes)
      decodeKey(bytes)
    }
  }

  def decodeVersion1(msgType: Byte, ts: Instant, msg: MSG): KafkaGeoMessage = msgType match {
    case KafkaGeoMessageEncoder.createOrUpdateType =>
      val sf = sfDecoder.decode(msg.message())
      CreateOrUpdate(ts, sf)
    case KafkaGeoMessageEncoder.deleteType =>
      val id = new String(msg.message(), StandardCharsets.UTF_8)
      Delete(ts, id)
    case KafkaGeoMessageEncoder.clearType =>
      Clear(ts)
    case _ =>
      throw new IllegalArgumentException("Unknown message type: " + msgType.toChar)
  }
}

