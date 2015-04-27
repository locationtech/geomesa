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
import java.util

import com.typesafe.scalalogging.slf4j.Logging
import kafka.message.MessageAndMetadata
import kafka.producer.KeyedMessage
import org.joda.time.Instant
import org.locationtech.geomesa.feature.EncodingOption.EncodingOptions
import org.locationtech.geomesa.feature.{AvroFeatureDecoder, AvroFeatureEncoder, SimpleFeatureDecoder, SimpleFeatureEncoder}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

sealed trait KafkaGeoMessage {

  def timestamp: Instant
}

object KafkaGeoMessage {

  implicit val clock = SystemClock

  def clear()(implicit clock: Clock): Clear =
    new Clear(clock.now)

  def delete(id: String)(implicit clock: Clock): Delete =
    new Delete(clock.now, id)

  def createOrUpdate(sf: SimpleFeature)(implicit clock: Clock): CreateOrUpdate =
    new CreateOrUpdate(clock.now, sf)
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

trait Clock {
  def now: Instant
}

object SystemClock extends Clock {
  override def now = Instant.now
}

/** Encodes [[KafkaGeoMessage]]s using the following encoding:
  *
  * Key: version (1 byte) type (1 byte) timstamp (8 bytes)
  *
  * The current version is 1
  * The type is 'C' for create or update, 'R' for delete and 'X' for clear
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

  val sfEncoder: SimpleFeatureEncoder = new AvroFeatureEncoder(schema, EncodingOptions.withUserData)

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

  private lazy val V0_DELETE_KEY = "delete".getBytes(StandardCharsets.UTF_8)
  private lazy val V0_CLEAR_KEY = "clear".getBytes(StandardCharsets.UTF_8)
  private lazy val V0_TIMESTAMP = new Instant(0L)

  val sfDecoder: SimpleFeatureDecoder = new AvroFeatureDecoder(schema, EncodingOptions.withUserData)

  def decode(msg: MSG): KafkaGeoMessage = {

    val key = msg.key()

    if (key != null && key(0) == 1) {
      decodeVersion1(msg)
    } else {
      // try version 0
      decodeVersion0(msg)
    }
  }

  def decodeKey(key: Array[Byte]): (Instant, Byte) = {
    if (key.length != 10) {
      throw new IllegalArgumentException(s"Invalid message key.  Expecting 10 bytes but found ${key.length}: "
        + s"${new String(key, StandardCharsets.UTF_8)}")
    }

    val buffer = ByteBuffer.wrap(key)

    buffer.get() // version
    val msgType = buffer.get()
    val ts = new Instant(buffer.getLong)

    (ts, msgType)
  }

  def decodeVersion1(msg: MSG): KafkaGeoMessage = {
    val (ts, msgType) = decodeKey(msg.key())

    msgType match {
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

  def decodeVersion0(msg: MSG): KafkaGeoMessage = {

    if(msg.key() != null) {
      if(util.Arrays.equals(msg.key(), V0_DELETE_KEY)) {
        val id = new String(msg.message(), StandardCharsets.UTF_8)
        Delete(V0_TIMESTAMP, id)
      } else if(util.Arrays.equals(msg.key(), V0_CLEAR_KEY)) {
        Clear(V0_TIMESTAMP)
      } else {
        throw new IllegalArgumentException("Unknown message key: " + new String(msg.key(), StandardCharsets.UTF_8))
      }
    } else {
      val sf = sfDecoder.decode(msg.message())
      CreateOrUpdate(V0_TIMESTAMP, sf)
    }
  }
}
