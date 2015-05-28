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

import kafka.message.{Message, MessageAndMetadata}
import kafka.producer.KeyedMessage
import org.opengis.feature.simple.SimpleFeatureType


/** Encodes [[Clear]] and [[Delete]] messages for transport via Kafka.
  *
  */
object KafkaGeoMessageEncoder {

  import GeoMessageEncoder.encodeKey

  type MSG = KeyedMessage[Array[Byte], Array[Byte]]

  def encodeClearMessage(topic: String, msg: Clear): MSG =
    new MSG(topic, encodeKey(msg), GeoMessageEncoder.encodeClearMessage(msg))

  def encodeDeleteMessage(topic: String, msg: Delete): MSG =
    new MSG(topic, encodeKey(msg), GeoMessageEncoder.encodeDeleteMessage(msg))
}

/** Encodes [[GeoMessage]]s for transport via Kafka.
  *
  * @param schema the [[SimpleFeatureType]]; required to serialize [[CreateOrUpdate]] messages
  */
class KafkaGeoMessageEncoder(schema: SimpleFeatureType) extends GeoMessageEncoder(schema) {

  import GeoMessageEncoder._
  import KafkaGeoMessageEncoder._

  def encodeMessage(topic: String, msg: GeoMessage): MSG = new MSG(topic, encodeKey(msg), encodeMessage(msg))

  val encodeClearMessage: (String, Clear) => MSG = KafkaGeoMessageEncoder.encodeClearMessage

  val encodeDeleteMessage: (String, Delete) => MSG = KafkaGeoMessageEncoder.encodeDeleteMessage

  def encodeCreateOrUpdateMessage(topic: String, msg: CreateOrUpdate): MSG =
    new MSG(topic, encodeKey(msg), encodeCreateOrUpdateMessage(msg))
}

/** Decodes a [[GeoMessage]] transported via Kafka.
  *
  * @param schema the [[SimpleFeatureType]]; required to deserialize [[CreateOrUpdate]] messages
  */
class KafkaGeoMessageDecoder(schema: SimpleFeatureType) extends GeoMessageDecoder(schema) {

  type MSG = MessageAndMetadata[Array[Byte], Array[Byte]]

  def decode(msg: MSG): GeoMessage = decode(msg.key(), msg.message())

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
}
