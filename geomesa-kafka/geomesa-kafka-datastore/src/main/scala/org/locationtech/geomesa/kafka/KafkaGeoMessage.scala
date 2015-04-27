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

import java.nio.charset.StandardCharsets
import java.util

import com.typesafe.scalalogging.slf4j.Logging
import kafka.message.MessageAndMetadata
import kafka.producer.KeyedMessage
import org.locationtech.geomesa.feature.EncodingOption.EncodingOptions
import org.locationtech.geomesa.feature.{AvroFeatureDecoder, AvroFeatureEncoder, SimpleFeatureDecoder, SimpleFeatureEncoder}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

sealed trait KafkaGeoMessage

object KafkaGeoMessage {

  val DELETE_KEY = "delete".getBytes(StandardCharsets.UTF_8)
  val CLEAR_KEY  = "clear".getBytes(StandardCharsets.UTF_8)

}

/** Create a new simple feature or update an existing [[SimpleFeature]]
  *
  * @param feature the [[SimpleFeature]]
  */
case class CreateOrUpdate(feature: SimpleFeature) extends KafkaGeoMessage {

  val id = feature.getID
}

/** Delete an existing [[SimpleFeature]]
  *
  * @param id the id of the simple feature
 */
case class Delete(id: String) extends KafkaGeoMessage

/** Delete all [[SimpleFeature]]s
  */
case object Clear extends KafkaGeoMessage


/** Encodes [[Delete]] and [[Clear]] messages without requiring a [[SimpleFeatureEncoder]].
  */
object KafkaGeoMessageEncoder {

  type MSG = KeyedMessage[Array[Byte], Array[Byte]]

  private val EMPTY = Array.empty[Byte]

  def encodeClearMessage(topic: String): MSG = new MSG(topic, KafkaGeoMessage.CLEAR_KEY, EMPTY)

  def encodeDeleteMessage(topic: String, id: String): MSG = {
    val idEncoded = id.getBytes(StandardCharsets.UTF_8)
    new MSG(topic, KafkaGeoMessage.DELETE_KEY, idEncoded)
  }
}

/** For encoding any [[KafkaGeoMessage]].
  *
  * @param schema the [[SimpleFeatureType]]; required to serialize [[CreateOrUpdate]] messages
  */
class KafkaGeoMessageEncoder(val schema: SimpleFeatureType) {

  import KafkaGeoMessageEncoder._

  val sfEncoder: SimpleFeatureEncoder = new AvroFeatureEncoder(schema, EncodingOptions.withUserData)

  def encode(topic: String, msg: KafkaGeoMessage): MSG = msg match {
    case CreateOrUpdate(sf) =>
      val sfEncoded = sfEncoder.encode(sf)
      new MSG(topic, sfEncoded)
    case Delete(id) =>
      encodeDeleteMessage(topic, id)
    case Clear =>
      encodeClearMessage(topic)
  }
}

class KafkaGeoMessageDecoder(val schema: SimpleFeatureType) extends Logging {

  type MSG = MessageAndMetadata[Array[Byte], Array[Byte]]

  val sfDecoder: SimpleFeatureDecoder = new AvroFeatureDecoder(schema, EncodingOptions.withUserData)

  def decode(msg: MSG): KafkaGeoMessage = {
    if(msg.key() != null) {
      if(util.Arrays.equals(msg.key(), KafkaGeoMessage.DELETE_KEY)) {
        val id = new String(msg.message(), StandardCharsets.UTF_8)
        Delete(id)
      } else if(util.Arrays.equals(msg.key(), KafkaGeoMessage.CLEAR_KEY)) {
        Clear
      } else {
        throw new IllegalArgumentException("Unknow message key: " + new String(msg.key(), StandardCharsets.UTF_8))
      }
    } else {
      val sf = sfDecoder.decode(msg.message())
      CreateOrUpdate(sf)
    }
  }
}