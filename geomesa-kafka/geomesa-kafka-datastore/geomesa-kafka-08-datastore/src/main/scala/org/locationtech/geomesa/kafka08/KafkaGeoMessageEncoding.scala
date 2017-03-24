/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka08

import kafka.message.{Message, MessageAndMetadata}
import kafka.producer.KeyedMessage
import org.locationtech.geomesa.kafka._
import org.opengis.feature.simple.SimpleFeatureType

/** Encodes [[GeoMessage]]s for transport via Kafka.
  *
  * @param schema the [[SimpleFeatureType]]; required to serialize [[CreateOrUpdate]] messages
  */
class KafkaGeoMessageEncoder(schema: SimpleFeatureType) extends GeoMessageEncoder(schema) {

  type MSG = KeyedMessage[Array[Byte], Array[Byte]]

  def encodeMessage(topic: String, msg: GeoMessage): MSG =
    new MSG(topic, encodeKey(msg), encodeMessage(msg))

  def encodeClearMessage(topic: String, msg: Clear): MSG =
    new MSG(topic, encodeKey(msg), encodeClearMessage(msg))

  def encodeDeleteMessage(topic: String, msg: Delete): MSG =
    new MSG(topic, encodeKey(msg), encodeDeleteMessage(msg))

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
