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

import java.util.Properties
import java.util.concurrent.BlockingQueue

import com.typesafe.scalalogging.slf4j.Logging
import kafka.common.TopicAndPartition
import kafka.consumer._
import kafka.message.{Message, MessageAndMetadata}
import kafka.serializer.{Decoder, DefaultDecoder}
import org.apache.commons.lang3.RandomStringUtils
import org.locationtech.geomesa.kafka.KafkaConsumerFactory.RawKafkaStream
import org.locationtech.geomesa.kafka.OffsetManager.Offsets
import org.locationtech.geomesa.kafka.RequestedOffset.MessagePredicate

import scala.collection.Seq
import scala.language.implicitConversions

class KafkaConsumerFactory {

  /** @param zookeepers the zookeeper connection string
    *
    * @return a [[ConsumerConfig]] containing the given ``zookeepers``, a random Group ID and other
    *         fixed properties
    */
  def consumerConfig(zookeepers: String): ConsumerConfig = {
    val groupId = RandomStringUtils.randomAlphanumeric(5)

    val props = new Properties()
    props.put("zookeeper.connect", zookeepers)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "2000")
    props.put("zookeeper.sync.time.ms", "1000")
    props.put("auto.commit.interval.ms", "1000")

    new ConsumerConfig(props)
  }

  /** @param zookeepers the zookeeper connection string
    *
    * @return a high level [[ConsumerConfig]]
    */
  def consumerConnector(zookeepers: String): ConsumerConnector = Consumer.create(consumerConfig(zookeepers))

  /** @param zookeepers the zookeeper connection string
    *
    * @return a low level [[KafkaConsumer]]
    */
  def kafkaConsumer(zookeepers: String): KafkaConsumer[Array[Byte], Array[Byte]] = {
    kafkaConsumer(consumerConfig(zookeepers))
  }

  /** @param config the Kafaka consumer connection configuration
    *
    * @return a low level [[KafkaConsumer]]
    */
  def kafkaConsumer(config: ConsumerConfig): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val decoder: DefaultDecoder = new DefaultDecoder(null)
    new KafkaConsumer[Array[Byte], Array[Byte]](config, decoder, decoder)
  }

  def offsetManager(config: ConsumerConfig): OffsetManager = new OffsetManager(config)

  def messageStreams(zookeepers: String, topic: String, numStreams: Int = 1): Seq[RawKafkaStream] = {

    val client = consumerConnector(zookeepers)
    val whiteList = new Whitelist(topic)
    val decoder = KafkaConsumerFactory.defaultDecoder
    client.createMessageStreamsByFilter(whiteList, 1, decoder, decoder)
  }
}

object KafkaConsumerFactory {

  type RawKafkaStream = KafkaStream[Array[Byte], Array[Byte]]

  implicit val factory: KafkaConsumerFactory = new KafkaConsumerFactory

  val defaultDecoder: Decoder[Array[Byte]] = new DefaultDecoder(null)

}


//////////////////////////////////////////////////////////////////////////
// Delete all code below when KafkaConsumer has been moved into GeoMesa //
//////////////////////////////////////////////////////////////////////////



/** This is a stub for KafkaConsumer, currently part of WT, until is is moved to GeoMesa. */
class KafkaConsumer[K, V](val config: ConsumerConfig, keyDecoder: Decoder[K], valueDecoder: Decoder[V]) {

  def createMessageStreams(topic: String,
                           numStreams: Int,
                           startFrom: RequestedOffset = NextOffset(config.groupId)): List[KafkaStreamLike[K, V]] = ???

}

class OffsetManager(val config: ConsumerConfig) extends AutoCloseable with Logging {

  def getOffsets(topic: String, when: RequestedOffset): Offsets = ???

  override def close(): Unit = {}
}

object OffsetManager extends Logging {
  type Offsets = Map[TopicAndPartition, Long]
}

/** Copied from WT.  Delete when moved to GeoMesa. */
sealed trait RequestedOffset

case object EarliestOffset                          extends RequestedOffset
case object LatestOffset                            extends RequestedOffset
case class  NextOffset(group: String)               extends RequestedOffset
case class  DateOffset(date: Long)                  extends RequestedOffset
case class  FindOffset(predicate: MessagePredicate) extends RequestedOffset

object RequestedOffset {
  // 0 indicates a match, -1 indicates less than, 1 indicates greater than
  type MessagePredicate = (Message) => Int
}

/** This is a stub for KafkaStreamLike, currently part of WT, until is is moved to GeoMesa. */
class KafkaStreamLike[K, V](queue: BlockingQueue[FetchedDataChunk],
                            timeoutMs: Long,
                            keyDecoder: Decoder[K],
                            valueDecoder: Decoder[V]) extends Iterable[MessageAndMetadata[K, V]] {
  override def iterator: Iterator[MessageAndMetadata[K, V]] = ???
}