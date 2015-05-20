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

import kafka.api._
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.{ConsumerTimeoutException, ConsumerConfig}
import kafka.message.{Message, MessageAndMetadata, MessageAndOffset}
import kafka.producer.KeyedMessage
import kafka.serializer.Decoder
import org.locationtech.geomesa.kafka.consumer._
import org.locationtech.geomesa.kafka.consumer.offsets.FindOffset._
import org.locationtech.geomesa.kafka.consumer.offsets.OffsetManager._
import org.locationtech.geomesa.kafka.consumer.offsets._
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

class MockKafka {

  var data: Map[TopicAndPartition, Seq[MockMessage]] = Map.empty

  /** @return the latest offset for the given ``tap``, same as Kafka would */
  def latestOffset(tap: TopicAndPartition): Long = data.get(tap).map(_.size : Long).getOrElse(0L)

  val kafkaConsumerFactory = new MockKafkaConsumerFactory(this)
  val consumerConfig = kafkaConsumerFactory.config

  /** Simulate sending a message to a producer
    */
  def send(keyedMessage: KeyedMessage[Array[Byte], Array[Byte]], partition: Int = 0): Unit = {
    val tap = new TopicAndPartition(keyedMessage.topic, partition)
    val oldMsgs = data.getOrElse(tap, Seq.empty)
    val offset = oldMsgs.size
    val msgs = oldMsgs :+ MockMessage(tap, keyedMessage, offset)

    data += (tap -> msgs)
  }

  def fetch(tap: TopicAndPartition): Iterable[MessageAndOffset] =
    data.getOrElse(tap, Seq.empty).map(_.asMessageAndOffset)

  def fetch(tap: TopicAndPartition, offset: Long, maxBytes: Int): Iterable[MessageAndOffset] = {
    fetch(tap)
      .drop(offset.asInstanceOf[Int]).foldLeft((0, Seq.empty[MessageAndOffset])) {
      case ((totalBytes, kept), next) if totalBytes <= maxBytes =>
        val size = next.message.size
        if (totalBytes + size <= maxBytes) {
          (totalBytes + size, kept :+ next)
        } else {
          (maxBytes + 1, kept)
        }
      case (result, next) => result
    }._2
  }
}

case class MockMessage(tap: TopicAndPartition, key: Array[Byte], msg: Array[Byte], offset: Long) {

  def asMessage: Message = new Message(msg, key)

  def asMessageAndOffset: MessageAndOffset = new MessageAndOffset(asMessage, offset)

  def asMessageAndMetadata[K, V](keyDecoder: Decoder[K], valueDecoder: Decoder[V]): MessageAndMetadata[K, V] =
    new MessageAndMetadata[K, V](tap.topic, tap.partition, asMessage, offset, keyDecoder, valueDecoder)
}

object MockMessage {
  
  def apply(tap: TopicAndPartition, kMessage: KeyedMessage[Array[Byte], Array[Byte]], offset: Long): MockMessage =
    MockMessage(tap, kMessage.key, kMessage.message, offset)

}

class MockKafkaConsumerFactory(val mk: MockKafka)
  extends KafkaConsumerFactory("mock-broker:9092", "mock-zoo") {

  import KafkaConsumerFactory._

  override def kafkaConsumer(topic: String, extraConfig: Map[String, String] = Map.empty) =
    MockKafkaConsumer[Array[Byte], Array[Byte]](mk, topic, defaultDecoder, defaultDecoder, extraConfig)

  override val offsetManager = new MockOffsetManager(mk)
}

object MockKafkaStream {

  def apply[K, V](data: Seq[MockMessage],
                  keyDecoder: Decoder[K],
                  valueDecoder: Decoder[V],
                  timeoutEnabled: Boolean): KafkaStreamLike[K, V] =
    apply(data.map(_.asMessageAndMetadata(keyDecoder, valueDecoder)), timeoutEnabled)

  def apply[K, V](data: Seq[MessageAndMetadata[K, V]],
                  timeoutEnabled: Boolean): KafkaStreamLike[K, V] = {

    val ksl = mock(classOf[KafkaStreamLike[K, V]])

    // Kafka consumer iterators will block until there is a new message
    // for testing, throw an exception if a consumer tries to read beyond ``data``
    // if the client specified a consumer timeout then a ConsumerTimeoutException will be expected
    val endException = if (timeoutEnabled)
      () => throw new ConsumerTimeoutException()
    else
      () => throw new IllegalStateException("Attempting to read beyond given mock data.")

    val end: Iterator[MessageAndMetadata[K, V]] = new Iterator[MessageAndMetadata[K, V]] {

      override def hasNext: Boolean = throw endException()
      override def next(): MessageAndMetadata[K, V] = throw endException()
    }

    when(ksl.iterator).thenAnswer(new Answer[Iterator[MessageAndMetadata[K, V]]] {

      override def answer(invocation: InvocationOnMock): Iterator[MessageAndMetadata[K, V]] = {
        data.iterator ++ end
      }
    })

    ksl
  }

}

object MockKafkaConsumer {

  def apply[K, V](mk: MockKafka,
                  topic: String,
                  keyDecoder: Decoder[K],
                  valueDecoder: Decoder[V],
                  extraConfig: Map[String, String]): KafkaConsumer[K, V] = {

    val timeoutEnabled = extraConfig.get("consumer.timeout.ms").exists(_.toInt >= 0)

    val consumer = mock(classOf[KafkaConsumer[K, V]])
    when(consumer.createMessageStreams(anyInt(), any(classOf[RequestedOffset])))
      .thenAnswer(new Answer[List[KafkaStreamLike[K,V]]]() {

      override def answer(invocation: InvocationOnMock): List[KafkaStreamLike[K, V]] = {

        val numStreams = invocation.getArguments()(0).asInstanceOf[Int]
        val startFrom = invocation.getArguments()(1).asInstanceOf[RequestedOffset]
        createMessageStreams(mk, topic, keyDecoder, valueDecoder, numStreams, startFrom, timeoutEnabled)
      }
    })

    consumer
  }

  def createMessageStreams[K, V](mk: MockKafka,
                                 topic: String,
                                 keyDecoder: Decoder[K],
                                 valueDecoder: Decoder[V],
                                 numStreams: Int,
                                 startFrom: RequestedOffset,
                                 timeoutEnabled: Boolean): List[KafkaStreamLike[K, V]] = {

    val offsets = mk.kafkaConsumerFactory.offsetManager.getOffsets(topic, startFrom)
    val partitions = offsets.keys.toArray

    val numPartitionsPerStream =
      partitions.length / numStreams + (if (partitions.length % numStreams == 0) 0 else 1)

    if (numPartitionsPerStream > 0) {
      val streams = ArrayBuffer.empty[KafkaStreamLike[K, V]]

      partitions.grouped(numPartitionsPerStream).foreach { streamPartitions =>

        val data = streamPartitions.foldLeft(Seq.empty[MockMessage]) { (all, tap) =>
          all ++ mk.data(tap).drop(offsets(tap).asInstanceOf[Int])
        }

        streams.append(MockKafkaStream(data, keyDecoder, valueDecoder, timeoutEnabled))
      }

      streams.toList
    } else {
      // no data for the topic
      List.fill(numStreams)(MockKafkaStream(Seq.empty[MessageAndMetadata[K, V]], timeoutEnabled))
    }
  }
}

class MockOffsetManager(mk: MockKafka) extends OffsetManager(mk.consumerConfig) {


  def findPartitions(topic: String): Seq[PartitionMetadata] =
    mk.data.keySet.filter(_.topic == topic)
      .map(tap => new PartitionMetadata(tap.partition, None, Seq.empty)).toSeq

  private var savedOffsets = Map.empty[TopicAndPartition, OffsetAndMetadata]

  override def getOffsets(topic: String, when: RequestedOffset): Offsets =
    getOffsets(topic, findPartitions(topic), when)

  override def getOffsets(topic: String, partitions: Seq[PartitionMetadata], when: RequestedOffset): Offsets = when match {
    case GroupOffset      => getGroupOffsets(partitions.map(p => TopicAndPartition(topic, p.partitionId)), config)
    case EarliestOffset   => getOffsetsBefore(topic, partitions, OffsetRequest.EarliestTime, config)
    case LatestOffset     => getLatestOffset(topic, partitions)
    case DateOffset(date) => getOffsetsBefore(topic, partitions, date, config)
    case FindOffset(pred) => findOffsets(topic, partitions, pred, config)
    case _                => throw new NotImplementedError()
  }

  override def commitOffsets(offsets: Map[TopicAndPartition, OffsetAndMetadata], isAutoCommit: Boolean): Unit =
    savedOffsets ++= offsets

  override def close(): Unit = {}

  def getGroupOffsets(partitions: Seq[TopicAndPartition], config: ConsumerConfig): Offsets =
    partitions.map(tap => (tap, savedOffsets.get(tap).map(_.offset).getOrElse(0L))).toMap

  def getOffsetsBefore(topic: String, partitions: Seq[PartitionMetadata], time: Long, config: ConsumerConfig): Offsets = ???

  def getLatestOffset(topic: String, partitions: Seq[PartitionMetadata]): Offsets =
    if (partitions.nonEmpty) {
      partitions.map(pm => new TopicAndPartition(topic, pm.partitionId))
        .map(tap => tap -> mk.latestOffset(tap)).toMap
    } else {
      Map(new TopicAndPartition(topic, 0) -> 0L)
    }

  def findOffsets(topic: String,
                  partitions: Seq[PartitionMetadata],
                  predicate: MessagePredicate,
                  config: ConsumerConfig): Offsets =

    partitions.flatMap { partition =>
      val tap = TopicAndPartition(topic, partition.partitionId)
      val bounds = (0L, mk.data.get(tap).map(_.size : Long).getOrElse(0L))
      binaryOffsetSearch(tap, predicate, bounds).map(tap -> _)
    }.toMap

  @tailrec
  private def binaryOffsetSearch(tap: TopicAndPartition,
                                 predicate: MessagePredicate,
                                 bounds: (Long, Long)): Option[Long] = {

    val (start, end) = bounds
    val offset = (start + end) / 2
    val maxBytes = mk.consumerConfig.fetchMessageMaxBytes

    val result = mk.fetch(tap, offset, maxBytes) match {
      case messageSet if messageSet.nonEmpty =>
        logger.debug(s"checking $bounds found ${messageSet.map(_.offset).mkString(",")}")
        val messages = messageSet.toSeq

        val head = messages.head
        val headCheck = predicate(head.message)
        lazy val last = messages.last
        lazy val lastCheck = predicate(last.message)
        lazy val count = messages.length

        if (headCheck == 0) {
          Right(head.offset)
        } else if (headCheck > 0) {
          // look left
          if (start == end || start == offset) {
            // no more ranges left to check
            Right(head.offset)
          } else {
            Left((start, offset))
          }
        } else if (lastCheck == 0) {
          Right(last.offset)
        } else if (lastCheck < 0) {
          // look right
          if (start == end || start == offset || offset + count >= end) {
            // no more ranges left to check
            Right(last.offset)
          } else {
            Left((offset + count, end))
          }
        } else {
          // it's in this block
          Right(messages.tail.find(m => predicate(m.message) >= 0).map(_.offset).get)
        }

      case _ => Right(-1L) // no messages found
    }

    result match {
      case Right(found) if found != -1 => Some(found)
      case Right(found)                => None
      case Left(nextBounds)            => binaryOffsetSearch(tap, predicate, nextBounds)
    }
  }

}