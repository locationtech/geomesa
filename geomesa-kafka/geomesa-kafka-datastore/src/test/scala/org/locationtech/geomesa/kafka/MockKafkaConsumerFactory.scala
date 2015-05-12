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

import kafka.api._
import kafka.common.{OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.ConsumerConfig
import kafka.message.{Message, MessageAndMetadata, MessageAndOffset}
import kafka.producer.KeyedMessage
import kafka.serializer.Decoder
import org.locationtech.geomesa.kafka.MockKafka.KeyAndMessage
import org.locationtech.geomesa.kafka.consumer.KafkaConsumer._
import org.locationtech.geomesa.kafka.consumer._
import org.locationtech.geomesa.kafka.consumer.offsets.FindOffset._
import org.locationtech.geomesa.kafka.consumer.offsets.OffsetManager._
import org.locationtech.geomesa.kafka.consumer.offsets._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

class MockKafkaConsumerFactory(val mk: MockKafka)
  extends KafkaConsumerFactory("mock-broker:9092", "mock-zoo") {

  import KafkaConsumerFactory._

  override def kafkaConsumer(topic: String) =
    new MockKafkaConsumer[Array[Byte], Array[Byte]](mk, topic, defaultDecoder, defaultDecoder)

  override val offsetManager = mock(classOf[OffsetManager])
}

object MockKafka {
  type KeyAndMessage = (Array[Byte], Array[Byte])
}

class MockKafka {

  var data: Map[TopicAndPartition, Seq[KeyAndMessage]] = Map.empty
  
  def nextOffset(tap: TopicAndPartition): Long = data.get(tap).map(_.size : Long).getOrElse(0L)

  val kafkaConsumerFactory = new MockKafkaConsumerFactory(this)
  val consumerConfig = kafkaConsumerFactory.config

  /** Simulate sending a message to a producer
    */
  def send(keyedMessage: KeyedMessage[Array[Byte], Array[Byte]], partition: Int = 0): Unit = {
    val tap = new TopicAndPartition(keyedMessage.topic, partition)
    val msg = new Message(keyedMessage.message, keyedMessage.key)

    val msgs = data.getOrElse(tap, Seq.empty) :+ (keyedMessage.key, keyedMessage.message)
    data += (tap -> msgs)
  }

  def fetch(tap: TopicAndPartition): Iterable[MessageAndOffset] = {
    data.get(tap).map { seq =>
      seq
        .zipWithIndex
        .map { case (km, o) => new MessageAndOffset(new Message(km._2, km._1), o) }
    }.getOrElse(Seq.empty)
  }

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

object MockKafkaStream {

  def apply[K, V](data: Seq[KeyedMessage[K, V]], offset: Int = 0): KafkaStreamLike[K, V] = {
    apply(data.drop(offset).map { km =>
      val mam = mock(classOf[MessageAndMetadata[K, V]])
      when(mam.key()).thenReturn(km.key)
      when(mam.message()).thenReturn(km.message)
      mam
    })
  }

  def apply[K, V](data: Seq[KeyAndMessage], keyDecoder: Decoder[K], valueDecoder: Decoder[V]): KafkaStreamLike[K, V] = {
    apply(data.map { km =>
      val mam = mock(classOf[MessageAndMetadata[K, V]])
      val k = keyDecoder.fromBytes(km._1)
      val v = valueDecoder.fromBytes(km._2)

      when(mam.key()).thenReturn(k)
      when(mam.message()).thenReturn(v)
      mam
    })
  }

  def apply[K, V](data: Seq[MessageAndMetadata[K, V]]): KafkaStreamLike[K, V] = {
    val ksl = mock(classOf[KafkaStreamLike[K, V]])

    // Kafka consumer iterators will block until there is a new message
    // for testing, throw an exception if a consumer tries to read beyond ``data``
    val end: Iterator[MessageAndMetadata[K, V]] = new Iterator[MessageAndMetadata[K, V]] {

      override def hasNext: Boolean =
        throw new IllegalStateException("Attempting to read beyond given mock data.")

      override def next(): MessageAndMetadata[K, V] =
        throw new IllegalStateException("Attempting to read beyond given mock data.")
    }

    when(ksl.iterator).thenAnswer(new Answer[Iterator[MessageAndMetadata[K, V]]] {

      override def answer(invocation: InvocationOnMock): Iterator[MessageAndMetadata[K, V]] = {
        data.iterator ++ end
      }
    })

    ksl
  }

}

class MockKafkaConsumer[K, V](mk: MockKafka, topic: String, keyDecoder: Decoder[K], valueDecoder: Decoder[V])
  extends KafkaConsumer(topic, mk.consumerConfig, keyDecoder, valueDecoder) {

  override def createMessageStreams(numStreams: Int,
                                    startFrom: RequestedOffset = GroupOffset): List[KafkaStreamLike[K, V]] = {

    val offsets = mk.kafkaConsumerFactory.offsetManager.getOffsets(topic, startFrom)
    val partitions = offsets.keys.toArray
    
    val numPartitionsPerStream =
      partitions.length / numStreams + (if (partitions.length % numStreams == 0) 0 else 1)

    val streams = ArrayBuffer.empty[KafkaStreamLike[K, V]]

    partitions.grouped(numPartitionsPerStream).foreach { streamPartitions =>

      val data = streamPartitions.foldLeft(Seq.empty[KeyAndMessage]) { (all, tap) =>
        all ++ mk.data(tap).drop(offsets(tap).asInstanceOf[Int])
      }

      streams.append(MockKafkaStream(data, keyDecoder, valueDecoder))
    }

    streams.toList
  }

  def createMessageStreams(topic: String, numStreams: Int, startFrom: Offsets): List[KafkaStreamLike[K, V]] = ???
}

abstract class MockOffsetManager(mk: MockKafka) extends OffsetManager(mk.consumerConfig) {


  def findPartitions(topic: String): Seq[PartitionMetadata]

  private var savedOffsets = Map.empty[TopicAndPartition, OffsetAndMetadata]

  override def getOffsets(topic: String, when: RequestedOffset): Offsets =
    getOffsets(topic, findPartitions(topic), when)

  override def getOffsets(topic: String, partitions: Seq[PartitionMetadata], when: RequestedOffset): Offsets = when match {
    case GroupOffset      => getGroupOffsets(partitions.map(p => TopicAndPartition(topic, p.partitionId)), config)
    case EarliestOffset   => getOffsetsBefore(topic, partitions, OffsetRequest.EarliestTime, config)
    case LatestOffset     => getOffsetsBefore(topic, partitions, OffsetRequest.LatestTime, config)
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