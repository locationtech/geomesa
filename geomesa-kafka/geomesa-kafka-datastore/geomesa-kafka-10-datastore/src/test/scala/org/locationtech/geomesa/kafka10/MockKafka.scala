/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka10

import java.util.concurrent.atomic.AtomicLong

import kafka.api._
import kafka.common.{LongRef, OffsetAndMetadata, TopicAndPartition}
import kafka.consumer.{ConsumerConfig, ConsumerTimeoutException, SimpleConsumer}
import kafka.message._
import kafka.producer.KeyedMessage
import kafka.serializer.Decoder
import org.locationtech.geomesa.kafka10.consumer.offsets.FindOffset.MessagePredicate
import org.locationtech.geomesa.kafka10.consumer.offsets.OffsetManager.Offsets
import org.locationtech.geomesa.kafka10.consumer.offsets._
import org.locationtech.geomesa.kafka10.consumer.{KafkaConsumer, KafkaConsumerFactory, KafkaStreamLike, WrappedConsumer}
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer

import scala.collection.mutable.ArrayBuffer
import scala.util.{Success, Try}

class MockKafka {

  var data: Map[TopicAndPartition, Seq[MockMessage]] = Map.empty

  /** @return the latest offset for the given ``tap``, same as Kafka would */
  def latestOffset(tap: TopicAndPartition): Long = data.get(tap).map(_.size : Long).getOrElse(0L)

  val kafkaConsumerFactory = new MockKafkaConsumerFactory(this)

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

  def asMessage: Message = new Message(msg, key, System.currentTimeMillis(), Message.CurrentMagicValue)

  def asMessageAndOffset: MessageAndOffset = new MessageAndOffset(asMessage, offset)

  def asMessageAndMetadata[K, V](keyDecoder: Decoder[K], valueDecoder: Decoder[V]): MessageAndMetadata[K, V] =
    new MessageAndMetadata[K, V](tap.topic, tap.partition, asMessage, offset, keyDecoder, valueDecoder)
}

object MockMessage {
  
  def apply(tap: TopicAndPartition, kMessage: KeyedMessage[Array[Byte], Array[Byte]], offset: Long): MockMessage =
    MockMessage(tap, kMessage.key, kMessage.message, offset)

}

class MockKafkaConsumerFactory(val mk: MockKafka)
  extends KafkaConsumerFactory("mock-broker:9092", "mock-zoo", "largest") {

  import KafkaConsumerFactory._

  override def kafkaConsumer(topic: String, extraConfig: Map[String, String] = Map.empty) =
    MockKafkaConsumer[Array[Byte], Array[Byte]](mk, topic, defaultDecoder, defaultDecoder, extraConfig)

  // must pass config directly - it cannot be accessed through mk because mk isn't fully initialized yet
  override val offsetManager = new MockOffsetManager(mk, config)
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

class MockOffsetManager(mk: MockKafka, consumerConfig: ConsumerConfig) extends OffsetManager(consumerConfig) {


  def findPartitions(topic: String): Seq[PartitionMetadata] =
    mk.data.keySet.filter(_.topic == topic)
      .map(tap => new PartitionMetadata(tap.partition, None, Seq.empty)).toSeq

  private var savedOffsets = Map.empty[TopicAndPartition, OffsetAndMetadata]

  override def getOffsets(topic: String, when: RequestedOffset): Offsets =
    getOffsets(topic, findPartitions(topic), when)

  override def getOffsets(topic: String, partitions: Seq[PartitionMetadata], when: RequestedOffset): Offsets = when match {
    case GroupOffset       => getGroupOffsets(partitions.map(p => TopicAndPartition(topic, p.partitionId)), config)
    case EarliestOffset    => getOffsetsBefore(topic, partitions, OffsetRequest.EarliestTime, config)
    case LatestOffset      => getLatestOffset(topic, partitions)
    case DateOffset(date)  => getOffsetsBefore(topic, partitions, date, config)
    case FindOffset(pred)  => findOffsets(topic, partitions, pred, config)
    case SpecificOffset(o) => partitions.map(p => TopicAndPartition(topic, p.partitionId)).map(_ -> o).toMap
    case _                 => throw new NotImplementedError()
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

  override def findOffsets(topic: String,
                  partitions: Seq[PartitionMetadata],
                  predicate: MessagePredicate,
                  config: ConsumerConfig): Offsets =

    partitions.flatMap { partition =>
      val tap = TopicAndPartition(topic, partition.partitionId)
      val bounds = (0L, mk.data.get(tap).map(_.size : Long).getOrElse(0L))
      val consumer = WrappedConsumer(null, tap, config)

      binaryOffsetSearch(consumer, predicate, bounds).map(tap -> _)
    }.toMap

  override def fetch(consumer: SimpleConsumer,
                     topic: String,
                     partition: Int,
                     offset: Long,
                     maxBytes: Int): Try[ByteBufferMessageSet] = {

    val offsetCounter = new AtomicLong(offset)
    val msgs = mk.fetch(new TopicAndPartition(topic, partition), offset, maxBytes).map(_.message).toArray

    Success(new ByteBufferMessageSet(NoCompressionCodec, new LongRef(offsetCounter.get()), msgs: _*))
  }
}