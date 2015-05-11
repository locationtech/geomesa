/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.kafka.consumer

import java.util.concurrent.{BlockingQueue, TimeUnit}

import kafka.consumer.{ConsumerTimeoutException, FetchedDataChunk, PartitionTopicInfo}
import kafka.message.{MessageAndMetadata, MessageAndOffset}
import kafka.serializer.Decoder

import scala.collection.Iterator._

/**
 * Replacement for kafka.consumer.KafkaStream - the original uses private methods we can't access
 */
class KafkaStreamLike[K, V](protected[consumer] val queue: BlockingQueue[FetchedDataChunk],
                            timeoutMs: Long,
                            keyDecoder: Decoder[K],
                            valueDecoder: Decoder[V]) extends Iterable[MessageAndMetadata[K, V]] {

  override val iterator = new KafkaStreamIterator

  protected[consumer] def shutdown() = {
    queue.clear()
    queue.put(KafkaStreamLike.shutdownMessage)
  }

  class KafkaStreamIterator extends Iterator[MessageAndMetadata[K, V]] {
    ksi =>

    private var topicInfo: PartitionTopicInfo = null
    private var messages: Iterator[MessageAndOffset] = Iterator.empty

    override def hasNext = if (messages.hasNext) { true } else {
      try {
        val chunk = if (timeoutMs < 0) { queue.take() } else {
          Option(queue.poll(timeoutMs, TimeUnit.MILLISECONDS)).getOrElse(throw new ConsumerTimeoutException)
        }
        if (chunk.eq(KafkaStreamLike.shutdownMessage)) {
          queue.offer(chunk) // replace the shutdown for any other iterators using this queue
          false
        } else {
          topicInfo = chunk.topicInfo
          messages = chunk.messages.iterator
          hasNext
        }
      } catch {
        case e: InterruptedException =>
          Thread.currentThread().interrupt()
          false
      }
    }

    override def next() = {
      val topic = topicInfo.topic
      val partition = topicInfo.partitionId
      val message = messages.next()
      topicInfo.resetConsumeOffset(message.nextOffset)
      MessageAndMetadata(topic, partition, message.message, message.offset, keyDecoder, valueDecoder)
    }

    /** @param p the predicate defining when to stop
      *
      * @return a [[Iterator]] which will delegate to ``this`` and return all elements up to and including
      *         the first element ``a`` for which p(a) == true after which no more calls will be made
      *         to ``this.hasNext()``
      */
    def stopAfter(p: MessageAndMetadata[K, V] => Boolean): Iterator[MessageAndMetadata[K, V]] = new Iterator[MessageAndMetadata[K, V]] {
      private var stop = false

      override def hasNext = !stop && ksi.hasNext

      override def next() = {
        if (!stop) {
          val next = ksi.next()
          stop = p(next)
          next
        } else {
          empty.next()
        }
      }
    }
  }
}

object KafkaStreamLike {
  val shutdownMessage = FetchedDataChunk(null, null, -1)
}