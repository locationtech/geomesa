/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka09.consumer

import java.util.concurrent.{BlockingQueue, TimeUnit}

import kafka.consumer.{ConsumerTimeoutException, FetchedDataChunk, PartitionTopicInfo}
import kafka.message.{MessageAndMetadata, MessageAndOffset}
import kafka.serializer.Decoder

/**
 * Replacement for kafka.consumer.KafkaStream - the original uses private methods we can't access
 */
class KafkaStreamLike[K, V](protected[consumer] val queue: BlockingQueue[FetchedDataChunk],
                            timeoutMs: Long,
                            keyDecoder: Decoder[K],
                            valueDecoder: Decoder[V]) extends Iterable[MessageAndMetadata[K, V]] {

  override val iterator: Iterator[MessageAndMetadata[K, V]] = new KafkaStreamIterator

  protected[consumer] def shutdown() = {
    queue.clear()
    queue.put(KafkaStreamLike.shutdownMessage)
  }

  class KafkaStreamIterator extends Iterator[MessageAndMetadata[K, V]] {

    private var topicInfo: PartitionTopicInfo = null
    private var messages: Iterator[MessageAndOffset] = Iterator.empty

    override def hasNext: Boolean = if (messages.hasNext) { true } else {
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
      if (!hasNext) throw new NoSuchElementException

      val topic = topicInfo.topic
      val partition = topicInfo.partitionId
      val message = messages.next()
      topicInfo.resetConsumeOffset(message.nextOffset)
      MessageAndMetadata(topic, partition, message.message, message.offset, keyDecoder, valueDecoder)
    }
  }
}

object KafkaStreamLike {
  val shutdownMessage = FetchedDataChunk(null, null, -1)

  implicit class KafkaStreamLikeIterator[K, V](ksi: Iterator[MessageAndMetadata[K, V]]) {

    def stopOnTimeout: Iterator[MessageAndMetadata[K, V]] = new Iterator[MessageAndMetadata[K, V]] {
      private var timeout = false

      override def hasNext = {
        try {
          // once timeout has occurred, don't try again
          !timeout && ksi.hasNext
        } catch {
          case e: ConsumerTimeoutException =>
            timeout = true
            false
        }
      }

      override def next() = {
        if (!hasNext) throw new NoSuchElementException

        ksi.next()
      }
    }

  }
}
