/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.consumer

import java.time.Duration
import java.util.Collections
import java.util.concurrent._

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.apache.kafka.common.errors.{InterruptException, WakeupException}
import org.locationtech.geomesa.kafka.KafkaConsumerVersions
import org.locationtech.geomesa.kafka.consumer.ThreadedConsumer.{ConsumerErrorHandler, LogOffsetCommitCallback}

import scala.util.control.NonFatal

/**
 * Consumer that will process messages in batch, with guaranteed at-least-once processing
 *
 * @param consumers consumers
 * @param frequency poll frequency
 */
abstract class BatchConsumer(consumers: Seq[Consumer[Array[Byte], Array[Byte]]], frequency: Duration)
    extends BaseThreadedConsumer(consumers) {

  import scala.collection.JavaConverters._

  private val messages =
    Collections.newSetFromMap(
      new ConcurrentHashMap[ConsumerRecord[Array[Byte], Array[Byte]], java.lang.Boolean]()).asScala

  private val barrier = new CyclicBarrier(consumers.length, new ConsumerCoordinator())
  private val callback = new LogOffsetCommitCallback(logger)

  override protected def createConsumerRunnable(
      id: String,
      consumer: Consumer[Array[Byte], Array[Byte]],
      handler: ConsumerErrorHandler): Runnable = {
    new ConsumerRunnable(id, consumer, handler)
  }

  /**
   * Consume a batch of records. If this method returns false, the messages will be replayed
   * on a subsequent call.
   *
   * @param records records
   * @return true if messages were processed, false otherwise
   */
  protected def consume(records: Seq[ConsumerRecord[Array[Byte], Array[Byte]]]): Boolean

  /**
   * Invokes a callback on a batch of messages and commits offsets
   */
  class ConsumerCoordinator extends Runnable {
    override def run(): Unit = {
      if (messages.nonEmpty) {
        try {
          if (consume(messages.toSeq.sortBy(_.offset))) {
            consumers.foreach { c =>
              try { c.commitAsync(callback) } catch {
                case NonFatal(e) => logger.error("Error committing offsets:", e)
              }
            }
            messages.clear()
          }
        } catch {
          case NonFatal(e) => logger.error("Error processing message batch:", e)
        }
      }
    }
  }

  class ConsumerRunnable(id: String, consumer: Consumer[Array[Byte], Array[Byte]], handler: ConsumerErrorHandler)
      extends Runnable {

    override def run(): Unit = {
      try {
        var interrupted = false
        while (isOpen && !interrupted) {
          try {
            val result = KafkaConsumerVersions.poll(consumer, frequency)
            lazy val topics = result.partitions.asScala.map(tp => s"[${tp.topic}:${tp.partition}]").mkString(",")
            logger.debug(s"Consumer [$id] poll received ${result.count()} records for $topics")
            if (!result.isEmpty) {
              val records = result.iterator()
              while (records.hasNext) {
                messages += records.next()
              }
              logger.trace(s"Consumer [$id] finished processing ${result.count()} records from topic $topics")
            }
          } catch {
            case _: WakeupException | _: InterruptException | _: InterruptedException => interrupted = true
            case NonFatal(e) => if (!handler.handle(id, e)) { interrupted = true }
          } finally {
            if (!interrupted) {
              logger.trace(s"Consumer [$id] waiting on barrier")
              try { barrier.await() } catch {
                case _: BrokenBarrierException | _: InterruptedException => interrupted = true
              }
              logger.trace(s"Consumer [$id] passed barrier")
            }
          }
        }
      } finally {
        try { consumer.close() } catch {
          case NonFatal(e) => logger.warn(s"Error calling close on consumer: ", e)
        }
      }
    }
  }
}
