/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.kafka.consumer.BatchConsumer.BatchResult

/**
 * Consumer that will process messages in batch, with guaranteed at-least-once processing
 *
 * @param consumers consumers
 * @param frequency poll frequency
 */
abstract class BatchConsumer(consumers: Seq[Consumer[Array[Byte], Array[Byte]]], frequency: Duration)
    extends BaseThreadedConsumer(consumers) {

  import BatchResult.BatchResult

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
   * Consume a batch of records.
   *
   * The response from this method will determine the continued processing of messages. If `Commit`
   * is returned, the batch is considered complete and won't be presented again. If `Continue` is
   * returned, the batch will be presented again in the future, and more messages will be read off the topic
   * in the meantime. If `Pause` is returned, the batch will be presented again in the future, but
   * no more messages will be read off the topic in the meantime.
   *
   * This method should return in a reasonable amount of time. If too much time is spent processing
   * messages, consumers may be considered inactive and be dropped from processing. See
   * https://kafka.apache.org/26/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html
   *
   * Note: if there is an error committing the batch or something else goes wrong, some messages may
   * be repeated in a subsequent call, regardless of the response from this method
   *
   * @param records records
   * @return commit, continue, or pause
   */
  protected def consume(records: Seq[ConsumerRecord[Array[Byte], Array[Byte]]]): BatchResult

  /**
   * Invokes a callback on a batch of messages and commits offsets
   */
  class ConsumerCoordinator extends Runnable {

    private var paused = false

    override def run(): Unit = {
      if (messages.nonEmpty) {
        try {
          consume(messages.toSeq.sortBy(_.offset)) match {
            case BatchResult.Commit   => resume(); commit(); messages.clear()
            case BatchResult.Continue => resume()
            case BatchResult.Pause    => pause()
          }
        } catch {
          case NonFatal(e) => logger.error("Error processing message batch:", e)
        }
      }
    }

    private def commit(): Unit ={
      consumers.foreach { c =>
        try { c.commitAsync(callback) } catch {
          case NonFatal(e) => logger.error("Error committing offsets:", e)
        }
      }
    }

    private def pause(): Unit = {
      if (!paused) {
        consumers.foreach { c =>
          try { c.pause(c.assignment()) } catch {
            case NonFatal(e) => logger.error("Error pausing consumer:", e)
          }
        }
        paused = true
      }
    }

    private def resume(): Unit = {
      if (paused) {
        consumers.foreach { c =>
          try { c.resume(c.assignment()) } catch {
            case NonFatal(e) => logger.error("Error resuming consumer:", e)
          }
        }
        paused = false
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

object BatchConsumer {
  object BatchResult extends Enumeration {
    type BatchResult = Value
    val Commit, Continue, Pause = Value
  }
}
