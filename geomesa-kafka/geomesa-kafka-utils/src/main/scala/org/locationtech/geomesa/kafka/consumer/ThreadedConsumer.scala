/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.consumer

import java.time.Duration

import com.typesafe.scalalogging.Logger
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{InterruptException, WakeupException}
import org.locationtech.geomesa.kafka.KafkaConsumerVersions
import org.locationtech.geomesa.kafka.consumer.ThreadedConsumer.{ConsumerErrorHandler, LogOffsetCommitCallback}

import scala.util.control.NonFatal

abstract class ThreadedConsumer(
    consumers: Seq[Consumer[Array[Byte], Array[Byte]]],
    frequency: Duration,
    closeConsumers: Boolean = true
  ) extends BaseThreadedConsumer(consumers) {

  import scala.collection.JavaConverters._

  private val callback = new LogOffsetCommitCallback(logger)

  override protected def createConsumerRunnable(
      id: String,
      consumer: Consumer[Array[Byte], Array[Byte]],
      handler: ConsumerErrorHandler): Runnable = {
    new ConsumerRunnable(id, consumer, handler)
  }

  protected def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit

  class ConsumerRunnable(id: String, consumer: Consumer[Array[Byte], Array[Byte]], handler: ConsumerErrorHandler)
      extends Runnable {

    private var errorCount = 0

    lazy private val topics = consumer.subscription().asScala.mkString(", ")

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
                consume(records.next())
              }
              logger.trace(s"Consumer [$id] finished processing ${result.count()} records from topic $topics")
              consumer.commitAsync(callback)
              errorCount = 0 // reset error count
            }
          } catch {
            case _: WakeupException | _: InterruptException | _: InterruptedException => interrupted = true
            case NonFatal(e) =>
              errorCount += 1
              logger.warn(s"Consumer [$id] error receiving message from topic $topics:", e)
              if (errorCount <= 300 || handler.handle(id, e)) {
                Thread.sleep(1000)
              } else {
                logger.error(s"Consumer [$id] shutting down due to too many errors from topic $topics:", e)
                throw e
              }
          }
        }
      } finally {
        if (closeConsumers) {
          try { consumer.close() } catch {
            case NonFatal(e) => logger.warn(s"Error calling close on consumer: ", e)
          }
        }
      }
    }
  }
}

object ThreadedConsumer {

  import scala.collection.JavaConverters._

  /**
    * Handler for asynchronous errors in the consumer threads
    */
  trait ConsumerErrorHandler {

    /**
      * Invoked on a fatal error
      *
      * @param consumer consumer identifier
      * @param e exception
      * @return true to continue processing, false to terminate the consumer
      */
    def handle(consumer: String, e: Throwable): Boolean
  }

  class LoggingConsumerErrorHandler(logger: Logger, topics: Seq[String]) extends ConsumerErrorHandler {
    override def handle(consumer: String, e: Throwable): Boolean = {
      logger.error(s"Consumer [$consumer] error receiving message from topic ${topics.mkString(", ")}:", e)
      false
    }
  }

  class LogOffsetCommitCallback(logger: Logger) extends OffsetCommitCallback() {
    override def onComplete(offsets: java.util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
      lazy val o = offsets.asScala.map { case (tp, om) => s"[${tp.topic}:${tp.partition}:${om.offset}]"}.mkString(",")
      if (exception == null) {
        logger.trace(s"Consumer committed offsets: $o")
      } else {
        logger.error(s"Consumer error committing offsets: $o : ${exception.getMessage}", exception)
      }
    }
  }
}
