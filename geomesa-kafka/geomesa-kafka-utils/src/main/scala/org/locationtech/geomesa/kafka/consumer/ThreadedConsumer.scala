/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.consumer

import java.io.Closeable
import java.time.Duration
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord, OffsetAndMetadata, OffsetCommitCallback}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{InterruptException, WakeupException}
import org.locationtech.geomesa.kafka.KafkaConsumerVersions
import org.locationtech.geomesa.kafka.consumer.ThreadedConsumer.ConsumerErrorHandler

import scala.util.control.NonFatal

abstract class ThreadedConsumer(
    consumers: Seq[Consumer[Array[Byte], Array[Byte]]],
    frequency: Duration,
    closeConsumers: Boolean = true
  ) extends Closeable with LazyLogging {

  import scala.collection.JavaConverters._

  protected def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit

  private val topics = scala.collection.mutable.Set.empty[String]

  private val executor: ExecutorService = Executors.newFixedThreadPool(consumers.length)

  @volatile
  private var open = true

  private val commitCallback = new OffsetCommitCallback() {
    override def onComplete(offsets: java.util.Map[TopicPartition, OffsetAndMetadata], exception: Exception): Unit = {
      lazy val o = offsets.asScala.map { case (tp, om) => s"[${tp.topic}:${tp.partition}:${om.offset}]"}.mkString(",")
      if (exception == null) {
        logger.trace(s"Consumer committed offsets: $o")
      } else {
        logger.error(s"Consumer error committing offsets: $o : ${exception.getMessage}", exception)
      }
    }
  }

  def startConsumers(handler: Option[ConsumerErrorHandler] = None): Unit = {
    val format = if (consumers.lengthCompare(10) > 0) { "%02d" } else { "%d" }
    var i = 0
    consumers.foreach { c =>
      c.subscription().asScala.foreach(topics.add)
      executor.execute(new ConsumerRunnable(c, String.format(format, Int.box(i)), handler))
      i += 1
    }
    logger.debug(s"Started $i consumer(s) on topic ${topics.mkString(", ")}")
  }

  override def close(): Unit = {
    open = false
    executor.shutdown()
    executor.awaitTermination(Long.MaxValue, TimeUnit.SECONDS)
  }

  class ConsumerRunnable(
      consumer: Consumer[Array[Byte], Array[Byte]],
      id: String,
      handler: Option[ConsumerErrorHandler]
    ) extends Runnable {

    private var errorCount = 0

    override def run(): Unit = {
      try {
        var interrupted = false
        while (open && !interrupted) {
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
              // we commit the offsets so that the next poll doesn't return the same records
              consumer.commitAsync(commitCallback)
              errorCount = 0 // reset error count
            }
          } catch {
            case _: WakeupException | _: InterruptException | _: InterruptedException => interrupted = true
            case NonFatal(e) =>
              errorCount += 1
              logger.warn(s"Consumer [$id] error receiving message from topic ${topics.mkString(", ")}:", e)
              if (errorCount <= 300 || handler.exists(_.handle(id, e))) {
                Thread.sleep(1000)
              } else {
                logger.error(s"Consumer [$id] shutting down due to too many errors from topic ${topics.mkString(", ")}:", e)
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
}
