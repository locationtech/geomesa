/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.consumer

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.apache.kafka.common.errors.WakeupException

import scala.util.control.NonFatal

trait ThreadedConsumer extends Closeable with LazyLogging {

  protected val consumers: Seq[Consumer[Array[Byte], Array[Byte]]]
  protected val topic: String
  protected val frequency: Long

  protected def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit

  private val closed = new AtomicBoolean(false)

  private val executor = Executors.newFixedThreadPool(consumers.length)

  consumers.foreach(c => executor.execute(new ConsumerRunnable(c, frequency)))

  logger.debug(s"Started ${consumers.length} consumer(s) on topic $topic")

  override def close(): Unit = {
    closed.set(true)
    consumers.foreach(_.wakeup())
    executor.shutdownNow()
    executor.awaitTermination(1, TimeUnit.SECONDS)
  }

  class ConsumerRunnable(val consumer: Consumer[Array[Byte], Array[Byte]], frequency: Long) extends Runnable {

    private var errorCount = 0

    override def run(): Unit = {
      try {
        while (!closed.get()) {
          try {
            val result = consumer.poll(frequency)
            logger.debug(s"Consumer poll received ${result.count()} records from topic $topic")
            if (!result.isEmpty) {
              val records = result.iterator()
              while (records.hasNext) {
                consume(records.next())
              }
              // we commit the offsets so that the next poll doesn't return the same records
              consumer.commitAsync()
              logger.trace(s"Consumer finished processing ${result.count()} records from topic $topic")
              errorCount = 0 // reset error count
            }
          } catch {
            case e: WakeupException => throw e
            case e: InterruptedException => throw e
            case NonFatal(e) =>
              logger.warn(s"Error receiving message from topic $topic:", e)
              errorCount += 1
              if (errorCount < 300) { Thread.sleep(1000) } else {
                logger.error("Shutting down due to too many errors")
                throw new InterruptedException("Too many errors")
              }
          }
        }
      } catch {
        case _: WakeupException | _: InterruptedException => // return
      } finally {
        consumer.close()
      }
    }
  }
}
