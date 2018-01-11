/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{Executors, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.errors.WakeupException
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.lambda.stream.OffsetManager
import org.locationtech.geomesa.lambda.stream.kafka.KafkaFeatureCache.WritableFeatureCache
import org.locationtech.geomesa.lambda.stream.kafka.KafkaStore.MessageTypes
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import scala.util.control.NonFatal

/**
  * Consumes from kakfa and populates the local cache
  *   1. reads offsets stored in zk on startup
  *   2. scheduled repeating - reads features from kafka, adds to in-memory cache
  *   3. listens for offsets change in zk, removes expired features from in-memory cache
  *
  * @param offsetManager offset manager
  * @param serializer feature serializer
  * @param cache shared state
  * @param config kafka consumer config
  * @param topic kafka topic
  */
class KafkaCacheLoader(offsetManager: OffsetManager,
                       serializer: KryoFeatureSerializer,
                       cache: WritableFeatureCache,
                       config: Map[String, String],
                       topic: String,
                       parallelism: Int) extends Closeable with LazyLogging {

  private val running = new AtomicInteger(0)

  private val frequency = SystemProperty("geomesa.lambda.load.interval").toDuration.map(_.toMillis).getOrElse(100L)

  private val executor = Executors.newScheduledThreadPool(parallelism)

  private val consumers = KafkaStore.consumers(config, topic, offsetManager, parallelism, cache.partitionAssigned)

  private val schedules = consumers.map { c =>
    executor.scheduleWithFixedDelay(new ConsumerRunnable(c), frequency, frequency, TimeUnit.MILLISECONDS)
  }

  override def close(): Unit = {
    consumers.foreach(_.wakeup())
    schedules.foreach(_.cancel(true))
    executor.shutdownNow()
    // ensure run has finished so that we don't get ConcurrentAccessExceptions closing the consumer
    while (running.get > 0) {
      Thread.sleep(10)
    }
    consumers.foreach(_.close())
    executor.awaitTermination(1, TimeUnit.SECONDS)
  }

  class ConsumerRunnable(val consumer: Consumer[Array[Byte], Array[Byte]]) extends Runnable {
    override def run(): Unit = {
      running.getAndIncrement()
      try {
        val result = consumer.poll(0)
        logger.trace(s"Consumer poll received ${result.count()} records")
        val records = result.iterator
        while (records.hasNext) {
          val record = records.next()
          val (time, action) = KafkaStore.deserializeKey(record.key)
          val feature = serializer.deserialize(record.value)
          action match {
            case MessageTypes.Write  => cache.add(feature, record.partition, record.offset, time)
            case MessageTypes.Delete => cache.delete(feature, record.partition, record.offset, time)
            case _ => logger.error(s"Unhandled message type: $action")
          }
        }
        logger.trace(s"Consumer finished processing ${result.count()} records")
        // we commit the offsets so that the next poll doesn't return the same records
        // on init we roll back to the last offsets persisted to storage
        consumer.commitSync()
        logger.trace(s"Consumer committed offsets")
      } catch {
        case _: WakeupException | _: InterruptedException => // ignore
        case NonFatal(e)        => logger.warn("Error receiving message from kafka:", e)
      } finally {
        running.decrementAndGet()
      }
    }
  }
}
