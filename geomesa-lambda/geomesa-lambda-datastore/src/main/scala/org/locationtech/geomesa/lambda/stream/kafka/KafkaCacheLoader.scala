/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.stream.kafka

import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.errors.WakeupException
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.lambda.stream.OffsetManager
import org.locationtech.geomesa.lambda.stream.OffsetManager.OffsetListener
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
  * @param state shared state
  * @param config kafka consumer config
  * @param topic kafka topic
  */
class KafkaCacheLoader(offsetManager: OffsetManager,
                       serializer: KryoFeatureSerializer,
                       state: SharedState,
                       config: Map[String, String],
                       topic: String,
                       parallelism: Int) extends OffsetListener with Closeable with LazyLogging {

  private val running = new AtomicInteger(0)

  private val offsets = new ConcurrentHashMap[Int, Long]()

  private val frequency = SystemProperty("geomesa.lambda.load.interval").toDuration.getOrElse(100L)

  private val consumers =
    KafkaStore.consumers(config, topic, offsetManager, state, parallelism).map(new ConsumerRunnable(_))

  // register as a listener for offset changes
  offsetManager.addOffsetListener(topic, this)

  private val schedules =
    consumers.map(KafkaStore.executor.scheduleWithFixedDelay(_, 0L, frequency, TimeUnit.MILLISECONDS))

  override def offsetChanged(partition: Int, offset: Long): Unit = {
    // remove the expired features from the cache
    var current = offsets.get(partition)
    logger.trace(s"Offsets changed for [$topic:$partition]: $current->$offset")
    if (current < offset) {
      offsets.put(partition, offset)
      do {
        state.remove(partition, current)
        current += 1
      } while (current < offset)
    }
    logger.trace(s"Current size for [$topic]: ${state.debug()}")
  }

  override def close(): Unit = {
    consumers.foreach(_.consumer.wakeup())
    schedules.foreach(_.cancel(true))
    offsetManager.removeOffsetListener(topic, this)
    // ensure run has finished so that we don't get ConcurrentAccessExceptions closing the consumer
    while (running.get > 0) {
      Thread.sleep(10)
    }
    consumers.foreach(_.consumer.close())
  }

  class ConsumerRunnable(val consumer: Consumer[Array[Byte], Array[Byte]]) extends Runnable {
    override def run(): Unit = {
      running.getAndIncrement()
      try {
        val records = consumer.poll(0).iterator
        while (records.hasNext) {
          val record = records.next()
          val (time, action) = KafkaStore.deserializeKey(record.key)
          val feature = serializer.deserialize(record.value)
          action match {
            case MessageTypes.Write  =>
              logger.trace(s"Adding [${record.partition}:${record.offset}] $feature created at " +
                  new DateTime(time, DateTimeZone.UTC))
              state.add(feature, record.partition, record.offset, time)

            case MessageTypes.Delete =>
              logger.trace(s"Deleting [${record.partition}:${record.offset}] $feature created at " +
                  new DateTime(time, DateTimeZone.UTC))
              state.delete(feature, record.partition, record.offset, time)

            case _ => logger.error(s"Unhandled message type: $action")
          }
        }
        // we commit the offsets so that the next poll doesn't return the same records
        // on init we roll back to the last offsets persisted to storage
        consumer.commitSync()
      } catch {
        case _: WakeupException => // ignore
        case NonFatal(e)        => logger.warn("Error receiving message from kafka", e)
      } finally {
        running.decrementAndGet()
      }
    }
  }
}
