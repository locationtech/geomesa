/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.data

import java.io.Closeable
import java.util.Collections
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.{ConcurrentHashMap, Executors, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.errors.WakeupException
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.data.{FeatureEvent, FeatureListener}
import org.locationtech.geomesa.kafka.index.KafkaFeatureCache
import org.locationtech.geomesa.kafka.utils.GeoMessage.{Clear, Change, Delete}
import org.locationtech.geomesa.kafka.utils.{GeoMessageSerializer, KafkaFeatureEvent}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.control.NonFatal

/**
  * Reads from Kafka and populates a `KafkaFeatureCache`.
  * Manages geotools feature listeners
  */
trait KafkaCacheLoader extends Closeable with LazyLogging {

  private val listeners = Collections.newSetFromMap {
    new ConcurrentHashMap[(SimpleFeatureSource, FeatureListener), java.lang.Boolean]()
  }

  def cache: KafkaFeatureCache

  def addListener(source: SimpleFeatureSource, listener: FeatureListener): Unit =
    listeners.add((source, listener))

  def removeListener(source: SimpleFeatureSource, listener: FeatureListener): Unit =
    listeners.remove((source, listener))

  protected def fireEvent(message: Change): Unit = {
    if (!listeners.isEmpty) {
      fireEvent(KafkaFeatureEvent.changed(_, message.feature, message.timestamp.toEpochMilli))
    }
  }

  protected def fireEvent(message: Delete): Unit = {
    if (!listeners.isEmpty) {
      val removed = cache.query(message.id).orNull
      fireEvent(KafkaFeatureEvent.removed(_, message.id, removed, message.timestamp.toEpochMilli))
    }
  }

  protected def fireEvent(message: Clear): Unit = {
    if (!listeners.isEmpty) {
      fireEvent(KafkaFeatureEvent.cleared(_, message.timestamp.toEpochMilli))
    }
  }

  private def fireEvent(toEvent: (SimpleFeatureSource) => FeatureEvent): Unit = {
    import scala.collection.JavaConversions._
    val events = scala.collection.mutable.Map.empty[SimpleFeatureSource, FeatureEvent]
    listeners.foreach { case (source, listener) =>
      val event = events.getOrElseUpdate(source, toEvent(source))
      try { listener.changed(event) } catch {
        case NonFatal(e) => logger.error(s"Error in feature listener for $event", e)
      }
    }
  }
}

object KafkaCacheLoader {

  object NoOpLoader extends KafkaCacheLoader {
    override val cache: KafkaFeatureCache = KafkaFeatureCache.empty()
    override def addListener(source: SimpleFeatureSource, listener: FeatureListener): Unit = {}
    override def removeListener(source: SimpleFeatureSource, listener: FeatureListener): Unit = {}
    override def close(): Unit = {}
  }

  class KafkaCacheLoaderImpl(sft: SimpleFeatureType,
                             val cache: KafkaFeatureCache,
                             consumers: Seq[Consumer[Array[Byte], Array[Byte]]])
      extends KafkaCacheLoader with LazyLogging {

    private val running = new AtomicInteger(0)

    private val serializer = new GeoMessageSerializer(sft)

    private val topic = KafkaDataStore.topic(sft)
    private val frequency = SystemProperty("geomesa.kafka.load.interval").toDuration.map(_.toMillis).getOrElse(100L)

    private val executor = Executors.newScheduledThreadPool(consumers.length)

    private val schedules = consumers.zipWithIndex.map { case (c, i) =>
      executor.scheduleWithFixedDelay(new ConsumerRunnable(c, i), frequency, frequency, TimeUnit.MILLISECONDS)
    }

    logger.debug(s"Started ${consumers.length} consumer(s) on topic $topic")

    override def close(): Unit = {
      consumers.foreach(_.wakeup())
      schedules.foreach(_.cancel(true))
      executor.shutdownNow()
      // ensure run has finished so that we don't get ConcurrentAccessExceptions closing the consumer
      while (running.get > 0) {
        Thread.sleep(10)
      }
      consumers.foreach(_.close())
      cache.close()
      executor.awaitTermination(1, TimeUnit.SECONDS)
    }

    class ConsumerRunnable(val consumer: Consumer[Array[Byte], Array[Byte]], i: Int) extends Runnable {

      private var errorCount = 0

      override def run(): Unit = {
        running.getAndIncrement()
        try {
          val result = consumer.poll(0)
          logger.trace(s"Consumer poll received ${result.count()} records from topic $topic")
          val records = result.iterator
          while (records.hasNext) {
            val record = records.next()
            errorCount = 0 // reset error count
            val message = serializer.deserialize(record.key, record.value)
            logger.debug(s"Consumed message [$topic:${record.partition}:${record.offset}] $message")
            message match {
              case m: Change => fireEvent(m); cache.put(m.feature)
              case m: Delete => fireEvent(m); cache.remove(m.id)
              case m: Clear  => fireEvent(m); cache.clear()
              case m => throw new IllegalArgumentException(s"Unknown message: $m")
            }
          }
          logger.trace(s"Consumer finished processing ${result.count()} records from topic $topic")
          // we commit the offsets so that the next poll doesn't return the same records
          consumer.commitSync()
          logger.trace(s"Consumer committed offsets")
        } catch {
          case _: WakeupException | _: InterruptedException => // ignore
          case NonFatal(e) =>
            logger.warn(s"Error receiving message from topic $topic:", e)
            errorCount += 1
            if (errorCount < 300) { Thread.sleep(1000) } else {
              logger.error("Shutting down due to too many errors")
              schedules(i).cancel(false)
            }
        } finally {
          running.decrementAndGet()
        }
      }
    }
  }
}

