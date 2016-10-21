/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka10

import java.io.Closeable
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{Executors, LinkedBlockingQueue, ScheduledThreadPoolExecutor, TimeUnit}

import com.github.benmanes.caffeine.cache.Ticker
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.store.ContentEntry
import org.geotools.data.{FeatureEvent, Query}
import org.locationtech.geomesa.kafka._
import org.locationtech.geomesa.kafka10.consumer.KafkaConsumerFactory
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.mutable

class LiveKafkaConsumerFeatureSource(e: ContentEntry,
                                     sft: SimpleFeatureType,
                                     topic: String,
                                     kf: KafkaConsumerFactory,
                                     expirationPeriod: Option[Long] = None,
                                     cleanUpCache: Boolean,
                                     useCQCache: Boolean,
                                     q: Query,
                                     monitor: Boolean)
                                    (implicit ticker: Ticker = Ticker.systemTicker())
  extends KafkaConsumerFeatureSource(e, sft, q, monitor) with Runnable with Closeable with LazyLogging {

  private[kafka10] val featureCache: LiveFeatureCache = if (useCQCache)
    new LiveFeatureCacheCQEngine(sft, expirationPeriod)
  else
    new LiveFeatureCacheGuava(sft, expirationPeriod)

  private lazy val contentState = entry.getState(getTransaction)

  private val msgDecoder = new KafkaGeoMessageDecoder(sft)
  private val queue = new LinkedBlockingQueue[GeoMessage]()
  private val (client, streams) = kf.messageStreams(topic, 1)
  private val stream = streams.head // we only have 1 since we used 1 thread

  private val running = new AtomicBoolean(true)

  val es = Executors.newFixedThreadPool(2)
  val ses = new ScheduledThreadPoolExecutor(1)

  sys.addShutdownHook(close())

  es.submit(this)
  es.submit(new Runnable() {
    override def run(): Unit = {
      var count = 0
      // keep track of last offset we've read to avoid re-processing messages
      // each index in the array corresponds to a partition - since we don't know the number of partitions
      // up front, we start with 3 and expand as needed below
      var lastOffsets = mutable.ArrayBuffer.fill(3)(-1L)
      while (running.get) {
        try {
          val iter = stream.iterator()
          while (running.get && iter.hasNext()) {
            val msg = iter.next()
            val isValid = try {
              msg.offset > lastOffsets(msg.partition)
            } catch {
              case e: IndexOutOfBoundsException =>
                // resize the offsets array to accommodate the partitions
                // since this should happen very infrequently, it should be cheaper than checking the size
                // each time through the loop
                val copy = mutable.ArrayBuffer.fill(msg.partition + 1)(-1L)
                lastOffsets.indices.foreach(i => copy(i) = lastOffsets(i))
                lastOffsets = copy
                true
            }
            if (isValid) {
              lastOffsets(msg.partition) = msg.offset // keep track of last read offset
              count = 0 // reset error count
              val geoMessage = msgDecoder.decode(msg)
              logger.debug(s"Consumed message $geoMessage")
              if (!queue.offer(geoMessage)) {
                logger.warn(s"Dropped message $geoMessage due to queue capacity")
              }
            } else {
              logger.debug(s"Ignoring replayed message from kafka with offset ${msg.offset}")
            }
          }
        } catch {
          case t: InterruptedException =>
            running.set(false)

          case t: Throwable =>
            logger.error("Caught exception while running consumer", t)
            count += 1
            if (count == 300) {
              count = 0
              running.set(false)
            } else {
              Thread.sleep(1000)
            }
        }
      }
    }
  })

  if (expirationPeriod.isDefined && cleanUpCache) {
    ses.scheduleAtFixedRate(new Runnable() {
      override def run(): Unit = featureCache.cleanUp()
    }, 0, 10, TimeUnit.SECONDS)
  }

  override def run(): Unit =
    while (running.get) {
      queue.take() match {
        case update: CreateOrUpdate =>
          if (query.getFilter.evaluate(update.feature)) {
            fireEvent(KafkaFeatureEvent.changed(this, update.feature))
            featureCache.createOrUpdateFeature(update)
          }
        case del: Delete =>
          fireEvent(KafkaFeatureEvent.removed(this, featureCache.getFeatureById(del.id).sf))
          featureCache.removeFeature(del)
        case clr: Clear =>
          fireEvent(KafkaFeatureEvent.cleared(this))
          featureCache.clear()
        case m => throw new IllegalArgumentException(s"Unknown message: $m")
      }
    }

  // optimized for filter.include
  override def getCountInternal(query: Query): Int = featureCache.size(query.getFilter)

  override def getReaderForFilter(f: Filter): FR = featureCache.getReaderForFilter(f)

  override def canEvent: Boolean = true

  // Lazily fires events.
  def fireEvent(event: => FeatureEvent) = {
    if (contentState.hasListener) {
      contentState.fireFeatureEvent(event)
    }
  }

  override def close(): Unit = {
    running.set(false)
    client.shutdown()
    es.shutdownNow()
    ses.shutdownNow()
  }
}