/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.kafka

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ScheduledThreadPoolExecutor, Executors, LinkedBlockingQueue, TimeUnit}

import com.google.common.base.Ticker
import com.google.common.cache._
import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.data.Query
import org.geotools.data.store.ContentEntry
import org.locationtech.geomesa.kafka.consumer.KafkaConsumerFactory
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.FR
import org.locationtech.geomesa.utils.index.{BucketIndex, SpatialIndex}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.collection.JavaConverters._
import scala.collection.mutable

class LiveKafkaConsumerFeatureSource(entry: ContentEntry,
                                     sft: SimpleFeatureType,
                                     topic: String,
                                     kf: KafkaConsumerFactory,
                                     expirationPeriod: Option[Long] = None,
                                     cleanUpCache: Boolean,
                                     query: Query = null)
                                    (implicit ticker: Ticker = Ticker.systemTicker())
  extends KafkaConsumerFeatureSource(entry, sft, query) with Runnable with Logging {

  private[kafka] val featureCache = new LiveFeatureCache(sft, expirationPeriod)

  private val msgDecoder = new KafkaGeoMessageDecoder(sft)
  private val queue = new LinkedBlockingQueue[GeoMessage]()
  private val stream = kf.messageStreams(topic, 1).head

  private val running = new AtomicBoolean(true)

  val es = Executors.newFixedThreadPool(2)
  val ses = new ScheduledThreadPoolExecutor(1)

  sys.addShutdownHook {
    running.set(false)
    es.shutdownNow()
    ses.shutdownNow()
  }

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
    }, 0, 1, TimeUnit.SECONDS)
  }

  override def run(): Unit = while (running.get) {
    queue.take() match {
      case update: CreateOrUpdate => featureCache.createOrUpdateFeature(update)
      case del: Delete            => featureCache.removeFeature(del)
      case clr: Clear             => featureCache.clear()
      case m                      => throw new IllegalArgumentException(s"Unknown message: $m")
    }
  }

  // optimized for filter.include
  override def getCountInternal(query: Query): Int = featureCache.size(query.getFilter)

  override def getReaderForFilter(f: Filter): FR = featureCache.getReaderForFilter(f)
}

/** @param sft the [[SimpleFeatureType]]
  * @param expirationPeriod the number of milliseconds after write to expire a feature or ``None`` to not
  *                         expire
  * @param ticker used to determine elapsed time for expiring entries
  */
class LiveFeatureCache(override val sft: SimpleFeatureType,
                       expirationPeriod: Option[Long])(implicit ticker: Ticker)
  extends KafkaConsumerFeatureCache with Logging {

  def cleanUp(): Unit = {
    cache.cleanUp()
  }

  var spatialIndex: SpatialIndex[SimpleFeature] = newSpatialIndex()

  val cache: Cache[String, FeatureHolder] = {
    val cb = CacheBuilder.newBuilder().ticker(ticker)
    expirationPeriod.foreach { ep =>
      cb.expireAfterWrite(ep, TimeUnit.MILLISECONDS)
        .removalListener(new RemovalListener[String, FeatureHolder] {
          def onRemoval(removal: RemovalNotification[String, FeatureHolder]) = {
            if (removal.getCause == RemovalCause.EXPIRED) {
              logger.debug(s"Removing feature ${removal.getKey} due to expiration after ${ep}ms")
              spatialIndex.remove(removal.getValue.env, removal.getValue.sf)
            }
          }
        })
    }
    cb.build()
  }

  override val features: mutable.Map[String, FeatureHolder] = cache.asMap().asScala

  def createOrUpdateFeature(update: CreateOrUpdate): Unit = {
    val sf = update.feature
    val id = sf.getID
    val old = cache.getIfPresent(id)
    if (old != null) {
      spatialIndex.remove(old.env, old.sf)
    }
    val env = sf.geometry.getEnvelopeInternal
    spatialIndex.insert(env, sf)
    cache.put(id, FeatureHolder(sf, env))
  }

  def removeFeature(toDelete: Delete): Unit = {
    val id = toDelete.id
    val old = cache.getIfPresent(id)
    if (old != null) {
      spatialIndex.remove(old.env, old.sf)
      cache.invalidate(id)
    }
  }

  def clear(): Unit = {
    cache.invalidateAll()
    spatialIndex = newSpatialIndex()
  }

  private def newSpatialIndex() = new BucketIndex[SimpleFeature]
}
