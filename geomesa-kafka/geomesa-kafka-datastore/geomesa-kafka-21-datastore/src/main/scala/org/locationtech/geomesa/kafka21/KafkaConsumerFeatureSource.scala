/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka21

import java.io.Closeable
import java.util.concurrent._

import com.github.benmanes.caffeine.cache.Ticker
import com.google.common.util.concurrent.MoreExecutors
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.geotools.data.store.{ContentEntry, ContentFeatureSource}
import org.geotools.data.{FeatureEvent, Query}
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.filter.visitor.BindingFilterVisitor
import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.geomesa.filter.visitor.QueryPlanFilterVisitor
import org.locationtech.geomesa.kafka._
import org.locationtech.geomesa.kafka21.KafkaConsumerFeatureSource.FeatureSourceConsumer
import org.locationtech.geomesa.security.ContentFeatureSourceSecuritySupport
import org.locationtech.geomesa.utils.geotools._
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.util.control.NonFatal

class KafkaConsumerFeatureSource(
    entry: ContentEntry,
    private val sft: SimpleFeatureType,
    private val topic: String,
    private val consumers: Seq[Consumer[Array[Byte], Array[Byte]]],
    expirationPeriod: Option[Long] = None,
    consistencyCheck: Option[Long] = None,
    cleanUpCache: Boolean,
    useCQCache: Boolean,
    cleanUpCachePeriod: Long = 10000L,
    monitor: Boolean)
    (implicit ticker: Ticker = Ticker.systemTicker()
  ) extends ContentFeatureSource(entry, null)
    with ContentFeatureSourceSecuritySupport
    with ContentFeatureSourceReTypingSupport
    with ContentFeatureSourceInfo
    with Closeable
    with LazyLogging {

  private lazy val contentState = entry.getState(getTransaction)

  private [kafka21] val featureCache: LiveFeatureCache = if (useCQCache) {
    new LiveFeatureCacheCQEngine(sft, expirationPeriod)
  } else {
    new LiveFeatureCacheGuava(sft, expirationPeriod, consistencyCheck)
  }

  private val es: Option[ScheduledExecutorService] =
    if (expirationPeriod.isEmpty || !cleanUpCache) { None } else {
      Some(MoreExecutors.getExitingScheduledExecutorService(new ScheduledThreadPoolExecutor(1)))
    }

  private val consumer = new FeatureSourceConsumer(this)

  consumer.startConsumers()

  es.foreach { service =>
    val runnable = new Runnable() { override def run(): Unit = featureCache.cleanUp() }
    service.scheduleAtFixedRate(runnable, 0, cleanUpCachePeriod, TimeUnit.MILLISECONDS)
  }

  override protected def getBoundsInternal(query: Query): ReferencedEnvelope =
    org.locationtech.geomesa.utils.geotools.wholeWorldEnvelope

  override protected def buildFeatureType(): SimpleFeatureType = {
    val builder = new SimpleFeatureTypeBuilder()
    builder.init(sft)
    builder.setNamespaceURI(getDataStore.getNamespaceURI)
    sft.getUserData.foreach { case (k, v) => builder.userData(k, v) }
    builder.buildFeatureType()
  }

  override protected def getCountInternal(query: Query): Int = {
    Option(query.getFilter).filter(_ != Filter.INCLUDE) match {
      case None => featureCache.size()
      case Some(f) => featureCache.size(f)
    }
  }

  override protected def getReaderInternal(query: Query): FR = {
    val filter = Option(query).map(_.getFilter).getOrElse(Filter.INCLUDE)
    val optimized = Seq(new BindingFilterVisitor(sft), new QueryPlanFilterVisitor(sft)).foldLeft(filter) {
      case (f, visitor) => f.accept(visitor, null).asInstanceOf[Filter]
    }
    val reader = addSupport(query, featureCache.getReaderForFilter(optimized))
    if (monitor) { new MonitoringFeatureReader("Kafka", query, reader) } else { reader }
  }

  override def close(): Unit = {
    consumer.close()
    es.foreach(_.shutdownNow())
  }

  // lazily fires events.
  private def fireEvent(event: => FeatureEvent): Unit = {
    if (contentState.hasListener) {
      try {
        contentState.fireFeatureEvent(event)
      } catch {
        case NonFatal(e) => logger.error("Error in feature listeners:", e)
      }
    }
  }

  override protected val canFilter: Boolean = true
  override protected val canEvent: Boolean = true
}

object KafkaConsumerFeatureSource {

  private class FeatureSourceConsumer(kfs: KafkaConsumerFeatureSource) extends {
    // use early initialization to prevent NPEs in ThreadedConsumer instantiation
    override protected val consumers: Seq[Consumer[Array[Byte], Array[Byte]]] = kfs.consumers
    override protected val topic: String = kfs.topic
    override protected val frequency: Long = 1000L
  } with ThreadedConsumer {

    private val decoder = new GeoMessageDecoder(kfs.sft)

    override protected def consume(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
      try {
        val msg = decoder.decode(record.key(), record.value())
        logger.debug(s"Consumed message $msg")
        msg match {
          case update: CreateOrUpdate =>
            kfs.fireEvent(KafkaFeatureEvent.changed(kfs, update.feature))
            kfs.featureCache.createOrUpdateFeature(update)

          case del: Delete =>
            kfs.fireEvent(KafkaFeatureEvent.removed(kfs, kfs.featureCache.getFeatureById(del.id).sf))
            kfs.featureCache.removeFeature(del)

          case _: Clear =>
            kfs.fireEvent(KafkaFeatureEvent.cleared(kfs))
            kfs.featureCache.clear()

          case m => throw new IllegalArgumentException(s"Unknown message: $m")
        }
      } catch {
        case NonFatal(e) => logger.warn(s"Error consuming message: ${e.getMessage}", e)
      }
    }
  }
}
