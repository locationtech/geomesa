/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import java.io.Closeable
import java.util.Date
import java.util.concurrent._

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.index.{BucketIndexSupport, SizeSeparatedBucketIndexSupport, SpatialIndexSupport}
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.index.BasicFeatureCache.{ExtentOps, PointOps}
import org.locationtech.geomesa.memory.cqengine.GeoCQIndexSupport
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.Expression

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

trait KafkaFeatureCache extends Closeable {
  def put(feature: SimpleFeature): Unit
  def remove(id: String): Unit
  def clear(): Unit
  def size(): Int
  def size(filter: Filter): Int
  def query(id: String): Option[SimpleFeature]
  def query(filter: Filter): Iterator[SimpleFeature]

  protected def time: Option[Expression] = None
}

object KafkaFeatureCache extends LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def apply(sft: SimpleFeatureType,
            config: KafkaDataStore.IndexConfig): KafkaFeatureCache = {
    val eventTimeExpression = config.eventTime.map(e => (FastFilterFactory.toExpression(sft, e.expression), e.ordering))
    val expiryMillis = if (config.expiry == Duration.Inf) { -1 } else { config.expiry.toMillis }

    if (sft.nonPoints) {
      // note: CQEngine handles points vs non-points internally
      val support = if (config.cqAttributes.nonEmpty) { cqIndexSupport(sft, config) } else {
        SizeSeparatedBucketIndexSupport(sft, config.resolutionX / 360d, config.resolutionY / 180d)
      }
      eventTimeExpression match {
        case None         => new BasicFeatureCache(support, sft.getGeomIndex, expiryMillis) with ExtentOps
        case Some((e, o)) => new EventTimeFeatureCache(support, sft.getGeomIndex, expiryMillis, e, o) with ExtentOps
      }
    } else {
      val support = if (config.cqAttributes.nonEmpty) { cqIndexSupport(sft, config) } else {
        BucketIndexSupport(sft, config.resolutionX, config.resolutionY)
      }
      eventTimeExpression match {
        case None         => new BasicFeatureCache(support, sft.getGeomIndex, expiryMillis) with PointOps
        case Some((e, o)) => new EventTimeFeatureCache(support, sft.getGeomIndex, expiryMillis, e, o) with PointOps
      }
    }
  }

  def empty(): KafkaFeatureCache = EmptyFeatureCache

  def nonIndexing(cache: KafkaFeatureCache): KafkaFeatureCache = new NonIndexingFeatureCache(cache.time)

  /**
    * Create a CQEngine index support. Note that CQEngine handles points vs non-points internally
    *
    * @param sft simple feature type
    * @param config index config
    * @return
    */
  private def cqIndexSupport(sft: SimpleFeatureType, config: KafkaDataStore.IndexConfig): SpatialIndexSupport = {
    val attributes = if (config.cqAttributes == Seq(KafkaDataStore.CqIndexFlag)) {
      // deprecated boolean config to enable indices based on the stored simple feature type
      CQIndexType.getDefinedAttributes(sft) ++ Option(sft.getGeomField).map((_, CQIndexType.GEOMETRY))
    } else {
      config.cqAttributes
    }
    GeoCQIndexSupport(sft, attributes, config.resolutionX, config.resolutionY)
  }

  /**
    * Non-indexing feature cache that just tracks the most recent feature
    *
    * @param time event time expression
    */
  class NonIndexingFeatureCache(time: Option[Expression]) extends KafkaFeatureCache {

    protected val state = new ConcurrentHashMap[String, SimpleFeature]

    /**
      * WARNING: this method is not thread-safe
      *
      * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
      */
    override def put(feature: SimpleFeature): Unit = {
      val old = state.put(feature.getID, feature)
      time.foreach { t =>
        try {
          if (old != null && t.evaluate(old).asInstanceOf[Date].after(t.evaluate(feature).asInstanceOf[Date])) {
            state.replace(feature.getID, feature, old)
          }
        } catch {
          case NonFatal(e) => logger.error(s"Error evaluating event time for $feature", e)
        }
      }
    }

    override def remove(id: String): Unit = state.remove(id)

    override def clear(): Unit = state.clear()

    override def close(): Unit = {}

    override def size(): Int = state.size()

    override def size(filter: Filter): Int = query(filter).length

    override def query(id: String): Option[SimpleFeature] = Option(state.get(id))

    override def query(filter: Filter): Iterator[SimpleFeature] = {
      import scala.collection.JavaConverters._
      val features = state.asScala.valuesIterator
      if (filter == Filter.INCLUDE) { features } else {
        features.filter(filter.evaluate)
      }
    }
  }

  object EmptyFeatureCache extends KafkaFeatureCache {
    override def put(feature: SimpleFeature): Unit = throw new NotImplementedError("Empty feature cache")
    override def remove(id: String): Unit = throw new NotImplementedError("Empty feature cache")
    override def clear(): Unit = throw new NotImplementedError("Empty feature cache")
    override def size(): Int = 0
    override def size(filter: Filter): Int = 0
    override def query(id: String): Option[SimpleFeature] = None
    override def query(filter: Filter): Iterator[SimpleFeature] = Iterator.empty
    override def close(): Unit = {}
  }
}
