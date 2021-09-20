/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.{FeatureListener, SimpleFeatureSource}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.api.filter.expression.Expression
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.index.SpatialIndexSupport
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.data.KafkaDataStore._
import org.locationtech.geomesa.memory.cqengine.GeoCQIndexSupport
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType
import org.locationtech.geomesa.metrics.core.GeoMesaMetrics

import java.io.Closeable
import java.util.concurrent._

trait KafkaFeatureCache extends KafkaListeners with Closeable {
  def put(feature: SimpleFeature): Unit
  def remove(id: String): Unit
  def clear(): Unit
  def size(): Int
  def size(filter: Filter): Int
  def query(id: String): Option[SimpleFeature]
  def query(filter: Filter): Iterator[SimpleFeature]
  def views: Seq[KafkaFeatureCacheView]
}

object KafkaFeatureCache extends LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  /**
   * Create a standard feature cache
   *
   * @param sft simple feature type
   * @param config cache config
   * @param views layer view config
   * @param metrics optional metrics hook
   * @return
   */
  def apply(
    sft: SimpleFeatureType,
    config: IndexConfig,
    views: Seq[LayerView] = Seq.empty,
    metrics: Option[GeoMesaMetrics] = None): KafkaFeatureCache = {
    if (config.expiry == ImmediatelyExpireConfig) {
      new NoOpFeatureCache(views.map(v => KafkaFeatureCacheView.empty(v.viewSft)))
    } else {
      metrics match {
        case None => new KafkaFeatureCacheImpl(sft, config, views)
        case Some(m) => new KafkaFeatureCacheWithMetrics(sft, config, views, m)
      }
    }
  }

  /**
    * No-op cache
    *
    * @return
    */
  def empty(views: Seq[LayerView] = Seq.empty): KafkaFeatureCache =
    new EmptyFeatureCache(views.map(v => KafkaFeatureCacheView.empty(v.viewSft)))

  /**
    * Cache that won't spatially index the features
    *
    * @param sft simple feature type
    * @param ordering feature ordering
    * @return
    */
  def nonIndexing(sft: SimpleFeatureType, ordering: ExpiryTimeConfig = NeverExpireConfig): KafkaFeatureCache = {
    val event: PartialFunction[ExpiryTimeConfig, String] = {
      case EventTimeConfig(_, exp, true) => exp
    }

    val ord = ordering match {
      case o if event.isDefinedAt(o) => Some(event.apply(o))
      // all filters use the same event time ordering
      case FilteredExpiryConfig(filters) if event.isDefinedAt(filters.head._2) => Some(event.apply(filters.head._2))
      case _ => None
    }

    ord.map(FastFilterFactory.toExpression(sft, _)) match {
      case None => new NonIndexingFeatureCache()
      case Some(exp) => new NonIndexingEventTimeFeatureCache(exp)
    }
  }

  /**
    * Create a CQEngine index support. Note that CQEngine handles points vs non-points internally
    *
    * @param sft simple feature type
    * @param config index config
    * @return
    */
  private [index] def cqIndexSupport(sft: SimpleFeatureType, config: IndexConfig): SpatialIndexSupport = {
    val attributes = if (config.cqAttributes == Seq(KafkaDataStore.CqIndexFlag)) {
      // deprecated boolean config to enable indices based on the stored simple feature type
      CQIndexType.getDefinedAttributes(sft) ++ Option(sft.getGeomField).map((_, CQIndexType.GEOMETRY))
    } else {
      config.cqAttributes
    }
    GeoCQIndexSupport(sft, attributes, config.resolution.x, config.resolution.y)
  }

  /**
    * Non-indexing feature cache that just tracks the most recent feature
    */
  class NonIndexingFeatureCache extends KafkaFeatureCache {

    private val state = new ConcurrentHashMap[String, SimpleFeature]

    override def put(feature: SimpleFeature): Unit = state.put(feature.getID, feature)

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

    override def views: Seq[KafkaFeatureCacheView] = Seq.empty
  }

  /**
    * Non-indexing feature cache that just tracks the most recent feature, based on event time
    *
    * @param time event time expression
    */
  class NonIndexingEventTimeFeatureCache(time: Expression) extends KafkaFeatureCache {

    private val state = new ConcurrentHashMap[String, (SimpleFeature, Long)]

    /**
      * Note: this method is not thread-safe. The `state` and can fail to replace the correct values
      * if the same feature is updated simultaneously from two different threads
      *
      * In our usage, this isn't a problem, as a given feature ID is always operated on by a single thread
      * due to kafka consumer partitioning
      */
    override def put(feature: SimpleFeature): Unit = {
      val tuple = (feature, FeatureStateFactory.time(time, feature))
      val old = state.put(feature.getID, tuple)
      if (old != null && old._2 > tuple._2) {
        state.replace(feature.getID, tuple, old)
      }
    }

    override def remove(id: String): Unit = state.remove(id)

    override def clear(): Unit = state.clear()

    override def close(): Unit = {}

    override def size(): Int = state.size()

    override def size(filter: Filter): Int = query(filter).length

    override def query(id: String): Option[SimpleFeature] = Option(state.get(id)).map(_._1)

    override def query(filter: Filter): Iterator[SimpleFeature] = {
      import scala.collection.JavaConverters._
      val features = state.asScala.valuesIterator.map(_._1)
      if (filter == Filter.INCLUDE) { features } else {
        features.filter(filter.evaluate)
      }
    }

    override def views: Seq[KafkaFeatureCacheView] = Seq.empty
  }

  class EmptyFeatureCache(val views: Seq[KafkaFeatureCacheView]) extends KafkaFeatureCache {
    override def put(feature: SimpleFeature): Unit = throw new NotImplementedError("Empty feature cache")
    override def remove(id: String): Unit = throw new NotImplementedError("Empty feature cache")
    override def clear(): Unit = throw new NotImplementedError("Empty feature cache")
    override def size(): Int = 0
    override def size(filter: Filter): Int = 0
    override def query(id: String): Option[SimpleFeature] = None
    override def query(filter: Filter): Iterator[SimpleFeature] = Iterator.empty
    override def close(): Unit = {}
    override def addListener(source: SimpleFeatureSource, listener: FeatureListener): Unit = {}
    override def removeListener(source: SimpleFeatureSource, listener: FeatureListener): Unit = {}
  }

  class NoOpFeatureCache(val views: Seq[KafkaFeatureCacheView]) extends KafkaFeatureCache {
    override def put(feature: SimpleFeature): Unit = {}
    override def remove(id: String): Unit = {}
    override def clear(): Unit = {}
    override def size(): Int = 0
    override def size(filter: Filter): Int = 0
    override def query(id: String): Option[SimpleFeature] = None
    override def query(filter: Filter): Iterator[SimpleFeature] = Iterator.empty
    override def close(): Unit = {}
    override def addListener(source: SimpleFeatureSource, listener: FeatureListener): Unit = {}
    override def removeListener(source: SimpleFeatureSource, listener: FeatureListener): Unit = {}
  }
}
