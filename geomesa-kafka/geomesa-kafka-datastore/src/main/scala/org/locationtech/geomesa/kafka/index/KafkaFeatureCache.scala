/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import java.io.Closeable
import java.util.concurrent._

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.index.SpatialIndexSupport
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.data.KafkaDataStore._
import org.locationtech.geomesa.memory.cqengine.GeoCQIndexSupport
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.Expression

trait KafkaFeatureCache extends Closeable {
  def put(feature: SimpleFeature): Unit
  def remove(id: String): Unit
  def clear(): Unit
  def size(): Int
  def size(filter: Filter): Int
  def query(id: String): Option[SimpleFeature]
  def query(filter: Filter): Iterator[SimpleFeature]
}

object KafkaFeatureCache extends LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  /**
    * Create a standard feature cache
    *
    * @param sft simple feature type
    * @param config cache config
    * @return
    */
  def apply(sft: SimpleFeatureType, config: IndexConfig): KafkaFeatureCache = {
    if (config.expiry == ImmediatelyExpireConfig) { NoOpFeatureCache } else {
      new KafkaFeatureCacheImpl(sft, config)
    }
  }

  /**
    * No-op cache
    *
    * @return
    */
  def empty(): KafkaFeatureCache = EmptyFeatureCache

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

  object NoOpFeatureCache extends KafkaFeatureCache {
    override def put(feature: SimpleFeature): Unit = {}
    override def remove(id: String): Unit = {}
    override def clear(): Unit = {}
    override def size(): Int = 0
    override def size(filter: Filter): Int = 0
    override def query(id: String): Option[SimpleFeature] = None
    override def query(filter: Filter): Iterator[SimpleFeature] = Iterator.empty
    override def close(): Unit = {}
  }
}
