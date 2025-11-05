/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import io.micrometer.core.instrument.{Metrics, Tags}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.kafka.data.KafkaDataStore
import org.locationtech.geomesa.kafka.data.KafkaDataStore.{IndexConfig, LayerView}
import org.locationtech.geomesa.kafka.index.FeatureStateFactory.{FeatureExpiration, FeatureState}
import org.locationtech.geomesa.memory.cqengine.GeoCQIndex
import org.locationtech.geomesa.memory.cqengine.utils.CQIndexType
import org.locationtech.geomesa.memory.index.SimpleFeatureSpatialIndex
import org.locationtech.geomesa.memory.index.impl.{BucketIndex, SizeSeparatedBucketIndex}
import org.locationtech.geomesa.metrics.micrometer.utils.GaugeUtils

import java.util.concurrent._

/**
 * Feature cache implementation
 *
 * @param sft simple feature type
 * @param config index config
 * @param layerViews layer views
 */
class KafkaFeatureCacheImpl(sft: SimpleFeatureType, config: IndexConfig, layerViews: Seq[LayerView] = Seq.empty, tags: Tags = Tags.empty())
    extends KafkaFeatureCache with FeatureExpiration {

  import org.locationtech.geomesa.kafka.index.KafkaFeatureCacheImpl.MetricsPrefix
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  // keeps location and expiry keyed by feature ID (we need a way to retrieve a feature based on ID for
  // update/delete operations). to reduce contention, we never iterate over this map
  private val state = GaugeUtils.mapSizeGauge(s"$MetricsPrefix.size", tags, new ConcurrentHashMap[String, FeatureState]())

  private val spatialIndex = createIndex(sft)

  private val factory = FeatureStateFactory(sft, spatialIndex, config.expiry, this, config.executor)

  override val views: Seq[KafkaFeatureCacheView] =
    layerViews.map(view => KafkaFeatureCacheView(view, createIndex(view.viewSft)))

  private val expirations = Metrics.counter(s"$MetricsPrefix.expirations", tags)

  logger.debug(s"Initialized KafkaFeatureCache with factory $factory and support $spatialIndex")

  /**
    * Note: this method is not thread-safe. The `state` and `index` can get out of sync if the same feature
    * is updated simultaneously from two different threads
    *
    * In our usage, this isn't a problem, as a given feature ID is always operated on by a single thread
    * due to kafka consumer partitioning
    */
  override def put(feature: SimpleFeature): Unit = {
    if (feature.getDefaultGeometry == null) {
      logger.warn(s"Null geometry detected for feature ${feature.getID}. Skipping loading into cache.")
      return
    }
    val featureState = factory.createState(feature)
    logger.trace(s"${featureState.id} adding feature $featureState")
    val old = state.put(featureState.id, featureState)
    if (old == null) {
      featureState.insertIntoIndex()
      views.foreach(_.put(feature))
    } else if (old.time <= featureState.time) {
      logger.trace(s"${featureState.id} removing old feature")
      old.removeFromIndex()
      featureState.insertIntoIndex()
      views.foreach { view =>
        view.remove(featureState.id)
        view.put(feature)
      }
    } else {
      logger.trace(s"${featureState.id} ignoring out of sequence feature")
      if (!state.replace(featureState.id, featureState, old)) {
        logger.warn(s"${featureState.id} detected inconsistent state... spatial index may be incorrect")
        old.removeFromIndex()
        views.foreach(_.remove(featureState.id))
      }
    }
    logger.trace(s"Current index size: ${state.size()}/${spatialIndex.size()}")
  }

  /**
    * Note: this method is not thread-safe. The `state` and `index` can get out of sync if the same feature
    * is updated simultaneously from two different threads
    *
    * In our usage, this isn't a problem, as a given feature ID is always operated on by a single thread
    * due to kafka consumer partitioning
    */
  override def remove(id: String): Unit = {
    logger.trace(s"$id removing feature")
    val old = state.remove(id)
    if (old != null) {
      old.removeFromIndex()
      views.foreach(_.remove(id))
    }
    logger.trace(s"Current index size: ${state.size()}/${spatialIndex.size()}")
  }

  override def expire(featureState: FeatureState): Unit = {
    logger.trace(s"${featureState.id} expiring from index")
    if (state.remove(featureState.id, featureState)) {
      featureState.removeFromIndex()
      views.foreach(_.remove(featureState.id))
      expirations.increment()
    }
    logger.trace(s"Current index size: ${state.size()}/${spatialIndex.size()}")
  }

  override def clear(): Unit = {
    logger.trace("Clearing index")
    state.clear()
    spatialIndex.clear()
    views.foreach(_.clear())
  }

  override def size(): Int = state.size()

  // optimized for filter.include
  override def size(f: Filter): Int = if (f == Filter.INCLUDE) { size() } else { query(f).length }

  override def query(id: String): Option[SimpleFeature] =
    Option(state.get(id)).flatMap(f => Option(f.retrieveFromIndex()))

  override def query(filter: Filter): Iterator[SimpleFeature] = spatialIndex.query(filter)

  override def close(): Unit = factory.close()

  private def createIndex(sft: SimpleFeatureType): SimpleFeatureSpatialIndex = {
    if (config.cqAttributes.nonEmpty) {
      val attributes =
        if (config.cqAttributes == Seq(KafkaDataStore.CqIndexFlag)) {
          // deprecated boolean config to enable indices based on the stored simple feature type
          CQIndexType.getDefinedAttributes(sft) ++ Option(sft.getGeomField).map((_, CQIndexType.GEOMETRY))
        } else {
          config.cqAttributes
        }
      // note: CQEngine handles points vs non-points internally
      new GeoCQIndex(sft, attributes, config.resolution.x, config.resolution.y)
    } else if (sft.isPoints) {
      BucketIndex(sft, config.resolution.x, config.resolution.y)
    } else {
      SizeSeparatedBucketIndex(sft, config.ssiTiers, config.resolution.x / 360d, config.resolution.y / 180d)
    }
  }
}

object KafkaFeatureCacheImpl {
  private val MetricsPrefix = s"${KafkaDataStore.MetricsPrefix}.index"
}
