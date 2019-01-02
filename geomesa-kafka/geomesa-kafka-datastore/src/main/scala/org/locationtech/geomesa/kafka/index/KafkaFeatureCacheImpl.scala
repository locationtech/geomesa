/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import java.util.concurrent._

import com.typesafe.scalalogging.StrictLogging
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.index.{BucketIndexSupport, SizeSeparatedBucketIndexSupport}
import org.locationtech.geomesa.kafka.data.KafkaDataStore.IndexConfig
import org.locationtech.geomesa.kafka.index.FeatureStateFactory.{FeatureExpiration, FeatureState}
import org.locationtech.geomesa.utils.cache.Ticker
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.concurrent.duration.Duration

/**
  * Feature cache implementation
  *
  * @param sft simple feature type
  * @param config index config
  */
class KafkaFeatureCacheImpl(sft: SimpleFeatureType, config: IndexConfig)
    extends KafkaFeatureCache with FeatureExpiration with StrictLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  // keeps location and expiry keyed by feature ID (we need a way to retrieve a feature based on ID for
  // update/delete operations). to reduce contention, we never iterate over this map
  private val state = new ConcurrentHashMap[String, FeatureState]

  // note: CQEngine handles points vs non-points internally
  private val support = if (config.cqAttributes.nonEmpty) {
    KafkaFeatureCache.cqIndexSupport(sft, config)
  } else if (sft.isPoints) {
    BucketIndexSupport(sft, config.resolutionX, config.resolutionY)
  } else {
    SizeSeparatedBucketIndexSupport(sft, config.ssiTiers, config.resolutionX / 360d, config.resolutionY / 180d)
  }

  private val factory = {
    val expiry = if (config.expiry == Duration.Inf) { None } else {
      val (executor, ticker) = config.executor.getOrElse {
        val ex = new ScheduledThreadPoolExecutor(2)
        // don't keep running scheduled tasks after shutdown
        ex.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
        // remove tasks when canceled, otherwise they will only be removed from the task queue
        // when they would be executed. we expect frequent cancellations due to feature updates
        ex.setRemoveOnCancelPolicy(true)
        (ex, Ticker.SystemTicker)
      }
      Some((this, executor, ticker, config.expiry.toMillis))
    }
    val eventTime = config.eventTime.map(e => (FastFilterFactory.toExpression(sft, e.expression), e.ordering))
    FeatureStateFactory(support.index, expiry, eventTime, sft.getGeomIndex)
  }

  /**
    * Note: this method is not thread-safe. The `state` and `index` can get out of sync if the same feature
    * is updated simultaneously from two different threads
    *
    * In our usage, this isn't a problem, as a given feature ID is always operated on by a single thread
    * due to kafka consumer partitioning
    */
  override def put(feature: SimpleFeature): Unit = {
    val featureState = factory.createState(feature)
    logger.trace(s"${featureState.id} adding feature $featureState")
    val old = state.put(featureState.id, featureState)
    if (old == null) {
      featureState.insertIntoIndex()
    } else if (old.time <= featureState.time) {
      logger.trace(s"${featureState.id} removing old feature")
      old.removeFromIndex()
      featureState.insertIntoIndex()
    } else {
      logger.trace(s"${featureState.id} ignoring out of sequence feature")
      if (!state.replace(featureState.id, featureState, old)) {
        logger.warn(s"${featureState.id} detected inconsistent state... spatial index may be incorrect")
        old.removeFromIndex()
      }
    }
    logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
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
    }
    logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
  }

  override def expire(featureState: FeatureState): Unit = {
    logger.trace(s"${featureState.id} expiring from index")
    if (state.remove(featureState.id, featureState)) {
      featureState.removeFromIndex()
    }
    logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
  }

  override def clear(): Unit = {
    logger.trace("Clearing index")
    state.clear()
    support.index.clear()
  }

  override def size(): Int = state.size()

  // optimized for filter.include
  override def size(f: Filter): Int = if (f == Filter.INCLUDE) { size() } else { query(f).length }

  override def query(id: String): Option[SimpleFeature] =
    Option(state.get(id)).flatMap(f => Option(f.retrieveFromIndex()))

  override def query(filter: Filter): Iterator[SimpleFeature] = support.query(filter)

  override def close(): Unit = factory.close()
}
