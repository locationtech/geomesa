/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import java.io.Closeable
import java.util.concurrent._

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.filter.index.SpatialIndexSupport
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.concurrent.duration.Duration

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

  def empty(): KafkaFeatureCache = EmptyFeatureCache

  def apply(sft: SimpleFeatureType, expiry: Duration, support: SpatialIndexSupport): KafkaFeatureCache = {
    if (sft.isPoints) {
      new KafkaFeatureCachePoints(sft, support, expiry)
    } else {
      logger.warn("Kafka geometry index only supports points; features will be indexed by their centroids")
      new KafkaFeatureCacheCentroids(sft, support, expiry)
    }
  }

  /**
    * Feature cache for point type geometries
    *
    * @param sft simple feature type
    * @param support spatial index
    * @param expiry expiry
    */
  class KafkaFeatureCachePoints(sft: SimpleFeatureType, support: SpatialIndexSupport, expiry: Duration)
      extends KafkaFeatureCache {

    private val executor = {
      val ex = new ScheduledThreadPoolExecutor(2)
      // don't keep running scheduled tasks after shutdown
      ex.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
      // remove tasks when canceled, otherwise they will only be removed from the task queue
      // when they would be executed. we expect frequent cancellations due to feature updates
      ex.setRemoveOnCancelPolicy(true)
      ex
    }

    private val expiryMillis = if (expiry == Duration.Inf) { -1 } else { expiry.toMillis }

    protected val geometryIndex: Int = sft.indexOf(sft.getGeomField)

    // keeps location and expiry keyed by feature ID (we need a way to retrieve a feature based on ID for
    // update/delete operations). to reduce contention, we never iterate over this map
    protected val state = new ConcurrentHashMap[String, FeatureState]

    /**
      * WARNING: this method is not thread-safe
      *
      * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
      */
    override def put(feature: SimpleFeature): Unit = {
      val pt = feature.getAttribute(geometryIndex).asInstanceOf[Point]
      val featureState = new FeatureState(pt.getX, pt.getY, feature.getID)
      logger.trace(s"${featureState.id} inserting with expiry $expiry")
      val old = state.put(featureState.id, featureState)
      if (old != null) {
        if (old.expire != null) {
          logger.trace(s"${featureState.id} cancelling old expiration")
          old.expire.cancel(true)
        }
        logger.trace(s"${featureState.id} removing old feature")
        support.index.remove(old.x, old.y, old.id)
      }
      support.index.insert(featureState.x, featureState.y, featureState.id, feature)
      logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
    }

    /**
      * WARNING: this method is not thread-safe
      *
      * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
      */
    override def remove(id: String): Unit = {
      logger.trace(s"$id removing feature")
      val old = state.remove(id)
      if (old != null) {
        if (old.expire != null) {
          logger.trace(s"$id cancelling old expiration")
          old.expire.cancel(true)
        }
        support.index.remove(old.x, old.y, old.id)
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
      Option(state.get(id)).flatMap(f => Option(support.index.get(f.x, f.y, id)))

    override def query(filter: Filter): Iterator[SimpleFeature] = support.query(filter)

    override def close(): Unit = executor.shutdown()

    /**
      * Holder for our key feature values
      *
      * @param x x coord
      * @param y y coord
      * @param id feature id
      */
    protected class FeatureState(val x: Double, val y: Double, val id: String) extends Runnable {

      val expire: ScheduledFuture[_] =
        if (expiryMillis == -1) { null } else { executor.schedule(this, expiryMillis, TimeUnit.MILLISECONDS) }

      override def run(): Unit = {
        logger.trace(s"$id expiring from index")
        state.remove(id)
        support.index.remove(x, y, id)
        logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
      }
    }
  }

  /**
    * Feature cache for non-point type geometries
    *
    * @param sft simple feature type
    * @param support spatial index
    * @param expiry expiry
    */
  class KafkaFeatureCacheCentroids(sft: SimpleFeatureType, support: SpatialIndexSupport, expiry: Duration)
      extends KafkaFeatureCachePoints(sft, support, expiry) {

    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry

    /**
      * WARNING: this method is not thread-safe
      *
      * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
      */
    override def put(feature: SimpleFeature): Unit = {
      val pt = feature.getAttribute(geometryIndex).asInstanceOf[Geometry].safeCentroid()
      val featureState = new FeatureState(pt.getX, pt.getY, feature.getID)
      logger.trace(s"${featureState.id} inserting with expiry $expiry")
      val old = state.put(featureState.id, featureState)
      if (old != null) {
        if (old.expire != null) {
          logger.trace(s"${featureState.id} cancelling old expiration")
          old.expire.cancel(false)
        }
        logger.trace(s"${featureState.id} removing old feature")
        support.index.remove(old.x, old.y, old.id)
      }
      support.index.insert(featureState.x, featureState.y, featureState.id, feature)
      logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
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
