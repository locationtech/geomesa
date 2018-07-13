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

import com.typesafe.scalalogging.{LazyLogging, StrictLogging}
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.index.SpatialIndexSupport
import org.locationtech.geomesa.kafka.data.KafkaDataStore.EventTimeConfig
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
            support: SpatialIndexSupport,
            expiry: Duration,
            eventTime: Option[EventTimeConfig]): KafkaFeatureCache = {
    val eventTimeExpression = eventTime.map(e => (FastFilterFactory.toExpression(sft, e.expression), e.ordering))
    val expiryMillis = if (expiry == Duration.Inf) { -1 } else { expiry.toMillis }
    if (sft.isPoints) {
      eventTimeExpression match {
        case None         => new BasicFeatureCache(sft.getGeomIndex, expiryMillis, support)
        case Some((e, o)) => new EventTimeFeatureCache(sft.getGeomIndex, e, o, expiryMillis, support)
      }
    } else {
      logger.warn("Kafka geometry index only supports points; features will be indexed by their centroids")
      eventTimeExpression match {
        case None         => new BasicFeatureCache(sft.getGeomIndex, expiryMillis, support) with CentroidsCache
        case Some((e, o)) => new EventTimeFeatureCache(sft.getGeomIndex, e, o, expiryMillis, support) with CentroidsCache
      }
    }
  }

  def empty(): KafkaFeatureCache = EmptyFeatureCache

  def nonIndexing(cache: KafkaFeatureCache): KafkaFeatureCache = new NonIndexingFeatureCache(cache.time)

  /**
    * Feature cache with support for expiry
    *
    * @param geom geometry attribute index
    * @param expiry expiry, or -1 if no expiry
    * @param support spatial index support
    */
  class BasicFeatureCache(protected val geom: Int, expiry: Long, support: SpatialIndexSupport)
      extends KafkaFeatureCache with StrictLogging {

    // keeps location and expiry keyed by feature ID (we need a way to retrieve a feature based on ID for
    // update/delete operations). to reduce contention, we never iterate over this map
    protected val state = new ConcurrentHashMap[String, FeatureState]

    protected val executor: ScheduledExecutorService = if (expiry < 1) { null } else {
      val ex = new ScheduledThreadPoolExecutor(2)
      // don't keep running scheduled tasks after shutdown
      ex.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
      // remove tasks when canceled, otherwise they will only be removed from the task queue
      // when they would be executed. we expect frequent cancellations due to feature updates
      ex.setRemoveOnCancelPolicy(true)
      ex
    }

    protected def geometry(feature: SimpleFeature): Point = feature.getAttribute(geom).asInstanceOf[Point]

    /**
      * WARNING: this method is not thread-safe
      *
      * TODO: https://geomesa.atlassian.net/browse/GEOMESA-1409
      */
    override def put(feature: SimpleFeature): Unit = {
      val pt = geometry(feature)
      val featureState = new FeatureState(pt.getX, pt.getY, feature.getID, 0L)
      logger.trace(s"${featureState.id} adding feature with expiry $expiry")
      if (expiry > 0) {
        featureState.expire = executor.schedule(featureState, expiry, TimeUnit.MILLISECONDS)
        val old = state.put(featureState.id, featureState)
        if (old != null) {
          old.expire.cancel(false)
          logger.trace(s"${featureState.id} removing old feature")
          support.index.remove(old.x, old.y, old.id)
        }
      } else {
        val old = state.put(featureState.id, featureState)
        if (old != null) {
          logger.trace(s"${featureState.id} removing old feature")
          support.index.remove(old.x, old.y, old.id)
        }
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
          old.expire.cancel(false)
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

    override def close(): Unit = if (executor != null) { executor.shutdown() }

    /**
      * Holder for our key feature values
      *
      * @param x x coord
      * @param y y coord
      * @param id feature id
      * @param time feature (or message) time
      */
    protected class FeatureState(val x: Double, val y: Double, val id: String, val time: Long) extends Runnable {

      var expire: ScheduledFuture[_] = _

      override def run(): Unit = {
        logger.trace(s"$id expiring from index")
        if (state.remove(id, this)) {
          support.index.remove(x, y, id)
        }
        logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
      }
    }
  }

  trait CentroidsCache {

    this: BasicFeatureCache =>

    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry

    override protected def geometry(feature: SimpleFeature): Point =
      feature.getAttribute(geom).asInstanceOf[Geometry].safeCentroid()
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
