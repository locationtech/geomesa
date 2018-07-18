/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import java.util.concurrent._

import com.typesafe.scalalogging.StrictLogging
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.filter.index.SpatialIndexSupport
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

/**
  * Feature cache with support for expiry
  *
  * @param support spatial index support
  * @param geom geometry attribute index
  * @param expiry expiry, or -1 if no expiry
  */
abstract class BasicFeatureCache(protected val support: SpatialIndexSupport, protected val geom: Int, expiry: Long)
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

  protected def createState(feature: SimpleFeature, time: Long): FeatureState

  /**
    * Note: this method is not thread-safe. The `state` and `index` can get out of sync if the same feature
    * is updated simultaneously from two different threads
    *
    * In our usage, this isn't a problem, as a given feature ID is always operated on by a single thread
    * due to kafka consumer partitioning
    */
  override def put(feature: SimpleFeature): Unit = {
    val featureState = createState(feature, 0L)
    logger.trace(s"${featureState.id} adding feature with expiry $expiry")
    if (expiry > 0) {
      featureState.expire = executor.schedule(featureState, expiry, TimeUnit.MILLISECONDS)
      val old = state.put(featureState.id, featureState)
      if (old != null) {
        old.expire.cancel(false)
        logger.trace(s"${featureState.id} removing old feature")
        old.removeFromIndex()
      }
    } else {
      val old = state.put(featureState.id, featureState)
      if (old != null) {
        logger.trace(s"${featureState.id} removing old feature")
        old.removeFromIndex()
      }
    }
    featureState.insertIntoIndex()
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
      if (old.expire != null) {
        old.expire.cancel(false)
      }
      old.removeFromIndex()
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

  override def close(): Unit = if (executor != null) { executor.shutdown() }

  /**
    * Holder for our key feature values
    *
    * @param feature simple feature
    * @param time feature or message time
    */
  protected abstract class FeatureState(val feature: SimpleFeature, val time: Long) extends Runnable {

    val id: String = feature.getID

    var expire: ScheduledFuture[_] = _

    def insertIntoIndex(): Unit
    def removeFromIndex(): SimpleFeature
    def retrieveFromIndex(): SimpleFeature

    override def run(): Unit = {
      logger.trace(s"$id expiring from index")
      if (state.remove(id, this)) {
        removeFromIndex()
      }
      logger.trace(s"Current index size: ${state.size()}/${support.index.size()}")
    }
  }

}

object BasicFeatureCache {

  trait PointOps {

    this: BasicFeatureCache =>

    override protected def createState(feature: SimpleFeature, time: Long): FeatureState =
      new FeatureStatePoint(feature, time)

    /**
      * Point feature state
      *
      * @param feature simple feature
      * @param time feature or message time
      */
    protected class FeatureStatePoint(feature: SimpleFeature, time: Long) extends FeatureState(feature, time) {
      private val point = feature.getAttribute(geom).asInstanceOf[Point]
      private val x = point.getX
      private val y = point.getY

      override def insertIntoIndex(): Unit = support.index.insert(x, y, id, feature)
      override def removeFromIndex(): SimpleFeature = support.index.remove(x, y, id)
      override def retrieveFromIndex(): SimpleFeature = support.index.get(x, y, id)
    }
  }

  trait ExtentOps {

    this: BasicFeatureCache =>

    override protected def createState(feature: SimpleFeature, time: Long): FeatureState =
      new FeatureStateNonPoint(feature, time)

    /**
      * Non-point feature state
      *
      * @param feature simple feature
      * @param time feature or message time
      */
    protected class FeatureStateNonPoint(feature: SimpleFeature, time: Long) extends FeatureState(feature, time) {

      // TODO https://geomesa.atlassian.net/browse/GEOMESA-2323 better anti-meridian handling

      private val envelope = feature.getAttribute(geom).asInstanceOf[Geometry].getEnvelopeInternal

      override def insertIntoIndex(): Unit = support.index.insert(envelope, id, feature)
      override def removeFromIndex(): SimpleFeature = support.index.remove(envelope, id)
      override def retrieveFromIndex(): SimpleFeature = support.index.get(envelope, id)
    }
  }
}