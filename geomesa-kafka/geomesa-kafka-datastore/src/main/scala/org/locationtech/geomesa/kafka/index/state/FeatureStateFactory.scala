/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index.state

import java.io.Closeable
import java.util.Date
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.geotools.util.Converters
import org.locationtech.geomesa.kafka.index.state.FeatureStateFactory.FeatureState
import org.locationtech.geomesa.utils.cache.Ticker
import org.locationtech.geomesa.utils.index.SpatialIndex
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.expression.Expression

import scala.util.control.NonFatal

/**
  * Factory trait for creating feature states
  */
trait FeatureStateFactory extends Closeable {
  def createState(feature: SimpleFeature): FeatureState
}

object FeatureStateFactory extends LazyLogging {

  def time(expression: Expression, feature: SimpleFeature): Long = {
    try {
      expression.evaluate(feature) match {
        case d: Date   => d.getTime
        case d: Number => d.longValue()
        case d => Option(Converters.convert(d, classOf[Date])).map(_.getTime).getOrElse(0L)
      }
    } catch {
      case NonFatal(e) => logger.error(s"Error evaluating event time for $feature", e); 0L
    }
  }

  def apply(index: SpatialIndex[SimpleFeature],
            expiry: Option[(FeatureExpiration, ScheduledExecutorService, Ticker, Long)],
            eventTime: Option[(Expression, Boolean)],
            points: Boolean,
            geom: Int): FeatureStateFactory = {

    if (points) {
      (expiry, eventTime) match {
        case (None, None) => new PointsFeatureStateFactory(index, geom)
        case (Some((ex, es, _, e)), None) => new PointsExpiryFeatureStateFactory(index, geom, ex, es, e)
        case (None, Some((ev, _))) => new PointsEventTimeFeatureStateFactory(index, geom, ev)
        case (Some((ex, es, t, e)), Some((ev, false))) => new PointsEventTimeExpiryFeatureStateFactory(index, geom, ev, ex, es, t, e)
        case (Some((ex, es, t, e)), Some((ev, true))) => new PointsEventTimeExpiryOrderingFeatureStateFactory(index, geom, ev, ex, es, t, e)
      }
    } else {
      (expiry, eventTime) match {
        case (None, None) => new ExtentsFeatureStateFactory(index, geom)
        case (Some((ex, es, _, e)), None) => new ExtentsExpiryFeatureStateFactory(index, geom, ex, es, e)
        case (None, Some((ev, _))) => new ExtentsEventTimeFeatureStateFactory(index, geom, ev)
        case (Some((ex, es, t, e)), Some((ev, false))) => new ExtentsEventTimeExpiryFeatureStateFactory(index, geom, ev, ex, es, t, e)
        case (Some((ex, es, t, e)), Some((ev, true))) => new ExtentsEventTimeExpiryOrderingFeatureStateFactory(index, geom, ev, ex, es, t, e)
      }
    }
  }

  /**
    * Trait for expiring features
    */
  trait FeatureExpiration {
    def expire(featureState: FeatureState): Unit
  }

  /**
    * Holder for our key feature values
    */
  trait FeatureState {

    def id: String
    def time: Long

    def insertIntoIndex(): Unit
    def retrieveFromIndex(): SimpleFeature
    def removeFromIndex(): SimpleFeature
  }

  class PointState(feature: SimpleFeature, index: SpatialIndex[SimpleFeature], geom: Int, val time: Long)
      extends FeatureState {

    private val point = feature.getAttribute(geom).asInstanceOf[Point]
    private val x = point.getX
    private val y = point.getY

    override val id: String = feature.getID
    override def insertIntoIndex(): Unit = index.insert(x, y, id, feature)
    override def removeFromIndex(): SimpleFeature = index.remove(x, y, id)
    override def retrieveFromIndex(): SimpleFeature = index.get(x, y, id)
  }

  class PointExpiryState(feature: SimpleFeature,
                         index: SpatialIndex[SimpleFeature],
                         geom: Int,
                         time: Long,
                         expiration: FeatureExpiration,
                         executor: ScheduledExecutorService,
                         expiry: Long) extends PointState(feature, index, geom, time) with Runnable {

    private var future: ScheduledFuture[_] = _

    override def run(): Unit = expiration.expire(this)

    override def insertIntoIndex(): Unit = {
      super.insertIntoIndex()
      future = executor.schedule(this, expiry, TimeUnit.MILLISECONDS)
    }

    override def removeFromIndex(): SimpleFeature = {
      future.cancel(false)
      super.removeFromIndex()
    }
  }

  class ExtentsState(feature: SimpleFeature, index: SpatialIndex[SimpleFeature], geom: Int, val time: Long)
      extends FeatureState {

    // TODO https://geomesa.atlassian.net/browse/GEOMESA-2323 better anti-meridian handling

    private val envelope = feature.getAttribute(geom).asInstanceOf[Geometry].getEnvelopeInternal

    override val id: String = feature.getID
    override def insertIntoIndex(): Unit = index.insert(envelope, id, feature)
    override def removeFromIndex(): SimpleFeature = index.remove(envelope, id)
    override def retrieveFromIndex(): SimpleFeature = index.get(envelope, id)
  }

  class ExtentsExpiryState(feature: SimpleFeature,
                           index: SpatialIndex[SimpleFeature],
                           geom: Int,
                           time: Long,
                           expiration: FeatureExpiration,
                           executor: ScheduledExecutorService,
                           expiry: Long) extends ExtentsState(feature, index, geom, time) with Runnable {

    private var future: ScheduledFuture[_] = _

    override def run(): Unit = expiration.expire(this)

    override def insertIntoIndex(): Unit = {
      super.insertIntoIndex()
      future = executor.schedule(this, expiry, TimeUnit.MILLISECONDS)
    }

    override def removeFromIndex(): SimpleFeature = {
      future.cancel(false)
      super.removeFromIndex()
    }
  }

  class ExpiredState(feature: SimpleFeature, val time: Long, expiration: FeatureExpiration) extends FeatureState {
    override val id: String = feature.getID
    override def insertIntoIndex(): Unit = expiration.expire(this)
    override def retrieveFromIndex(): SimpleFeature = null
    override def removeFromIndex(): SimpleFeature = null
  }
}
