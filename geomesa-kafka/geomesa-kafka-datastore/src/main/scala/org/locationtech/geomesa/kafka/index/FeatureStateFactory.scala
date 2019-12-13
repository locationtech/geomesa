/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

import java.io.Closeable
import java.util.Date
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, TimeUnit}

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Geometry
import org.locationtech.geomesa.kafka.index.FeatureStateFactory.FeatureState
import org.locationtech.geomesa.utils.cache.Ticker
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
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

  def apply(index: SpatialIndex[SimpleFeature],
            expiry: Option[(FeatureExpiration, ScheduledExecutorService, Ticker, Long)],
            eventTime: Option[(Expression, Boolean)],
            geom: Int): FeatureStateFactory = {
    (expiry, eventTime) match {
      case (None, None) => new BasicFactory(index, geom)
      case (Some((ex, es, _, e)), None) => new ExpiryFactory(index, geom, ex, es, e)
      case (None, Some((ev, _))) => new EventTimeFactory(index, geom, ev)
      case (Some((ex, es, t, e)), Some((ev, false))) => new EventTimeExpiryFactory(index, geom, ev, ex, es, t, e)
      case (Some((ex, es, t, e)), Some((ev, true))) => new EventTimeOrderedExpiryFactory(index, geom, ev, ex, es, t, e)
    }
  }

  def time(expression: Expression, feature: SimpleFeature): Long = {
    try {
      expression.evaluate(feature) match {
        case d: Date   => d.getTime
        case d: Number => d.longValue()
        case d => Option(FastConverter.convert(d, classOf[Date])).map(_.getTime).getOrElse(0L)
      }
    } catch {
      case NonFatal(e) => logger.error(s"Error evaluating event time for $feature", e); 0L
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

  /**
    * Basic state, handles inserts and updates to the spatial index
    *
    * @param feature feature
    * @param index spatial index
    * @param geom geometry attribute index
    * @param time feature time
    */
  class BasicState(feature: SimpleFeature, index: SpatialIndex[SimpleFeature], geom: Int, val time: Long)
      extends FeatureState {

    private val g = feature.getAttribute(geom).asInstanceOf[Geometry]

    override val id: String = feature.getID
    override def insertIntoIndex(): Unit = index.insert(g, id, feature)
    override def removeFromIndex(): SimpleFeature = index.remove(g, id)
    override def retrieveFromIndex(): SimpleFeature = index.get(g, id)

    override def toString: String = s"FeatureState($feature)"
  }

  /**
    * Handles time-based expiration
    *
    * @param feature feature
    * @param index spatial index
    * @param geom geometry attribute index
    * @param time feature time
    * @param expiration callback for expiration
    * @param executor executor used to schedule expiration callback
    * @param expiry expiry in milliseconds
    */
  class ExpiryState(feature: SimpleFeature,
                    index: SpatialIndex[SimpleFeature],
                    geom: Int,
                    time: Long,
                    expiration: FeatureExpiration,
                    executor: ScheduledExecutorService,
                    expiry: Long) extends BasicState(feature, index, geom, time) with Runnable {

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

    override def toString: String = s"ExpiryState($feature)"
  }

  /**
    * Already expired state - will short-circuit inserting and then expiring itself
    *
    * @param feature feature
    * @param time feature time
    * @param expiration expiration callback
    */
  class ExpiredState(feature: SimpleFeature, val time: Long, expiration: FeatureExpiration) extends FeatureState {
    override val id: String = feature.getID
    override def insertIntoIndex(): Unit = expiration.expire(this)
    override def retrieveFromIndex(): SimpleFeature = null
    override def removeFromIndex(): SimpleFeature = null
    override def toString: String = s"ExpiredState($feature)"
  }


  /**
    * Basic feature state factory
    *
    * @param index spatial index
    * @param geom geometry attribute index
    */
  class BasicFactory(index: SpatialIndex[SimpleFeature], geom: Int) extends FeatureStateFactory {
    override def createState(feature: SimpleFeature): FeatureState = new BasicState(feature, index, geom, 0L)
    override def close(): Unit = {}
  }

  /**
    * Feature state factory with timed expiration
    *
    * @param index spatial index
    * @param geom geometry attribute index
    * @param expiration expiration callback
    * @param executor executor service
    * @param expiry expiry in millis
    */
  class ExpiryFactory(index: SpatialIndex[SimpleFeature],
                      geom: Int,
                      expiration: FeatureExpiration,
                      executor: ScheduledExecutorService,
                      expiry: Long) extends FeatureStateFactory {

    override def createState(feature: SimpleFeature): FeatureState =
      new ExpiryState(feature, index, geom, 0L, expiration, executor, expiry)

    override def close(): Unit = executor.shutdownNow()
  }

  /**
    * Feature state factory with event time ordering
    *
    * @param index spatial index
    * @param geom geometry attribute
    * @param eventTime event time expression
    */
  class EventTimeFactory(index: SpatialIndex[SimpleFeature], geom: Int, eventTime: Expression)
      extends FeatureStateFactory with LazyLogging {

    override def createState(feature: SimpleFeature): FeatureState =
      new BasicState(feature, index, geom, FeatureStateFactory.time(eventTime, feature))

    override def close(): Unit = {}
  }

  /**
    * Feature state factory with event time expiration but message time ordering
    *
    * @param index spatial index
    * @param geom geometry attribute
    * @param eventTime event time expression
    * @param expiration expiration callback
    * @param executor executor
    * @param ticker scheduler
    * @param expiry expiry in millis
    */
  class EventTimeExpiryFactory(index: SpatialIndex[SimpleFeature],
                               geom: Int,
                               eventTime: Expression,
                               expiration: FeatureExpiration,
                               executor: ScheduledExecutorService,
                               ticker: Ticker,
                               expiry: Long) extends FeatureStateFactory {

    override def createState(feature: SimpleFeature): FeatureState = {
      val expiry = FeatureStateFactory.time(eventTime, feature) + this.expiry - ticker.currentTimeMillis()
      if (expiry < 1L) {
        new ExpiredState(feature, 0L, expiration)
      } else {
        new ExpiryState(feature, index, geom, 0L, expiration, executor, expiry)
      }
    }

    override def close(): Unit = executor.shutdownNow()
  }

  /**
    * Feature state factory with event time ordering and expiration
    *
    * @param index spatial index
    * @param geom geometry attribute
    * @param eventTime event time expression
    * @param expiration expiration callback
    * @param executor executor
    * @param ticker scheduler
    * @param expiry expiry in millis
    */
  class EventTimeOrderedExpiryFactory(index: SpatialIndex[SimpleFeature],
                                      geom: Int,
                                      eventTime: Expression,
                                      expiration: FeatureExpiration,
                                      executor: ScheduledExecutorService,
                                      ticker: Ticker,
                                      expiry: Long) extends FeatureStateFactory {

    override def createState(feature: SimpleFeature): FeatureState = {
      val time = FeatureStateFactory.time(eventTime, feature)
      val expiry = time + this.expiry - ticker.currentTimeMillis()
      if (expiry < 1L) {
        new ExpiredState(feature, time, expiration)
      } else {
        new ExpiryState(feature, index, geom, time, expiration, executor, expiry)
      }
    }

    override def close(): Unit = executor.shutdownNow()
  }
}
