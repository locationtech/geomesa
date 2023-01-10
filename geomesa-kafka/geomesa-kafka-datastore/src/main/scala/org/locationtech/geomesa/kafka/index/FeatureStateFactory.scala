/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka.index

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
import java.io.Closeable
import java.util.Date
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}
<<<<<<< HEAD
>>>>>>> c8e6854100 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> c8e685410 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
>>>>>>> 5ad2f6a7b2 (GEOMESA-3151 Fix CLI GT dependency versions (#2812))
=======
>>>>>>> d845d7c1bd (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
import com.github.benmanes.caffeine.cache.Ticker
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.api.filter.expression.Expression
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.kafka.data.KafkaDataStore._
import org.locationtech.geomesa.kafka.index.FeatureStateFactory.FeatureState
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.index.SpatialIndex
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.locationtech.jts.geom.Geometry

import java.io.Closeable
import java.util.Date
import java.util.concurrent.{ScheduledExecutorService, ScheduledFuture, ScheduledThreadPoolExecutor, TimeUnit}
import scala.util.control.NonFatal

/**
  * Factory trait for creating feature states
  */
trait FeatureStateFactory extends Closeable {
  def createState(feature: SimpleFeature): FeatureState
}

object FeatureStateFactory extends LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def apply(
      sft: SimpleFeatureType,
      index: SpatialIndex[SimpleFeature],
      expiry: ExpiryTimeConfig,
      expiration: FeatureExpiration,
      executor: Option[(ScheduledExecutorService, Ticker)]): FeatureStateFactory = {

    val geom = sft.getGeomIndex

    lazy val (es, ticker) = executor.getOrElse {
      val es = new ScheduledThreadPoolExecutor(2)
      // don't keep running scheduled tasks after shutdown
      es.setExecuteExistingDelayedTasksAfterShutdownPolicy(false)
      // remove tasks when canceled, otherwise they will only be removed from the task queue
      // when they would be executed. we expect frequent cancellations due to feature updates
      es.setRemoveOnCancelPolicy(true)
      (es, CurrentTimeTicker)
    }

    expiry match {
      case NeverExpireConfig =>
        new BasicFactory(index, geom)

      case IngestTimeConfig(ex) =>
        new ExpiryFactory(index, geom, expiration, es, ex.toMillis)

      case EventTimeConfig(ex, time, ordering) =>
        val expression = FastFilterFactory.toExpression(sft, time)
        if (!ex.isFinite && ordering) {
          new EventTimeFactory(index, geom, expression)
        } else if (ordering) {
          new EventTimeOrderedExpiryFactory(index, geom, expression, expiration, es, ticker, ex.toMillis)
        } else {
          new EventTimeExpiryFactory(index, geom, expression, expiration, es, ticker, ex.toMillis)
        }

      case FilteredExpiryConfig(ex) =>
        val delegates = ex.map { case (ecql, e) =>
          FastFilterFactory.toFilter(sft, ecql) -> apply(sft, index, e, expiration, Some(es -> ticker))
        }
        new FilteredExpiryFactory(delegates)

      case ImmediatelyExpireConfig =>
        throw new IllegalStateException("Can't use feature state with immediate expiration")
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
    override def toString: String = s"BasicFactory[geom:$geom]"
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

    override def toString: String = s"ExpiryFactory[geom:$geom,expiry:$expiry]"
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

    override def toString: String = s"EventTimeFactory[geom:$geom,eventTime:${ECQL.toCQL(eventTime)}]"
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
      val expiry = FeatureStateFactory.time(eventTime, feature) + this.expiry - (ticker.read() / 1000000L)
      if (expiry < 1L) {
        new ExpiredState(feature, 0L, expiration)
      } else {
        new ExpiryState(feature, index, geom, 0L, expiration, executor, expiry)
      }
    }

    override def close(): Unit = executor.shutdownNow()

    override def toString: String =
      s"EventTimeExpiryFactory[geom:$geom,eventTime:${ECQL.toCQL(eventTime)},expiry:$expiry]"
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
      val expiry = time + this.expiry - (ticker.read() / 1000000L)
      if (expiry < 1L) {
        new ExpiredState(feature, time, expiration)
      } else {
        new ExpiryState(feature, index, geom, time, expiration, executor, expiry)
      }
    }

    override def close(): Unit = executor.shutdownNow()

    override def toString: String =
      s"EventTimeOrderedExpiryFactory[geom:$geom,eventTime:${ECQL.toCQL(eventTime)},expiry:$expiry]"
  }

  class FilteredExpiryFactory(delegates: Seq[(Filter, FeatureStateFactory)]) extends FeatureStateFactory {

    require(delegates.last._1 == Filter.INCLUDE,
      "Filter feature state factory requires a fall back Filter.INCLUDE entry")

    override def createState(feature: SimpleFeature): FeatureState = {
      val opt = delegates.collectFirst {
        case (f, factory) if f.evaluate(feature) => factory.createState(feature)
      }
      opt.get // should always get a result due to the Filter.INCLUDE
    }

    override def close(): Unit = CloseWithLogging(delegates.map(_._2))

    override def toString: String =
      s"FilteredExpiryFactory[delegates:${delegates.map { case (f, d) => s"${ECQL.toCQL(f)}->$d"}.mkString(",")}]"
  }

  object CurrentTimeTicker extends Ticker {
    override def read(): Long = System.currentTimeMillis() * 1000000L
  }
}
