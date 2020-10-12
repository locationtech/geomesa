/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import java.io.Closeable
import java.time.ZonedDateTime
import java.util
import java.util.concurrent.TimeUnit

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, Query}
import org.locationtech.geomesa.filter.{Bounds, FilterValues}
import org.locationtech.geomesa.index.api
import org.locationtech.geomesa.index.api.QueryStrategy
import org.locationtech.geomesa.index.index.{SpatialIndexValues, TemporalIndexValues}
import org.locationtech.geomesa.index.metadata.TableBasedMetadata
import org.locationtech.geomesa.index.planning.QueryInterceptor.GraduatedQueryGuard.{SizeAndDuration, loadConfiguration}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes.Configs
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.opengis.feature.simple.SimpleFeatureType

import scala.concurrent.duration.{Duration, FiniteDuration}
import scala.util.Try
import scala.util.control.NonFatal

/**
  * Provides a hook to modify a query before executing it
  */
trait QueryInterceptor extends Closeable {

  /**
    * Called exactly once after the interceptor is instantiated
    *
    * @param ds data store
    * @param sft simple feature type
    */
  def init(ds: DataStore, sft: SimpleFeatureType): Unit

  /**
    * Modifies the query in place
    *
    * @param query query
    */
  def rewrite(query: Query): Unit

  /**
   * Hook to allow interception of a query after extracting the query values
   *
   * @param strategy query strategy
   * @return an exception if the query should be stopped
   */
  def guard(strategy: QueryStrategy): Option[IllegalArgumentException] = None
}

object QueryInterceptor extends LazyLogging {

  /**
    * Manages query interceptors
    */
  trait QueryInterceptorFactory extends Closeable {
    def apply(sft: SimpleFeatureType): Seq[QueryInterceptor]
  }

  object QueryInterceptorFactory {

    private val Empty: QueryInterceptorFactory = new QueryInterceptorFactory {
      override def apply(sft: SimpleFeatureType): Seq[QueryInterceptor] = Seq.empty
      override def close(): Unit = {}
    }

    def apply(ds: DataStore): QueryInterceptorFactory = new QueryInterceptorFactoryImpl(ds)

    def empty(): QueryInterceptorFactory = Empty

    /**
      * Manages query interceptor lifecycles for a given data store
      *
      * @param ds data store
      */
    private class QueryInterceptorFactoryImpl(ds: DataStore) extends QueryInterceptorFactory {

      import scala.collection.JavaConverters._

      private val expiry = TableBasedMetadata.Expiry.toDuration.get.toMillis

      private val loader =  new CacheLoader[String, Seq[QueryInterceptor]]() {
        override def load(key: String): Seq[QueryInterceptor] = QueryInterceptorFactoryImpl.this.load(key)
        override def reload(key: String, oldValue: Seq[QueryInterceptor]): Seq[QueryInterceptor] = {
          // only recreate the interceptors if they have changed
          Option(ds.getSchema(key)).flatMap(s => Option(s.getUserData.get(Configs.QueryInterceptors))) match {
            case None if oldValue.isEmpty => oldValue
            case Some(classes) if classes == oldValue.map(_.getClass.getName).mkString(",") => oldValue
            case _ => CloseWithLogging(oldValue); QueryInterceptorFactoryImpl.this.load(key)
          }
        }
      }

      private val cache = Caffeine.newBuilder().refreshAfterWrite(expiry, TimeUnit.MILLISECONDS).build(loader)

      override def apply(sft: SimpleFeatureType): Seq[QueryInterceptor] = cache.get(sft.getTypeName)

      override def close(): Unit = {
        cache.asMap.asScala.foreach { case (_, interceptors) => CloseWithLogging(interceptors) }
        cache.invalidateAll()
      }

      private def load(typeName: String): Seq[QueryInterceptor] = {
        val sft = ds.getSchema(typeName)
        if (sft == null) { Seq.empty } else {
          val classes = sft.getUserData.get(Configs.QueryInterceptors).asInstanceOf[String]
          if (classes == null || classes.isEmpty) { Seq.empty } else {
            classes.split(",").toSeq.flatMap { c =>
              var interceptor: QueryInterceptor = null
              try {
                interceptor = Class.forName(c).newInstance().asInstanceOf[QueryInterceptor]
                interceptor.init(ds, sft)
                Seq(interceptor)
              } catch {
                case NonFatal(e) =>
                  logger.error(s"Error creating query interceptor '$c':", e)
                  if (interceptor != null) {
                    CloseWithLogging(interceptor)
                  }
                  Seq.empty
              }
            }
          }
        }
      }
    }
  }

  class TemporalQueryGuard extends QueryInterceptor {

    import TemporalQueryGuard.Config
    import org.locationtech.geomesa.filter.filterToString

    private var max: Duration = _

    override def init(ds: DataStore, sft: SimpleFeatureType): Unit = {
      max = Try(Duration(sft.getUserData.get(Config).asInstanceOf[String])).getOrElse {
        throw new IllegalArgumentException(
          s"Temporal query guard expects valid duration under user data key '$Config'")
      }
    }

    override def rewrite(query: Query): Unit = {}

    override def guard(strategy: QueryStrategy): Option[IllegalArgumentException] = {
      val intervals = strategy.values.collect { case v: TemporalIndexValues => v.intervals }
      intervals.collect { case i if i.isEmpty || !i.forall(_.isBoundedBothSides) || duration(i.values) > max =>
          new IllegalArgumentException(
            s"Query exceeds maximum allowed filter duration of $max: ${filterToString(strategy.filter.filter)}")
      }
    }

    override def close(): Unit = {}

  }

  object TemporalQueryGuard {
    val Config = "geomesa.filter.max.duration"
  }

  object GraduatedQueryGuard extends LazyLogging {
    val ConfigPath = "geomesa.guards.graduated-guard"

    /**
     *
     * @param sizeLimit in square degrees
     * @param durationLimit Maximum duration for a query at or below the spatial size
     */
    case class SizeAndDuration(sizeLimit: Int, durationLimit: Duration)

    def loadConfiguration(sft: SimpleFeatureType): Seq[SizeAndDuration]  = {
      val conf = ConfigFactory.load()
      try {
        val guardConfig = conf.getConfigList(s"${ConfigPath}.${sft.getTypeName}")
        buildLimits(guardConfig)
      } catch {
        case e: ConfigException =>
          logger.error(s"Configuration for GraduatedQueryGuard not available or correct for type ${sft.getTypeName}.  " +
            s"This query guard is disabled.")
          Seq()
      }
    }

    /**
     * This function checks conditions on the limits.
     * 1. Sizes must not be repeated.
     * 2. Smaller sizes must allow for longer durations.
     * @param limits Sequence of limits
     * @return Sorted list of limits or throws an IllegalArgumentException
     */
    private def evaluateLimits(limits: Seq[SizeAndDuration]): Seq[SizeAndDuration] = {
      val candidate = limits.sortBy(_.sizeLimit)
      if (candidate.size > 1) {
        candidate.sliding(2).foreach { case Seq(first, second) =>
          if (first.sizeLimit == second.sizeLimit) {
            throw new IllegalArgumentException(s"Graduated query guard configuration " +
              s"has repeated size: ${first.sizeLimit}")
          } else if (first.durationLimit.compareTo(second.durationLimit) <= 0) {
            throw new IllegalArgumentException(s"Graduated query guard configuration " +
              s"has durations out of order: ${first.durationLimit} is less than ${second.durationLimit}")
          }
        }
      }
      candidate
    }

    def buildLimits(guardConfig: util.List[_ <: Config]): Seq[SizeAndDuration] = {
      import scala.collection.JavaConverters._
      val confs = guardConfig.asScala.map { durationConfig =>
        val size: Int = durationConfig.hasPath("size") match {
          case true => durationConfig.getInt("size")
          case false => Int.MaxValue
        }
        val duration = Duration(durationConfig.getString("duration"))
        SizeAndDuration(size, duration)
      }
      evaluateLimits(confs)
    }
  }

  class GraduatedQueryGuard extends QueryInterceptor {
    var guardLimits: Seq[SizeAndDuration] = Seq()

    /**
     * Called exactly once after the interceptor is instantiated
     *
     * @param ds  data store
     * @param sft simple feature type
     */
    override def init(ds: DataStore, sft: SimpleFeatureType): Unit = {
      guardLimits = loadConfiguration(sft)
    }

    /**
     * Modifies the query in place
     *
     * @param query query
     */
    override def rewrite(query: Query): Unit = { }

    /**
     * Hook to allow interception of a query after extracting the query values
     *
     * @param strategy query strategy
     * @return an exception if the query should be stopped
     */
    override def guard(strategy: api.QueryStrategy): Option[IllegalArgumentException] = {
      val intervalsOption: Option[FilterValues[Bounds[ZonedDateTime]]] = strategy.values.collect { case v: TemporalIndexValues => v.intervals }
      val spatialBoundsOption: Option[Seq[(Double, Double, Double, Double)]] = strategy.values.collect { case v: SpatialIndexValues => v.spatialBounds }

      val spatialExtent = spatialBoundsOption match {
        case None => 360.0 * 180.0  // Whole world
        case Some(seq) => seq.map { case  (lx: Double, ly: Double, ux: Double, uy: Double) =>
          (ux-lx) * (uy-ly)
        }.sum
      }

      intervalsOption match {
        // The None case reflects a query without a temporal extent or a full-table scan on a table with a temporal extent
        // The full-table scan block should be used to avoid the later case.
        // Here a none is returned indicating that this guard does not stop the query
        case None => None
        case Some(intervals) =>
          if (!intervals.forall(_.isBoundedBothSides) || intervals.isEmpty) {
            Some(new IllegalArgumentException("At least one part of the query is temporally unbounded.  Add bounded temporal constraints."))
          } else {
            val queryDuration = duration(intervals.values)
            guardLimits.find( _.sizeLimit >= spatialExtent).flatMap {
              limit =>
                if (queryDuration > limit.durationLimit) {
                  Some(new IllegalArgumentException(s"Query of spatial size: $spatialExtent and duration: $queryDuration failed with constraint: $limit"))
                } else {
                  None
                }
            }
          }
      }
    }

    override def close(): Unit = { }
  }

  private def duration(values: Seq[Bounds[ZonedDateTime]]): FiniteDuration = {
    values.foldLeft(Duration.Zero) { (sum, bounds) =>
      sum + Duration(bounds.upper.value.get.toEpochSecond - bounds.lower.value.get.toEpochSecond, TimeUnit.SECONDS)
    }
  }
}
