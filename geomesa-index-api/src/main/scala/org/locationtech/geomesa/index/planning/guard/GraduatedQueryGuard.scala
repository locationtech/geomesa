/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning.guard

import java.time.ZonedDateTime
import java.util

import com.typesafe.config.{Config, ConfigException, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, Query}
import org.locationtech.geomesa.filter.{Bounds, FilterValues}
import org.locationtech.geomesa.index.api
import org.locationtech.geomesa.index.index.{SpatialIndexValues, TemporalIndexValues}
import org.locationtech.geomesa.index.planning.QueryInterceptor
import org.locationtech.geomesa.index.planning.guard.GraduatedQueryGuard.{SizeAndDuration, loadConfiguration}
import org.opengis.feature.simple.SimpleFeatureType

import scala.concurrent.duration.Duration

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
