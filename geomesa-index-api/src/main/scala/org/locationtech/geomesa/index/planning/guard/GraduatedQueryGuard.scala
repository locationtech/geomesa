/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning.guard

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, Query}
import org.locationtech.geomesa.index.api.QueryStrategy
import org.locationtech.geomesa.index.index.{SpatialIndexValues, SpatioTemporalIndex, TemporalIndexValues}
import org.locationtech.geomesa.index.planning.QueryInterceptor
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.SimpleFeatureType

import scala.concurrent.duration.Duration

class GraduatedQueryGuard extends QueryInterceptor with LazyLogging {

  import org.locationtech.geomesa.index.planning.guard.GraduatedQueryGuard._

  private var guardLimits: Seq[SizeAndDuration] = _
  private var disabled: Boolean = false

  override def init(ds: DataStore, sft: SimpleFeatureType): Unit = {
    disabled = GraduatedQueryGuard.disabled(sft.getTypeName)
    if (disabled) {
      logger.info(s"This guard is disabled for schema '${sft.getTypeName}' via system property")
    }
    // let any errors bubble up and disable this guard
    guardLimits = buildLimits(ConfigFactory.load().getConfigList(s"$ConfigPath.${sft.getTypeName}"))
    // this should be ensured during the loading of limits, but just double check so we can use `.last` safely below
    require(guardLimits.nonEmpty)
  }

  override def rewrite(query: Query): Unit = {}

  override def guard(strategy: QueryStrategy): Option[IllegalArgumentException] = {
    val msg = if (disabled || !strategy.index.isInstanceOf[SpatioTemporalIndex[_, _]]) { None } else {
      val values = strategy.values.collect {
        case v: SpatialIndexValues with TemporalIndexValues => (v.spatialBounds, v.intervals)
      }
      values match {
        case None =>
          Some(s"Query does not have a temporal filter. Maximum allowed filter duration for " +
              s"whole world queries is ${guardLimits.last.durationLimit}")

        case Some((s, i)) =>
          val spatialExtent = s.map { case (lx, ly, ux, uy) => (ux - lx) * (uy - ly) }.sum
          val limit = guardLimits.find(_.sizeLimit >= spatialExtent).getOrElse {
            // should always be a valid limit due to our checks building the limit...
            logger.warn(s"Invalid extents/limits: ${s.mkString(", ")} / ${guardLimits.mkString(", ")}")
            guardLimits.last
          }
          if (validate(i, limit.durationLimit)) { None } else {
            Some(s"Query exceeds maximum allowed filter duration of ${limit.durationLimit} at ${limit.sizeLimit} degrees")
          }
      }
    }
    msg.map(m => new IllegalArgumentException(s"$m: ${filterString(strategy)}"))
  }

  override def close(): Unit = {}
}

object GraduatedQueryGuard extends LazyLogging {

  val ConfigPath = "geomesa.guard.graduated"

  /**
   *
   * @param sizeLimit in square degrees
   * @param durationLimit Maximum duration for a query at or below the spatial size
   */
  case class SizeAndDuration(sizeLimit: Int, durationLimit: Duration)

  def disabled(typeName: String): Boolean =
    SystemProperty(s"geomesa.guard.graduated.$typeName.disable").toBoolean.contains(true)

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
    } else if (candidate.isEmpty) {
      throw new IllegalArgumentException(s"Graduated query guard configuration is empty.")
    }
    if (candidate.last.sizeLimit != Int.MaxValue) {
      throw new IllegalArgumentException(s"Graduated query guard configuration must include " +
        "unbounded restriction.")
    }
    candidate
  }

  def buildLimits(guardConfig: java.util.List[_ <: Config]): Seq[SizeAndDuration] = {
    import scala.collection.JavaConverters._
    val confs = guardConfig.asScala.map { durationConfig =>
      val size: Int = if (durationConfig.hasPath("size")) {
        durationConfig.getInt("size")
      } else {
        Int.MaxValue
      }
      val duration = Duration(durationConfig.getString("duration"))
      SizeAndDuration(size, duration)
    }
    evaluateLimits(confs)
  }
}
