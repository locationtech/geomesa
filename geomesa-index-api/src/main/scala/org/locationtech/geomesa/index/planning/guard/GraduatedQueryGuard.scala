/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning.guard

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, Query}
import org.geotools.filter.visitor.ExtractBoundsFilterVisitor
import org.locationtech.geomesa.index.api.QueryStrategy
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.{SpatialIndexValues, SpatioTemporalIndex, TemporalIndexValues}
import org.locationtech.geomesa.index.planning.QueryInterceptor
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.jts.geom.Envelope
import org.opengis.feature.simple.SimpleFeatureType

import scala.concurrent.duration.Duration

class GraduatedQueryGuard extends QueryInterceptor with LazyLogging {

  import org.locationtech.geomesa.index.planning.guard.GraduatedQueryGuard._

  private var guardLimits: Seq[SizeAndLimits] = _
  private var disabled: Boolean = false

  override def init(ds: DataStore, sft: SimpleFeatureType): Unit = {
    disabled = GraduatedQueryGuard.disabled(sft.getTypeName)
    if (disabled) {
      logger.info(s"This guard is disabled for schema '${sft.getTypeName}' via system property")
    }
    // let any errors bubble up and disable this guard
    guardLimits = buildLimits(ConfigFactory.load().getConfigList(s"$ConfigPath.${sft.getTypeName}"), sft)
    // this should be ensured during the loading of limits, but just double check so we can use `.last` safely below
    require(guardLimits.nonEmpty)
  }

  override def rewrite(query: Query): Unit = {
    if (!disabled) {
      val bounds = query.getFilter.accept(ExtractBoundsFilterVisitor.BOUNDS_VISITOR, null).asInstanceOf[Envelope]
      val spatialExtent = (bounds.getMaxX - bounds.getMinX) * (bounds.getMaxY - bounds.getMinY)
      val limit = guardLimits.find(_.sizeLimit >= spatialExtent).getOrElse{
        logger.warn(s"Invalid extents/limits: ${query.getFilter.toString} / ${guardLimits.mkString(", ")}")
        guardLimits.last
      }
      limit.percentageLimit.map{ percentage =>
        query.getHints.put(QueryHints.SAMPLING, percentage.toFloat)
        limit.sampleAttribute.map(attr => query.getHints.put(QueryHints.SAMPLE_BY, attr))
      }
    }
  }

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
          limit.durationLimit.flatMap { l =>
            if (!validate(i, l)) Some(s"Query exceeds maximum allowed filter duration of ${limit.durationLimit} " +
              s"at ${limit.sizeLimit} degrees") else None
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
   * @param samplePercent Percentage of total records to return
   * @param sampleAttribute Attribute name to use a threading key for subsampling
   */
  class SizeAndLimits(val sizeLimit: Int, val durationLimit: Option[Duration],
                      samplePercent: Option[Double], val sampleAttribute: Option[String]) {
    require(samplePercent match {
      case Some(p) => 0 < p && p <= 1
      case None => true
    }, "Graduated query guard percentages must be in range (0,1]")

    val percentageLimit: Option[Double] = samplePercent
  }

  def disabled(typeName: String): Boolean =
    SystemProperty(s"geomesa.guard.graduated.$typeName.disable").toBoolean.contains(true)

  /**
   * This function checks conditions on the limits.
   * 1. Sizes must not be repeated.
   * 2. Smaller sizes must allow for longer durations.
   * 3. If a percentage is included it must decrease with larger sizes.
   * @param limits Sequence of limits
   * @return Sorted list of limits or throws an IllegalArgumentException
   */
  private def evaluateLimits(limits: Seq[SizeAndLimits], sft: SimpleFeatureType): Seq[SizeAndLimits] = {
    val candidate = limits.sortBy(_.sizeLimit)
    if (candidate.size > 1) {
      // Once a duration or percentage is specified, all following configurations must specify it.
      var hasDuration   = false
      var hasPercentage = false
      candidate.sliding(2).foreach { case Seq(first, second) =>
        if (first.sizeLimit == second.sizeLimit) {
          throw new IllegalArgumentException(s"Graduated query guard configuration " +
            s"has repeated size: ${first.sizeLimit}")
        }

        if (first.durationLimit.isDefined || hasDuration) {
          hasDuration = true
          if (second.durationLimit.isEmpty) {
            throw new IllegalArgumentException(s"Graduated query guard configuration " +
              s"has missing duration in size = ${second.sizeLimit}")
          }
          if (first.durationLimit.get.compareTo(second.durationLimit.get) <= 0) {
            throw new IllegalArgumentException(s"Graduated query guard configuration " +
              s"has durations out of order: ${first.durationLimit.get} is less than ${second.durationLimit.get}")
          }
        }

        if (first.percentageLimit.isDefined || hasPercentage) {
          hasPercentage = true
          if (second.percentageLimit.isEmpty) {
            throw new IllegalArgumentException(s"Graduated query guard configuration " +
              s"has missing percentage in size = ${second.sizeLimit}")
          }
          if (first.percentageLimit.get < second.percentageLimit.get) {
            throw new IllegalArgumentException(s"Graduated query guard configuration " +
              s"has percentages out of order or missing: " +
              s"${first.percentageLimit.get} is less than ${second.durationLimit.get}")
          }
        }

        Seq(first.sampleAttribute, second.sampleAttribute).foreach{
          case Some(attr) if sft.indexOf(attr) == -1 =>
            throw new IllegalArgumentException(s"Graduated query guard configuration " +
              s"has invalid attribute name for filter. ${first.sampleAttribute.get} not in sft ${sft.getTypeName}")
          case _ =>
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

  def buildLimits(guardConfig: java.util.List[_ <: Config], sft: SimpleFeatureType): Seq[SizeAndLimits] = {
    import scala.collection.JavaConverters._
    val confs = guardConfig.asScala.map { limitsConfig =>
      val size: Int = if (limitsConfig.hasPath("size")) {
        limitsConfig.getInt("size")
      } else {
        Int.MaxValue
      }

      val duration: Option[Duration] = if (limitsConfig.hasPath("duration")) {
        Some(Duration(limitsConfig.getString("duration")))
      } else None

      val percentage: Option[Double] = if (limitsConfig.hasPath("sampling-percentage")) {
        Some(limitsConfig.getDouble("sampling-percentage"))
      } else None

      val percentageAttr: Option[String] = if (limitsConfig.hasPath("sampling-attribute")) {
        Some(limitsConfig.getString("sampling-attribute"))
      } else None

      new SizeAndLimits(size, duration, percentage, percentageAttr)
    }
    evaluateLimits(confs.toSeq, sft)
  }
}
