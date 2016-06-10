/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data.stats

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.Geometry
import org.joda.time.DateTime
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.accumulo.index.RecordIdxStrategy
import org.locationtech.geomesa.curve.{Z2SFC, Z3SFC}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.stats._
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.collection.JavaConversions._

trait StatsBasedEstimator {

  this: GeoMesaStats =>

  /**
    * Estimates the count for a given filter, based off the per-attribute metadata we have stored
    *
    * @param sft simple feature type
    * @param filter filter to apply - should have been run through QueryPlanFilterVisitor so all props are right
    * @return estimated count, if available
    */
  private [stats] def estimateCount(sft: SimpleFeatureType, filter: Filter): Option[Long] = {
    // TODO currently we don't consider if the dates are actually ANDed with everything else
    val dates = CountEstimator.extractDates(sft, filter)
    new CountEstimator(sft, this).estimateCount(filter, dates._1, dates._2)
  }
}

/**
  * Estimates counts based on stored statistics
  */
class CountEstimator(sft: SimpleFeatureType, stats: GeoMesaStats) extends LazyLogging {

  import CountEstimator.{ZHistogramPrecision, bounds}
  import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

  /**
    * Estimates the count for a given filter, based off the per-attribute metadata we have stored
    *
    * @param filter filter to apply - should have been run through QueryPlanFilterVisitor so all props are right
    * @param loDate bounds on the dates to be queried, if any
    * @param hiDate bounds on the dates to be queried, if any
    * @return estimated count, if available
    */
  def estimateCount(filter: Filter, loDate: Option[Date], hiDate: Option[Date]): Option[Long] = {
    import Filter.{EXCLUDE, INCLUDE}

    filter match {
      case EXCLUDE => Some(0L)
      case INCLUDE => stats.getStats[CountStat](sft).headOption.map(_.count)

      case a: And  => estimateAndCount(a, loDate, hiDate)
      case o: Or   => estimateOrCount(o, loDate, hiDate)
      case n: Not  => estimateNotCount(n, loDate, hiDate)

      case i: Id   => Some(RecordIdxStrategy.intersectIdFilters(Seq(i)).size)
      case _       =>
        // single filter - equals, between, less than, etc
        val attribute = FilterHelper.propertyNames(filter, sft).headOption
        attribute.flatMap(estimateAttributeCount(filter, _, loDate, hiDate))
    }
  }

  /**
    * Estimate counts for AND filters. Since it's an AND, we calculate the child counts and
    * return the minimum.
    *
    * We check for spatio-temporal filters first, as those are the only ones that operate on 2+ properties.
    *
    * @param filter AND filter
    * @param loDate bounds on the dates to be queried, if any
    * @param hiDate bounds on the dates to be queried, if any
    * @return estimated count, if available
    */
  private def estimateAndCount(filter: And, loDate: Option[Date], hiDate: Option[Date]): Option[Long] = {
    val stCount = estimateSpatioTemporalCount(filter)
    // note: we might over count if we get bbox1 AND bbox2, as we don't intersect them
    val individualCounts = filter.getChildren.flatMap(estimateCount(_, loDate, hiDate))
    (stCount ++ individualCounts).minOption
  }

  /**
    * Estimate counts for OR filters. Because this is an OR, we sum up the child counts
    *
    * @param filter OR filter
    * @param loDate bounds on the dates to be queried, if any
    * @param hiDate bounds on the dates to be queried, if any
    * @return estimated count, if available
    */
  private def estimateOrCount(filter: Or, loDate: Option[Date], hiDate: Option[Date]): Option[Long] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

    // estimate for each child separately and sum
    // note that we might double count some values if the filter is complex
    filter.getChildren.flatMap(estimateCount(_, loDate, hiDate)).sumOption
  }

  /**
    * Estimates the count for NOT filters
    *
    * @param filter filter
    * @param loDate bounds on the dates to be queried, if any
    * @param hiDate bounds on the dates to be queried, if any
    * @return count, if available
    */
  private def estimateNotCount(filter: Not, loDate: Option[Date], hiDate: Option[Date]): Option[Long] = {
    for {
      all <- estimateCount(Filter.INCLUDE, None, None)
      neg <- estimateCount(filter.getFilter, loDate, hiDate)
    } yield {
      math.max(0, all - neg)
    }
  }

  /**
    * Estimate spatio-temporal counts for an AND filter.
    *
    * @param filter complex filter
    * @return count, if available
    */
  private def estimateSpatioTemporalCount(filter: And): Option[Long] = {
    // currently we don't consider if the spatial predicate is actually AND'd with the temporal predicate...
    // TODO add filterhelper method that accurately pulls out the st values
    for {
      geomField <- Option(sft.getGeomField)
      dateField <- sft.getDtgField
      geometry  <- FilterHelper.extractSingleGeometry(filter, geomField)
      intervals <- Option(FilterHelper.extractIntervals(filter, dateField)).filter(_.nonEmpty)
      bounds    <- stats.getStats[MinMax[Date]](sft, Seq(dateField)).headOption
    } yield {
      val inRangeIntervals = {
        val minTime = bounds.min.getTime
        val maxTime = bounds.max.getTime
        intervals.filter(i => i._1.getMillis <= maxTime && i._2.getMillis >= minTime)
      }
      if (inRangeIntervals.isEmpty) { 0L } else {
        estimateSpatioTemporalCount(geomField, dateField, geometry, inRangeIntervals)
      }
    }
  }

  /**
    * Estimates counts based on a combination of spatial and temporal values.
    *
    * @param geomField geometry attribute name for the simple feature type
    * @param dateField date attribute name for the simple feature type
    * @param geometry geometry to evaluate
    * @param intervals intervals to evaluate
    * @return
    */
  private def estimateSpatioTemporalCount(geomField: String,
                                          dateField: String,
                                          geometry: Geometry,
                                          intervals: Seq[(DateTime, DateTime)]): Long = {

    val weeksAndOffsets = intervals.map { interval =>
      val (epochWeekStart, lt) = Z3Table.getWeekAndSeconds(interval._1)
      val (epochWeekEnd, ut) = Z3Table.getWeekAndSeconds(interval._2)
      (Range.inclusive(epochWeekStart, epochWeekEnd).map(_.toShort), lt, ut)
    }
    val allWeeks = weeksAndOffsets.flatMap(_._1).distinct

    stats.getStats[Z3Histogram](sft, Seq(geomField, dateField), allWeeks).headOption match {
      case None => 0L
      case Some(histogram) =>
        // time range for a chunk is 0 to 1 week (in seconds)
        val (tmin, tmax) = (Z3SFC.time.min.toInt, Z3SFC.time.max.toInt)
        val (lx, ly, ux, uy) = bounds(geometry)

        def getIndices(t1: Int, t2: Int): Seq[Int] = {
          val w = histogram.weeks.head // z3 histogram bounds are fixed, so indices should be the same
          val zs = Z3SFC.ranges((lx, ux), (ly, uy), (t1, t2), ZHistogramPrecision)
          zs.flatMap(r => histogram.directIndex(w, r.lower) to histogram.directIndex(w, r.upper))
        }

        // build up our indices by week so that we can deduplicate them afterwards
        val weeksAndIndices = scala.collection.mutable.Map.empty[Short, Seq[Int]].withDefaultValue(Seq.empty)

        // the z3 index breaks time into 1 week chunks, so create a range for each week in our range
        weeksAndOffsets.foreach { case (weeks, lt, ut) =>
          if (weeks.length == 1) {
            weeksAndIndices(weeks.head) ++= getIndices(lt, ut)
          } else {
            val head +: middle :+ last = weeks.toList
            weeksAndIndices(head) ++= getIndices(lt, tmax)
            weeksAndIndices(last) ++= getIndices(tmin, ut)
            if (middle.nonEmpty) {
              val indices = getIndices(tmin, tmax)
              middle.foreach(w => weeksAndIndices(w) ++= indices)
            }
          }
        }

        weeksAndIndices.map { case (w, indices) => indices.distinct.map(histogram.count(w, _)).sum }.sum
    }
  }

  /**
    * Estimates the count for attribute filters (equals, less than, during, etc)
    *
    * @param filter filter
    * @param attribute attribute name to estimate
    * @param loDate bounds on the dates to be queried, if any
    * @param hiDate bounds on the dates to be queried, if any
    * @return count, if available
    */
  private def estimateAttributeCount(filter: Filter,
                                     attribute: String,
                                     loDate: Option[Date],
                                     hiDate: Option[Date]): Option[Long] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    if (attribute == sft.getGeomField) {
      estimateSpatialCount(filter)
    } else if (sft.getDtgField.contains(attribute)) {
      estimateTemporalCount(filter)
    } else {
      // we have an attribute filter
      val extractedBounds = for {
        descriptor <- Option(sft.getDescriptor(attribute))
        binding    =  descriptor.getListType().getOrElse(descriptor.getType.getBinding)
        bounds     <- FilterHelper.extractAttributeBounds(filter, attribute, binding.asInstanceOf[Class[Any]])
      } yield {
        bounds.bounds.map(_.bounds)
      }
      extractedBounds.flatMap { bounds =>
        if (bounds.contains((None, None))) {
          estimateCount(Filter.INCLUDE, loDate, hiDate) // inclusive filter
        } else {
          val (equalsBounds, rangeBounds) = bounds.partition { case (l, r) => l == r }
          val equalsCount = if (equalsBounds.isEmpty) { Some(0L) } else {
            estimateEqualsCount(attribute, equalsBounds.map(_._1.get), loDate, hiDate)
          }
          val rangeCount = if (rangeBounds.isEmpty) { Some(0L) } else {
            estimateRangeCount(attribute, rangeBounds)
          }
          for { e <- equalsCount; r <- rangeCount } yield { e + r }
        }
      }
    }
  }

  /**
    * Estimates counts from spatial predicates. Non-spatial predicates will be ignored.
    *
    * @param filter filter to evaluate
    * @return estimated count, if available
    */
  private def estimateSpatialCount(filter: Filter): Option[Long] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

    for {
      geometry  <- FilterHelper.extractSingleGeometry(filter, sft.getGeomField)
      histogram <- stats.getStats[Histogram[Geometry]](sft, Seq(sft.getGeomField)).headOption
    } yield {
      val (zLo, zHi) = {
        val (xmin, ymin, _, _) = bounds(histogram.min)
        val (_, _, xmax, ymax) = bounds(histogram.max)
        (Z2SFC.index(xmin, ymin).z, Z2SFC.index(xmax, ymax).z)
      }
      def inRange(r: IndexRange) = r.lower < zHi && r.upper > zLo

      val (lx, ly, ux, uy) = bounds(geometry)
      val ranges = Z2SFC.ranges((lx, ux), (ly, uy), ZHistogramPrecision)
      val indices = ranges.filter(inRange).flatMap { range =>
        val loIndex = Some(histogram.directIndex(range.lower)).filter(_ != -1).getOrElse(0)
        val hiIndex = Some(histogram.directIndex(range.upper)).filter(_ != -1).getOrElse(histogram.length - 1)
        loIndex to hiIndex
      }
      indices.distinct.map(histogram.count).sumOrElse(0L)
    }
  }

  /**
    * Estimates counts from temporal predicates. Non-temporal predicates will be ignored.
    *
    * @param filter filter to evaluate
    * @return estimated count, if available
    */
  private def estimateTemporalCount(filter: Filter): Option[Long] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

    for {
      dateField <- sft.getDtgField
      intervals <- Option(FilterHelper.extractIntervals(filter, dateField)).filter(_.nonEmpty)
      histogram <- stats.getStats[Histogram[Date]](sft, Seq(dateField)).headOption
    } yield {
      def inRange(interval: (DateTime, DateTime)) =
        interval._1.getMillis <= histogram.max.getTime && interval._2.getMillis >= histogram.min.getTime

      val indices = intervals.filter(inRange).flatMap { interval =>
        val loIndex = Some(histogram.indexOf(interval._1.toDate)).filter(_ != -1).getOrElse(0)
        val hiIndex = Some(histogram.indexOf(interval._2.toDate)).filter(_ != -1).getOrElse(histogram.length - 1)
        loIndex to hiIndex
      }
      indices.distinct.map(histogram.count).sumOrElse(0L)
    }
  }

  /**
    * Estimates an equals predicate. Uses frequency (count min sketch) for estimated value.
    * Frequency estimates will never return less than the actual number, but will often return more.
    *
    * Note: in our current stats, frequency has ~0.5% error rate based on the total number of features in the data set.
    * The error will be multiplied by the number of values you are evaluating, which can lead to large error rates.
    *
    * @param attribute attribute to evaluate
    * @param values values to be estimated
    * @param loDate bounds on the dates to be queried, if any
    * @param hiDate bounds on the dates to be queried, if any
    * @return estimated count, if available.
    */
  private def estimateEqualsCount(attribute: String,
                                  values: Seq[Any],
                                  loDate: Option[Date],
                                  hiDate: Option[Date]): Option[Long] = {
    val weeks = for { dtg <- sft.getDtgField; d1  <- loDate; d2  <- hiDate } yield {
      Range.inclusive(Frequency.getWeek(d1), Frequency.getWeek(d2)).map(_.toShort)
    }
    val options = weeks.getOrElse(Seq.empty)
    stats.getStats[Frequency[Any]](sft, Seq(attribute), options).headOption.map(f => values.map(f.count).sum)
  }

  /**
    * Estimates a potentially unbounded range predicate. Uses a binned histogram for estimated value.
    *
    * @param attribute attribute to evaluate
    * @param ranges ranges of values - may be unbounded (indicated by a None)
    * @return estimated count, if available
    */
  private def estimateRangeCount(attribute: String, ranges: Seq[(Option[Any], Option[Any])]): Option[Long] = {
    stats.getStats[Histogram[Any]](sft, Seq(attribute)).headOption.map { histogram =>
      val inRangeRanges = ranges.filter {
        case (None, None)         => true // inclusive filter
        case (Some(lo), None)     => histogram.defaults.min(lo, histogram.max) == lo
        case (None, Some(up))     => histogram.defaults.max(up, histogram.min) == up
        case (Some(lo), Some(up)) =>
          histogram.defaults.min(lo, histogram.max) == lo && histogram.defaults.max(up, histogram.min) == up
      }
      val indices = inRangeRanges.flatMap { case (lower, upper) =>
        val lowerIndex = lower.map(histogram.indexOf).filter(_ != -1).getOrElse(0)
        val upperIndex = upper.map(histogram.indexOf).filter(_ != -1).getOrElse(histogram.length - 1)
        lowerIndex to upperIndex
      }
      indices.distinct.map(histogram.count).sumOrElse(0L)
    }
  }
}

object CountEstimator {

  // we only need enough precision to cover the number of bins (e.g. 2^n == bins), plus 2 for unused bits
  val ZHistogramPrecision = math.ceil(math.log(GeoMesaStats.MaxHistogramSize) / math.log(2)).toInt + 2

  private [stats] def bounds(geometry: Geometry): (Double, Double, Double, Double) = {
    val env = geometry.getEnvelopeInternal
    (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
  }

  /**
    * Extracts date bounds from a filter.
    *
    * @param sft simple feature type
    * @param filter filter
    * @return date bounds, if any
    */
  private [stats] def extractDates(sft: SimpleFeatureType, filter: Filter): (Option[Date], Option[Date]) = {
    val intervals = sft.getDtgField.toSeq.flatMap(FilterHelper.extractIntervals(filter, _))
    // don't consider gaps, just get the endpoints of the intervals
    val dateTimes = intervals.reduceOption[(DateTime, DateTime)] { case (left, right) =>
      val lower = if (left._1.isAfter(right._1)) right._1 else left._1
      val upper = if (left._2.isBefore(right._2)) right._2 else left._2
      (lower, upper)
    }
    // filter out ubounded endpoints
    val dateOptions = dateTimes.map { case (lower, upper) =>
      val lowerOption = if (lower == FilterHelper.MinDateTime) None else Some(lower.toDate)
      val upperOption = if (upper == FilterHelper.MaxDateTime) None else Some(upper.toDate)
      (lowerOption, upperOption)
    }
    dateOptions.getOrElse((None, None))
  }
}
