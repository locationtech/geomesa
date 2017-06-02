/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.stats

import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.joda.time.DateTime
import org.locationtech.geomesa.curve.{BinnedTime, Z2SFC, Z3SFC}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.stats._
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.collection.JavaConversions._

trait StatsBasedEstimator {

  stats: GeoMesaStats =>

  import CountEstimator.ZHistogramPrecision
  import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

  /**
    * Estimates the count for a given filter, based off the per-attribute metadata we have stored
    *
    * @param sft simple feature type
    * @param filter filter to apply - should have been run through QueryPlanFilterVisitor so all props are right
    * @return estimated count, if available
    */
  protected def estimateCount(sft: SimpleFeatureType, filter: Filter): Option[Long] = {
    // TODO currently we don't consider if the dates are actually ANDed with everything else
    CountEstimator.extractDates(sft, filter) match {
      case None => Some(0L) // disjoint dates
      case Some(Bounds(lo, hi, _)) => estimateCount(sft, filter, lo, hi)
    }
  }

  /**
    * Estimates the count for a given filter, based off the per-attribute metadata we have stored
    *
    * @param sft simple feature type
    * @param filter filter to apply - should have been run through QueryPlanFilterVisitor so all props are right
    * @param loDate bounds on the dates to be queried, if any
    * @param hiDate bounds on the dates to be queried, if any
    * @return estimated count, if available
    */
  private def estimateCount(sft: SimpleFeatureType,
                            filter: Filter,
                            loDate: Option[Date],
                            hiDate: Option[Date]): Option[Long] = {
    import Filter.{EXCLUDE, INCLUDE}

    filter match {
      case EXCLUDE => Some(0L)
      case INCLUDE => stats.getStats[CountStat](sft).headOption.map(_.count)

      case a: And  => estimateAndCount(sft, a, loDate, hiDate)
      case o: Or   => estimateOrCount(sft, o, loDate, hiDate)
      case n: Not  => estimateNotCount(sft, n, loDate, hiDate)

      case i: Id   => Some(i.getIdentifiers.size)
      case _       =>
        // single filter - equals, between, less than, etc
        val attribute = FilterHelper.propertyNames(filter, sft).headOption
        attribute.flatMap(estimateAttributeCount(sft, filter, _, loDate, hiDate))
    }
  }

  /**
    * Estimate counts for AND filters. Since it's an AND, we calculate the child counts and
    * return the minimum.
    *
    * We check for spatio-temporal filters first, as those are the only ones that operate on 2+ properties.
    *
    * @param sft simple feature type
    * @param filter AND filter
    * @param loDate bounds on the dates to be queried, if any
    * @param hiDate bounds on the dates to be queried, if any
    * @return estimated count, if available
    */
  private def estimateAndCount(sft: SimpleFeatureType,
                               filter: And,
                               loDate: Option[Date],
                               hiDate: Option[Date]): Option[Long] = {
    val stCount = estimateSpatioTemporalCount(sft, filter)
    // note: we might over count if we get bbox1 AND bbox2, as we don't intersect them
    val individualCounts = filter.getChildren.flatMap(estimateCount(sft, _, loDate, hiDate))
    (stCount ++ individualCounts).minOption
  }

  /**
    * Estimate counts for OR filters. Because this is an OR, we sum up the child counts
    *
    * @param sft simple feature type
    * @param filter OR filter
    * @param loDate bounds on the dates to be queried, if any
    * @param hiDate bounds on the dates to be queried, if any
    * @return estimated count, if available
    */
  private def estimateOrCount(sft: SimpleFeatureType,
                              filter: Or,
                              loDate: Option[Date],
                              hiDate: Option[Date]): Option[Long] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

    // estimate for each child separately and sum
    // note that we might double count some values if the filter is complex
    filter.getChildren.flatMap(estimateCount(sft, _, loDate, hiDate)).sumOption
  }

  /**
    * Estimates the count for NOT filters
    *
    * @param sft simple feature type
    * @param filter filter
    * @param loDate bounds on the dates to be queried, if any
    * @param hiDate bounds on the dates to be queried, if any
    * @return count, if available
    */
  private def estimateNotCount(sft: SimpleFeatureType,
                               filter: Not,
                               loDate: Option[Date],
                               hiDate: Option[Date]): Option[Long] = {
    for {
      all <- estimateCount(sft, Filter.INCLUDE, None, None)
      neg <- estimateCount(sft, filter.getFilter, loDate, hiDate)
    } yield {
      math.max(0, all - neg)
    }
  }

  /**
    * Estimate spatio-temporal counts for an AND filter.
    *
    * @param sft simple feature type
    * @param filter complex filter
    * @return count, if available
    */
  private def estimateSpatioTemporalCount(sft: SimpleFeatureType, filter: And): Option[Long] = {
    // currently we don't consider if the spatial predicate is actually AND'd with the temporal predicate...
    // TODO add filterhelper method that accurately pulls out the st values
    for {
      geomField  <- Option(sft.getGeomField)
      dateField  <- sft.getDtgField
      geometries =  FilterHelper.extractGeometries(filter, geomField, sft.isPoints)
      if geometries.nonEmpty
      intervals  =  FilterHelper.extractIntervals(filter, dateField)
      if intervals.nonEmpty
      bounds     <- stats.getStats[MinMax[Date]](sft, Seq(dateField)).headOption
    } yield {
      lazy val inRangeIntervals = {
        val minTime = bounds.min.getTime
        val maxTime = bounds.max.getTime
        intervals.values.filter(i => i._1.getMillis <= maxTime && i._2.getMillis >= minTime)
      }
      if (geometries.disjoint || intervals.disjoint) { 0L } else {
        estimateSpatioTemporalCount(sft, geomField, dateField, geometries.values, inRangeIntervals)
      }
    }
  }

  /**
    * Estimates counts based on a combination of spatial and temporal values.
    *
    * @param sft simple feature type
    * @param geomField geometry attribute name for the simple feature type
    * @param dateField date attribute name for the simple feature type
    * @param geometries geometry to evaluate
    * @param intervals intervals to evaluate
    * @return
    */
  private def estimateSpatioTemporalCount(sft: SimpleFeatureType,
                                          geomField: String,
                                          dateField: String,
                                          geometries: Seq[Geometry],
                                          intervals: Seq[(DateTime, DateTime)]): Long = {
    val dateToBins = BinnedTime.dateToBinnedTime(sft.getZ3Interval)
    val binnedTimes = intervals.map { interval =>
      val BinnedTime(lb, lt) = dateToBins(interval._1)
      val BinnedTime(ub, ut) = dateToBins(interval._2)
      (Range.inclusive(lb, ub).map(_.toShort), lt, ut)
    }
    val allBins = binnedTimes.flatMap(_._1).distinct

    stats.getStats[Z3Histogram](sft, Seq(geomField, dateField), allBins).headOption match {
      case None => 0L
      case Some(histogram) =>
        // time range for a chunk is 0 to 1 week (in seconds)
        val sfc = Z3SFC(sft.getZ3Interval)
        val (tmin, tmax) = (sfc.time.min.toLong, sfc.time.max.toLong)
        val xy = geometries.map(GeometryUtils.bounds)

        def getIndices(t1: Long, t2: Long): Seq[Int] = {
          val w = histogram.timeBins.head // z3 histogram bounds are fixed, so indices should be the same
          val zs = sfc.ranges(xy, Seq((t1, t2)), ZHistogramPrecision)
          zs.flatMap(r => histogram.directIndex(w, r.lower) to histogram.directIndex(w, r.upper))
        }
        lazy val middleIndices = getIndices(tmin, tmax)

        // build up our indices by week so that we can deduplicate them afterwards
        val timeBinsAndIndices = scala.collection.mutable.Map.empty[Short, Seq[Int]].withDefaultValue(Seq.empty)

        // the z3 index breaks time into 1 week chunks, so create a range for each week in our range
        binnedTimes.foreach { case (bins, lt, ut) =>
          if (bins.length == 1) {
            timeBinsAndIndices(bins.head) ++= getIndices(lt, ut)
          } else {
            val head +: middle :+ last = bins.toList
            timeBinsAndIndices(head) ++= getIndices(lt, tmax)
            timeBinsAndIndices(last) ++= getIndices(tmin, ut)
            middle.foreach(m => timeBinsAndIndices(m) ++= middleIndices)
          }
        }

        timeBinsAndIndices.map { case (b, indices) => indices.distinct.map(histogram.count(b, _)).sum }.sum
    }
  }

  /**
    * Estimates the count for attribute filters (equals, less than, during, etc)
    *
    * @param sft simple feature type
    * @param filter filter
    * @param attribute attribute name to estimate
    * @param loDate bounds on the dates to be queried, if any
    * @param hiDate bounds on the dates to be queried, if any
    * @return count, if available
    */
  private def estimateAttributeCount(sft: SimpleFeatureType,
                                     filter: Filter,
                                     attribute: String,
                                     loDate: Option[Date],
                                     hiDate: Option[Date]): Option[Long] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    // noinspection ExistsEquals
    if (attribute == sft.getGeomField) {
      estimateSpatialCount(sft, filter)
    } else if (sft.getDtgField.exists(_ == attribute)) {
      estimateTemporalCount(sft, filter)
    } else {
      // we have an attribute filter
      val extractedBounds = for {
        descriptor <- Option(sft.getDescriptor(attribute))
        binding    =  if (descriptor.isList) { descriptor.getListType() } else { descriptor.getType.getBinding }
      } yield {
        FilterHelper.extractAttributeBounds(filter, attribute, binding.asInstanceOf[Class[Any]])
      }
      extractedBounds.flatMap { bounds =>
        if (bounds.disjoint) {
          Some(0L) // disjoint range
        } else if (bounds.values.exists(_.bounds == (None, None))) {
          estimateCount(sft, Filter.INCLUDE, loDate, hiDate) // inclusive filter
        } else {
          val (equalsBounds, rangeBounds) = bounds.values.map(_.bounds).partition { case (l, r) => l == r }
          val equalsCount = if (equalsBounds.isEmpty) { Some(0L) } else {
            estimateEqualsCount(sft, attribute, equalsBounds.map(_._1.get), loDate, hiDate)
          }
          val rangeCount = if (rangeBounds.isEmpty) { Some(0L) } else {
            estimateRangeCount(sft, attribute, rangeBounds)
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
  private def estimateSpatialCount(sft: SimpleFeatureType, filter: Filter): Option[Long] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

    val geometries = FilterHelper.extractGeometries(filter, sft.getGeomField, sft.isPoints)
    if (geometries.isEmpty) {
      None
    } else if (geometries.disjoint) {
      Some(0L)
    } else {
      stats.getStats[Histogram[Geometry]](sft, Seq(sft.getGeomField)).headOption.map { histogram =>
        val (zLo, zHi) = {
          val (xmin, ymin, _, _) = GeometryUtils.bounds(histogram.min)
          val (_, _, xmax, ymax) = GeometryUtils.bounds(histogram.max)
          (Z2SFC.index(xmin, ymin).z, Z2SFC.index(xmax, ymax).z)
        }
        def inRange(r: IndexRange) = r.lower < zHi && r.upper > zLo

        val ranges = Z2SFC.ranges(geometries.values.map(GeometryUtils.bounds), ZHistogramPrecision)
        val indices = ranges.filter(inRange).flatMap { range =>
          val loIndex = Some(histogram.directIndex(range.lower)).filter(_ != -1).getOrElse(0)
          val hiIndex = Some(histogram.directIndex(range.upper)).filter(_ != -1).getOrElse(histogram.length - 1)
          loIndex to hiIndex
        }
        indices.distinct.map(histogram.count).sumOrElse(0L)
      }
    }
  }

  /**
    * Estimates counts from temporal predicates. Non-temporal predicates will be ignored.
    *
    * @param sft simple feature type
    * @param filter filter to evaluate
    * @return estimated count, if available
    */
  private def estimateTemporalCount(sft: SimpleFeatureType, filter: Filter): Option[Long] = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce

    for {
      dateField <- sft.getDtgField
      intervals =  FilterHelper.extractIntervals(filter, dateField)
      if intervals.nonEmpty
      histogram <- stats.getStats[Histogram[Date]](sft, Seq(dateField)).headOption
    } yield {
      def inRange(interval: (DateTime, DateTime)) =
        interval._1.getMillis <= histogram.max.getTime && interval._2.getMillis >= histogram.min.getTime

      if (intervals.disjoint) { 0L } else {
        val indices = intervals.values.filter(inRange).flatMap { interval =>
          val loIndex = Some(histogram.indexOf(interval._1.toDate)).filter(_ != -1).getOrElse(0)
          val hiIndex = Some(histogram.indexOf(interval._2.toDate)).filter(_ != -1).getOrElse(histogram.length - 1)
          loIndex to hiIndex
        }
        indices.distinct.map(histogram.count).sumOrElse(0L)
      }
    }
  }

  /**
    * Estimates an equals predicate. Uses frequency (count min sketch) for estimated value.
    * Frequency estimates will never return less than the actual number, but will often return more.
    *
    * Note: in our current stats, frequency has ~0.5% error rate based on the total number of features in the data set.
    * The error will be multiplied by the number of values you are evaluating, which can lead to large error rates.
    *
    * @param sft simple feature type
    * @param attribute attribute to evaluate
    * @param values values to be estimated
    * @param loDate bounds on the dates to be queried, if any
    * @param hiDate bounds on the dates to be queried, if any
    * @return estimated count, if available.
    */
  private def estimateEqualsCount(sft: SimpleFeatureType,
                                  attribute: String,
                                  values: Seq[Any],
                                  loDate: Option[Date],
                                  hiDate: Option[Date]): Option[Long] = {
    val timeBins = for { d1  <- loDate; d2  <- hiDate } yield {
      val timeToBin = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
      Range.inclusive(timeToBin(d1.getTime).bin, timeToBin(d2.getTime).bin).map(_.toShort)
    }
    val options = timeBins.getOrElse(Seq.empty)
    stats.getStats[Frequency[Any]](sft, Seq(attribute), options).headOption.map(f => values.map(f.count).sum)
  }

  /**
    * Estimates a potentially unbounded range predicate. Uses a binned histogram for estimated value.
    *
    * @param sft simple feature type
    * @param attribute attribute to evaluate
    * @param ranges ranges of values - may be unbounded (indicated by a None)
    * @return estimated count, if available
    */
  private def estimateRangeCount(sft: SimpleFeatureType,
                                 attribute: String,
                                 ranges: Seq[(Option[Any],
                                 Option[Any])]): Option[Long] = {
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

  /**
    * Extracts date bounds from a filter. None is used to indicate a disjoint date range, otherwise
    * there will be a bounds object (which may be unbounded).
    *
    * @param sft simple feature type
    * @param filter filter
    * @return None, if disjoint filters, otherwise date bounds (which may be unbounded)
    */
  private [stats] def extractDates(sft: SimpleFeatureType, filter: Filter): Option[Bounds[Date]] = {
    sft.getDtgField match {
      case None => Some(Bounds(None, None, inclusive = false))
      case Some(dtg) =>
        val intervals = FilterHelper.extractIntervals(filter, dtg)
        if (intervals.disjoint) { None } else {
          // don't consider gaps, just get the endpoints of the intervals
          val dateTimes = intervals.values.reduceOption[(DateTime, DateTime)] { case (left, right) =>
            val lower = if (left._1.isAfter(right._1)) right._1 else left._1
            val upper = if (left._2.isBefore(right._2)) right._2 else left._2
            (lower, upper)
          }
          // filter out unbounded endpoints
          val (lowerOption, upperOption) = dateTimes.map { case (lower, upper) =>
            val lowerOption = if (lower == FilterHelper.MinDateTime) None else Some(lower.toDate)
            val upperOption = if (upper == FilterHelper.MaxDateTime) None else Some(upper.toDate)
            (lowerOption, upperOption)
          }.getOrElse((None, None))
          Some(Bounds(lowerOption, upperOption, inclusive = false))
        }
    }
  }
}
