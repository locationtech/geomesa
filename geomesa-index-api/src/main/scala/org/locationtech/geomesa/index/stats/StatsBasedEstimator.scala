/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.stats

import java.time.ZonedDateTime
import java.util.Date

import org.locationtech.geomesa.curve.{BinnedTime, Z2SFC, Z3SFC}
import org.locationtech.geomesa.filter.Bounds.Bound
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.stats._
import org.locationtech.jts.geom.Geometry
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.PropertyName

import scala.collection.JavaConversions._

trait StatsBasedEstimator {

  stats: GeoMesaStats =>

  import StatsBasedEstimator.{ErrorThresholds, ZHistogramPrecision}
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
    StatsBasedEstimator.extractDates(sft, filter) match {
      case None => Some(0L) // disjoint dates
      case Some(Bounds(lo, hi)) => estimateCount(sft, filter, lo.value, hi.value)
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
    filter.getFilter match {
      case f: PropertyIsNull =>
        // special handling for 'is not null'
        f.getExpression match {
          case p: PropertyName => estimateRangeCount(sft, p.getPropertyName, Seq((None, None)))
          case _ => estimateCount(sft, Filter.INCLUDE, None, None) // not something we can handle...
        }

      case f =>
        for {
          all <- estimateCount(sft, Filter.INCLUDE, None, None)
          neg <- estimateCount(sft, f, loDate, hiDate)
        } yield {
          math.max(0, all - neg)
        }
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
      if (geometries.disjoint || intervals.disjoint) { 0L } else {
        val inRangeIntervals = {
          val minTime = bounds.min.getTime
          val maxTime = bounds.max.getTime
          intervals.values.filter { i =>
            i.lower.value.forall(_.toInstant.toEpochMilli <= maxTime) &&
                i.upper.value.forall(_.toInstant.toEpochMilli >= minTime)
          }
        }
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
                                          intervals: Seq[Bounds[ZonedDateTime]]): Long = {
    val period = sft.getZ3Interval
    val dateToBins = BinnedTime.dateToBinnedTime(period)
    val boundsToDates = BinnedTime.boundsToIndexableDates(period)
    val binnedTimes = intervals.map { interval =>
      val (lower, upper) = boundsToDates(interval.bounds)
      val BinnedTime(lb, lt) = dateToBins(lower)
      val BinnedTime(ub, ut) = dateToBins(upper)
      (Range.inclusive(lb, ub).map(_.toShort), lt, ut)
    }
    val allBins = binnedTimes.flatMap(_._1).distinct

    stats.getStats[Z3Histogram](sft, Seq(geomField, dateField), allBins).headOption match {
      case None => 0L
      case Some(histogram) =>
        // time range for a chunk is 0 to 1 week (in seconds)
        val sfc = Z3SFC(period)
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
        } else if (!bounds.values.exists(_.isBounded)) {
          estimateCount(sft, Filter.INCLUDE, loDate, hiDate) // inclusive filter
        } else {
          val boundsValues = bounds.values.map(b => (b.lower.value, b.upper.value))
          val (equalsBounds, rangeBounds) = boundsValues.partition { case (l, r) => l == r }
          val equalsCount = if (equalsBounds.isEmpty) { Some(0L) } else {
            // compare equals estimate with range estimate and take the smaller
            val equals = estimateEqualsCount(sft, attribute, equalsBounds.map(_._1.get), loDate, hiDate)
            val range  = estimateRangeCount(sft, attribute, equalsBounds)
            (equals, range) match {
              case (Some(e), Some(r)) => Some(math.min(e, r))
              case (None, r) => r
              case (e, None) => e
            }
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
      def inRange(interval: Bounds[ZonedDateTime]) = {
        interval.lower.value.forall(_.toInstant.toEpochMilli <= histogram.max.getTime) &&
            interval.upper.value.forall(_.toInstant.toEpochMilli >= histogram.min.getTime)
      }

      if (intervals.disjoint) { 0L } else {
        val indices = intervals.values.filter(inRange).flatMap { interval =>
          val loIndex = interval.lower.value.map(i => histogram.indexOf(Date.from(i.toInstant))).filter(_ != -1).getOrElse(0)
          val hiIndex = interval.upper.value.map(i => histogram.indexOf(Date.from(i.toInstant))).filter(_ != -1).getOrElse(histogram.length - 1)
          loIndex to hiIndex
        }
        indices.distinct.map(histogram.count).sumOrElse(0L)
      }
    }
  }

  /**
    * Estimates an equals predicate. Uses frequency (count min sketch) for estimated value.
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
    stats.getStats[Frequency[Any]](sft, Seq(attribute), options).headOption.map { freq =>
      // frequency estimates will never return less than the actual number, but will often return more
      // frequency has ~0.5% error rate based on the total number of features in the data set
      // we adjust the raw estimate based on the absolute error rate

      val absoluteError = math.floor(freq.size * freq.eps)
      val counts = if (absoluteError < 1.0) { values.map(freq.count) } else {
        values.map { v =>
          val estimate = freq.count(v)
          if (estimate == 0L) {
            0L
          } else if (estimate > absoluteError) {
            val relativeError = absoluteError / estimate
            estimate - (ErrorThresholds.dropWhile(_ <= relativeError).head * 0.5 * absoluteError).toLong
          } else {
            val relativeError = estimate / absoluteError
            (ErrorThresholds.dropWhile(_ < relativeError).head * 0.5 * estimate).toLong
          }
        }
      }
      counts.sum
    }
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
                                 ranges: Seq[(Option[Any], Option[Any])]): Option[Long] = {
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

object StatsBasedEstimator {

  // we only need enough precision to cover the number of bins (e.g. 2^n == bins), plus 2 for unused bits
  val ZHistogramPrecision: Int = math.ceil(math.log(GeoMesaStats.MaxHistogramSize) / math.log(2)).toInt + 2

  val ErrorThresholds: Seq[Double] = Seq(0.1, 0.3, 0.5, 0.7, 0.9, 1.0)

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
      case None => Some(Bounds.everything)
      case Some(dtg) =>
        val intervals = FilterHelper.extractIntervals(filter, dtg)
        if (intervals.disjoint) { None } else {
          // don't consider gaps, just get the endpoints of the intervals
          val dateTimes = intervals.values.reduceOption[Bounds[ZonedDateTime]] { case (left, right) =>
            val lower = Bounds.smallerLowerBound(left.lower, right.lower)
            val upper = Bounds.largerUpperBound(left.upper, right.upper)
            Bounds(lower, upper)
          }
          val lower = dateTimes.map(d => Bound(d.lower.value.map(i => Date.from(i.toInstant)), d.lower.inclusive))
          val upper = dateTimes.map(d => Bound(d.upper.value.map(i => Date.from(i.toInstant)), d.upper.inclusive))
          Some(Bounds(lower.getOrElse(Bound.unbounded[Date]), upper.getOrElse(Bound.unbounded[Date])))
        }
    }
  }
}
