/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3

import java.util.Date

import com.google.common.primitives.Shorts
import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.curve.{BinnedTime, Z3SFC}
import org.locationtech.geomesa.filter.FilterValues
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.utils.{ByteArrays, Explainer}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

object Z3IndexKeySpace extends Z3IndexKeySpace {
  override def sfc(period: TimePeriod): Z3SFC = Z3SFC(period)
}

trait Z3IndexKeySpace extends IndexKeySpace[Z3ProcessingValues] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def sfc(period: TimePeriod): Z3SFC

  override val indexKeyLength: Int = 10

  override def supports(sft: SimpleFeatureType): Boolean = sft.getDtgField.isDefined && sft.isPoints

  override def toIndexKey(sft: SimpleFeatureType): (SimpleFeature) => Array[Byte] = {
    val z3 = sfc(sft.getZ3Interval)
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)
    val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("Z3 index requires a valid date"))

    (feature) => {
      val geom = feature.getAttribute(geomIndex).asInstanceOf[Point]
      if (geom == null) {
        throw new IllegalArgumentException(s"Null geometry in feature ${feature.getID}")
      }
      val dtg = feature.getAttribute(dtgIndex).asInstanceOf[Date]
      val time = if (dtg == null) { 0 } else { dtg.getTime }
      val BinnedTime(b, t) = timeToIndex(time)
      val z = try { z3.index(geom.getX, geom.getY, t).z } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid z value from geometry/time: $geom,$dtg", e)
      }
      ByteArrays.toBytes(b, z)
    }
  }

  override def getRanges(sft: SimpleFeatureType,
                         filter: Filter,
                         explain: Explainer): Iterator[(Array[Byte], Array[Byte])] = {

    import org.locationtech.geomesa.filter.FilterHelper._

    val dtgField = sft.getDtgField.getOrElse {
      throw new RuntimeException("Trying to execute a z3 query but the schema does not have a date")
    }

    // standardize the two key query arguments:  polygon and date-range

    val geometries: FilterValues[Geometry] = {
      val extracted = extractGeometries(filter, sft.getGeomField, sft.isPoints)
      if (extracted.nonEmpty) { extracted } else { FilterValues(Seq(WholeWorldPolygon)) }
    }

    // since we don't apply a temporal filter, we pass handleExclusiveBounds to
    // make sure we exclude the non-inclusive endpoints of a during filter.
    // note that this isn't completely accurate, as we only index down to the second
    val intervals = extractIntervals(filter, dtgField, handleExclusiveBounds = true)

    explain(s"Geometries: $geometries")
    explain(s"Intervals: $intervals")

    if (geometries.disjoint || intervals.disjoint) {
      explain("Disjoint geometries or dates extracted, short-circuiting to empty query")
      return Iterator.empty
    }

    val z3 = sfc(sft.getZ3Interval)
    val minTime = z3.time.min.toLong
    val maxTime = z3.time.max.toLong
    val wholePeriod = Seq((minTime, maxTime))

    // compute our accumulo ranges based on the coarse bounds for our query
    val xy = geometries.values.map(GeometryUtils.bounds)

    // calculate map of weeks to time intervals in that week
    val timesByBin = scala.collection.mutable.Map.empty[Short, Seq[(Long, Long)]].withDefaultValue(Seq.empty)
    val dateToIndex = BinnedTime.dateToBinnedTime(sft.getZ3Interval)
    val boundsToDates = BinnedTime.boundsToIndexableDates(sft.getZ3Interval)

    // note: intervals shouldn't have any overlaps
    intervals.foreach { interval =>
      val (lower, upper) = boundsToDates(interval.bounds)
      val BinnedTime(lb, lt) = dateToIndex(lower)
      val BinnedTime(ub, ut) = dateToIndex(upper)
      if (lb == ub) {
        timesByBin(lb) ++= Seq((lt, ut))
      } else {
        timesByBin(lb) ++= Seq((lt, maxTime))
        timesByBin(ub) ++= Seq((minTime, ut))
        Range.inclusive(lb + 1, ub - 1).foreach(b => timesByBin(b.toShort) = wholePeriod)
      }
    }

    // make our underlying index values available to other classes in the pipeline for processing
    processingValues.set(Z3ProcessingValues(z3, geometries, xy, intervals, timesByBin.toMap))

    val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)

    def toZRanges(t: Seq[(Long, Long)]): Seq[IndexRange] = z3.ranges(xy, t, 64, rangeTarget)

    lazy val wholePeriodRanges = toZRanges(wholePeriod)

    timesByBin.iterator.flatMap { case (bin, times) =>
      val b = Shorts.toByteArray(bin)
      val zs = if (times.eq(wholePeriod)) { wholePeriodRanges } else { toZRanges(times) }
      zs.map(range => (ByteArrays.toBytes(b, range.lower), ByteArrays.toBytesFollowingPrefix(b, range.upper)))
    }
  }
}
