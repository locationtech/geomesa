/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3

import java.util.Date

import com.google.common.primitives.Shorts
import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.curve.{BinnedTime, XZ3SFC}
import org.locationtech.geomesa.filter.FilterValues
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.utils.{ByteArrays, Explainer}
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

object XZ3IndexKeySpace extends XZ3IndexKeySpace

trait XZ3IndexKeySpace extends IndexKeySpace[XZ3IndexValues] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  override val indexKeyLength: Int = 10

  override def supports(sft: SimpleFeatureType): Boolean = sft.getDtgField.isDefined && sft.nonPoints

  override def toIndexKey(sft: SimpleFeatureType, lenient: Boolean): (SimpleFeature) => Array[Byte] = {
    val sfc = XZ3SFC(sft.getXZPrecision, sft.getZ3Interval)
    val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("XZ3 index requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)

    (feature) => {
      val geom = feature.getDefaultGeometry.asInstanceOf[Geometry]
      if (geom == null) {
        throw new IllegalArgumentException(s"Null geometry in feature ${feature.getID}")
      }
      val envelope = geom.getEnvelopeInternal
      // TODO support date intervals (remember to remove disjoint data check in getRanges)
      val dtg = feature.getAttribute(dtgIndex).asInstanceOf[Date]
      val time = if (dtg == null) { 0L } else { dtg.getTime }
      val BinnedTime(b, t) = timeToIndex(time)
      val xz = try {
        sfc.index(envelope.getMinX, envelope.getMinY, t, envelope.getMaxX, envelope.getMaxY, t, lenient)
      } catch {
        case NonFatal(e) => throw new IllegalArgumentException(s"Invalid xz value from geometry/time: $geom,$dtg", e)
      }
      ByteArrays.toBytes(b, xz)
    }
  }

  override def getIndexValues(sft: SimpleFeatureType, filter: Filter, explain: Explainer): XZ3IndexValues = {
    import org.locationtech.geomesa.filter.FilterHelper._

    // note: z3 requires a date field
    val dtgField = sft.getDtgField.getOrElse {
      throw new RuntimeException("Trying to execute an xz3 query but the schema does not have a date")
    }

    val sfc = XZ3SFC(sft.getXZPrecision, sft.getZ3Interval)

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

    // disjoint geometries are ok since they could still intersect a polygon
    if (intervals.disjoint) {
      explain("Disjoint dates extracted, short-circuiting to empty query")
      return XZ3IndexValues(sfc, FilterValues.empty, Seq.empty, FilterValues.empty, Map.empty)
    }

    // compute our ranges based on the coarse bounds for our query

    val xy = geometries.values.map(GeometryUtils.bounds)

    // calculate map of weeks to time intervals in that week
    val timesByBin = scala.collection.mutable.Map.empty[Short, (Double, Double)]
    val dateToIndex = BinnedTime.dateToBinnedTime(sft.getZ3Interval)
    val boundsToDates = BinnedTime.boundsToIndexableDates(sft.getZ3Interval)

    def updateTime(week: Short, lt: Double, ut: Double): Unit = {
      val times = timesByBin.get(week) match {
        case None => (lt, ut)
        case Some((min, max)) => (math.min(min, lt), math.max(max, ut))
      }
      timesByBin(week) = times
    }

    // note: intervals shouldn't have any overlaps
    intervals.foreach { interval =>
      val (lower, upper) = boundsToDates(interval.bounds)
      val BinnedTime(lb, lt) = dateToIndex(lower)
      val BinnedTime(ub, ut) = dateToIndex(upper)
      if (lb == ub) {
        updateTime(lb, lt, ut)
      } else {
        updateTime(lb, lt, sfc.zBounds._2)
        updateTime(ub, sfc.zBounds._1, ut)
        Range.inclusive(lb + 1, ub - 1).foreach(b => timesByBin(b.toShort) = sfc.zBounds)
      }
    }

    // make our underlying index values available to other classes in the pipeline for processing
    XZ3IndexValues(sfc, geometries, xy, intervals, timesByBin.toMap)
  }

  override def getRanges(sft: SimpleFeatureType, indexValues: XZ3IndexValues): Iterator[(Array[Byte], Array[Byte])] = {

    val XZ3IndexValues(sfc, _, xy, _, timesByBin) = indexValues

    val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)

    def toZRanges(t: (Double, Double)): Seq[IndexRange] =
      sfc.ranges(xy.map { case (xmin, ymin, xmax, ymax) => (xmin, ymin, t._1, xmax, ymax, t._2) }, rangeTarget)

    lazy val wholePeriodRanges = toZRanges(sfc.zBounds)

    timesByBin.iterator.flatMap { case (bin, times) =>
      val b = Shorts.toByteArray(bin)
      val zs = if (times.eq(sfc.zBounds)) { wholePeriodRanges } else { toZRanges(times) }
      zs.map(r => (ByteArrays.toBytes(b, r.lower), ByteArrays.toBytesFollowingPrefix(b, r.upper)))
    }
  }
}
