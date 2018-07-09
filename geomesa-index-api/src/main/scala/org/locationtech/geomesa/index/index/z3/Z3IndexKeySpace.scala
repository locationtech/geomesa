/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3

import java.util.Date

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.geotools.factory.Hints
import org.locationtech.geomesa.curve.BinnedTime.TimeToBinnedTime
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.curve.{BinnedTime, Z3SFC}
import org.locationtech.geomesa.filter.FilterValues
import org.locationtech.geomesa.index.conf.QueryHints.LOOSE_BBOX
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.index.IndexKeySpace
import org.locationtech.geomesa.index.index.IndexKeySpace._
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.control.NonFatal

object Z3IndexKeySpace extends Z3IndexKeySpace {
  override def sfc(period: TimePeriod): Z3SFC = Z3SFC(period)
}

trait Z3IndexKeySpace extends IndexKeySpace[Z3IndexValues, Z3IndexKey] {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  def sfc(period: TimePeriod): Z3SFC

  override val indexKeyByteLength: Int = 10

  override def supports(sft: SimpleFeatureType): Boolean = sft.getDtgField.isDefined && sft.isPoints

  override def toIndexKey(sft: SimpleFeatureType, lenient: Boolean): SimpleFeature => Seq[Z3IndexKey] = {
    val z3 = sfc(sft.getZ3Interval)
    val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("Z3 index requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)

    getZValue(z3, geomIndex, dtgIndex, timeToIndex, lenient)
  }

  override def toIndexKeyBytes(sft: SimpleFeatureType, lenient: Boolean): ToIndexKeyBytes = {
    val z3 = sfc(sft.getZ3Interval)
    val geomIndex = sft.indexOf(sft.getGeometryDescriptor.getLocalName)
    val dtgIndex = sft.getDtgIndex.getOrElse(throw new IllegalStateException("Z3 index requires a valid date"))
    val timeToIndex = BinnedTime.timeToBinnedTime(sft.getZ3Interval)

    getZValueBytes(z3, geomIndex, dtgIndex, timeToIndex, lenient)
  }

  override def getIndexValues(sft: SimpleFeatureType, filter: Filter, explain: Explainer): Z3IndexValues = {

    import org.locationtech.geomesa.filter.FilterHelper._

    val dtgField = sft.getDtgField.getOrElse {
      throw new RuntimeException("Trying to execute a z3 query but the schema does not have a date")
    }

    val z3 = sfc(sft.getZ3Interval)

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
      return Z3IndexValues(z3, geometries, Seq.empty, intervals, Map.empty)
    }

    val minTime = z3.time.min.toLong
    val maxTime = z3.time.max.toLong

    // compute our accumulo ranges based on the coarse bounds for our query
    val xy = geometries.values.map(GeometryUtils.bounds)

    // calculate map of weeks to time intervals in that week
    val timesByBin = scala.collection.mutable.Map.empty[Short, Seq[(Long, Long)]].withDefaultValue(Seq.empty)
    val dateToIndex = BinnedTime.dateToBinnedTime(sft.getZ3Interval)
    val boundsToDates = BinnedTime.boundsToIndexableDates(sft.getZ3Interval)

    // note: intervals shouldn't have any overlaps
    intervals.foreach { interval =>
      if (interval.isBoundedBothSides) {
        val (lower, upper) = boundsToDates(interval.bounds)
        val BinnedTime(lb, lt) = dateToIndex(lower)
        val BinnedTime(ub, ut) = dateToIndex(upper)
        if (lb == ub) {
          timesByBin(lb) ++= Seq((lt, ut))
        } else {
          timesByBin(lb) ++= Seq((lt, maxTime))
          timesByBin(ub) ++= Seq((minTime, ut))
          Range.inclusive(lb + 1, ub - 1).foreach(b => timesByBin(b.toShort) = z3.wholePeriod)
        }
      }
    }

    Z3IndexValues(z3, geometries, xy, intervals, timesByBin.toMap)
  }

  override def getRanges(values: Z3IndexValues): Iterator[ScanRange[Z3IndexKey]] = {
    val Z3IndexValues(z3, _, xy, _, timesByBin) = values

    val rangeTarget = QueryProperties.ScanRangesTarget.option.map(_.toInt)

    def toZRanges(t: Seq[(Long, Long)]): Seq[IndexRange] = z3.ranges(xy, t, 64, rangeTarget)

    lazy val wholePeriodRanges = toZRanges(z3.wholePeriod)

    timesByBin.iterator.flatMap { case (bin, times) =>
      val zs = if (times.eq(z3.wholePeriod)) { wholePeriodRanges } else { toZRanges(times) }
      zs.map(range => BoundedRange(Z3IndexKey(bin, range.lower), Z3IndexKey(bin, range.upper)))
    }
  }

  override def getRangeBytes(ranges: Iterator[ScanRange[Z3IndexKey]],
                             prefixes: Seq[Array[Byte]],
                             tier: Boolean): Iterator[ByteRange] = {
    if (prefixes.isEmpty) {
      ranges.map {
        case BoundedRange(lo, hi) =>
          BoundedByteRange(ByteArrays.toBytes(lo.bin, lo.z), ByteArrays.toBytesFollowingPrefix(hi.bin, hi.z))

        case r =>
          throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    } else {
      ranges.flatMap {
        case BoundedRange(lo, hi) =>
          val lower = ByteArrays.toBytes(lo.bin, lo.z)
          val upper = ByteArrays.toBytesFollowingPrefix(hi.bin, hi.z)
          prefixes.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

        case r =>
          throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    }
  }

  override def useFullFilter(values: Option[Z3IndexValues],
                             config: Option[GeoMesaDataStoreConfig],
                             hints: Hints): Boolean = {
    // if the user has requested strict bounding boxes, we apply the full filter
    // if the spatial predicate is rectangular (e.g. a bbox), the index is fine enough that we
    // don't need to apply the filter on top of it. this may cause some minor errors at extremely
    // fine resolutions, but the performance is worth it
    // if we have a complicated geometry predicate, we need to pass it through to be evaluated
    val looseBBox = Option(hints.get(LOOSE_BBOX)).map(Boolean.unbox).getOrElse(config.forall(_.looseBBox))
    def simpleGeoms = values.toSeq.flatMap(_.geometries.values).forall(GeometryUtils.isRectangular)
    !looseBBox || !simpleGeoms
  }

  private def getZValue(z3: Z3SFC,
                        geomIndex: Int,
                        dtgIndex: Int,
                        timeToIndex: TimeToBinnedTime,
                        lenient: Boolean)
                       (feature: SimpleFeature): Seq[Z3IndexKey] = {
    val geom = feature.getAttribute(geomIndex).asInstanceOf[Point]
    if (geom == null) {
      throw new IllegalArgumentException(s"Null geometry in feature ${feature.getID}")
    }
    val dtg = feature.getAttribute(dtgIndex).asInstanceOf[Date]
    val time = if (dtg == null) { 0 } else { dtg.getTime }
    val BinnedTime(b, t) = timeToIndex(time)
    val z = try { z3.index(geom.getX, geom.getY, t, lenient).z } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid z value from geometry/time: $geom,$dtg", e)
    }
    Seq(Z3IndexKey(b, z))
  }

  // note: duplicated code to avoid having to create an index key instance
  private def getZValueBytes(z3: Z3SFC,
                             geomIndex: Int,
                             dtgIndex: Int,
                             timeToIndex: TimeToBinnedTime,
                             lenient: Boolean)
                            (prefix: Seq[Array[Byte]],
                             feature: SimpleFeature,
                             suffix: Array[Byte]): Seq[Array[Byte]] = {
    val geom = feature.getAttribute(geomIndex).asInstanceOf[Point]
    if (geom == null) {
      throw new IllegalArgumentException(s"Null geometry in feature ${feature.getID}")
    }
    val dtg = feature.getAttribute(dtgIndex).asInstanceOf[Date]
    val time = if (dtg == null) { 0 } else { dtg.getTime }
    val BinnedTime(b, t) = timeToIndex(time)
    val z = try { z3.index(geom.getX, geom.getY, t, lenient).z } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid z value from geometry/time: $geom,$dtg", e)
    }

    // create the byte array - allocate a single array up front to contain everything
    val bytes = Array.ofDim[Byte](prefix.map(_.length).sum + 10 + suffix.length)
    var i = 0
    prefix.foreach { p => System.arraycopy(p, 0, bytes, i, p.length); i += p.length }
    ByteArrays.writeShort(b, bytes, i)
    ByteArrays.writeLong(z, bytes, i + 2)
    System.arraycopy(suffix, 0, bytes, i + 10, suffix.length)
    Seq(bytes)
  }
}
