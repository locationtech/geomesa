/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.locationtech.geomesa.curve.BinnedTime.TimeToBinnedTime
import org.locationtech.geomesa.curve.{BinnedTime, XZ3SFC}
import org.locationtech.geomesa.filter.FilterValues
import org.locationtech.geomesa.index.api.IndexKeySpace.IndexKeySpaceFactory
import org.locationtech.geomesa.index.api.ShardStrategy.{NoShardStrategy, ZShardStrategy}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.jts.geom.{Geometry, Point}
import org.locationtech.sfcurve.IndexRange
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.util.control.NonFatal

class XZ3IndexKeySpace(val sft: SimpleFeatureType, val sharding: ShardStrategy, geomField: String, dtgField: String)
    extends IndexKeySpace[XZ3IndexValues, Z3IndexKey] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  require(classOf[Geometry].isAssignableFrom(sft.getDescriptor(geomField).getType.getBinding),
    s"Expected field $geomField to have a geometry binding, but instead it has: " +
        sft.getDescriptor(geomField).getType.getBinding.getSimpleName)
  require(classOf[Date].isAssignableFrom(sft.getDescriptor(dtgField).getType.getBinding),
    s"Expected field $dtgField to have a date binding, but instead it has: " +
        sft.getDescriptor(dtgField).getType.getBinding.getSimpleName)

  protected val geomIndex: Int = sft.indexOf(geomField)
  protected val dtgIndex: Int = sft.indexOf(dtgField)

  protected val sfc = XZ3SFC(sft.getXZPrecision, sft.getZ3Interval)
  protected val timeToIndex: TimeToBinnedTime = BinnedTime.timeToBinnedTime(sft.getZ3Interval)

  private val dateToIndex = BinnedTime.dateToBinnedTime(sft.getZ3Interval)
  private val boundsToDates = BinnedTime.boundsToIndexableDates(sft.getZ3Interval)
  private val isPoints = classOf[Point].isAssignableFrom(sft.getDescriptor(geomIndex).getType.getBinding)

  override val attributes: Seq[String] = Seq(geomField, dtgField)

  override val indexKeyByteLength: Right[(Array[Byte], Int, Int) => Int, Int] = Right(10 + sharding.length)

  override val sharing: Array[Byte] = Array.empty

  override def toIndexKey(writable: WritableFeature,
                          tier: Array[Byte],
                          id: Array[Byte],
                          lenient: Boolean): RowKeyValue[Z3IndexKey] = {
    val geom = writable.getAttribute[Geometry](geomIndex)
    if (geom == null) {
      throw new IllegalArgumentException(s"Null geometry in feature ${writable.feature.getID}")
    }
    val envelope = geom.getEnvelopeInternal
    // TODO support date intervals (remember to remove disjoint data check in getRanges)
    val dtg = writable.getAttribute[Date](dtgIndex)
    val time = if (dtg == null) { 0L } else { dtg.getTime }
    val BinnedTime(b, t) = timeToIndex(time)
    val xz = try {
      sfc.index(envelope.getMinX, envelope.getMinY, t, envelope.getMaxX, envelope.getMaxY, t, lenient)
    } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid xz value from geometry/time: $geom,$dtg", e)
    }
    val shard = sharding(writable)

    // create the byte array - allocate a single array up front to contain everything
    // ignore tier, not used here
    val bytes = Array.ofDim[Byte](shard.length + 10 + id.length)

    if (shard.isEmpty) {
      ByteArrays.writeShort(b, bytes, 0)
      ByteArrays.writeLong(xz, bytes, 2)
      System.arraycopy(id, 0, bytes, 10, id.length)
    } else {
      bytes(0) = shard.head // shard is only a single byte
      ByteArrays.writeShort(b, bytes, 1)
      ByteArrays.writeLong(xz, bytes, 3)
      System.arraycopy(id, 0, bytes, 11, id.length)
    }

    SingleRowKeyValue(bytes, sharing, shard, Z3IndexKey(b, xz), tier, id, writable.values)
  }

  override def getIndexValues(filter: Filter, explain: Explainer): XZ3IndexValues = {
    import org.locationtech.geomesa.filter.FilterHelper._

    // standardize the two key query arguments:  polygon and date-range

    val geometries: FilterValues[Geometry] = {
      val extracted = extractGeometries(filter, geomField, isPoints)
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
      return XZ3IndexValues(sfc, FilterValues.empty, Seq.empty, FilterValues.empty, Map.empty, Seq.empty)
    }

    // compute our ranges based on the coarse bounds for our query
    val xy: Seq[(Double, Double, Double, Double)] = {
      val multiplier = QueryProperties.PolygonDecompMultiplier.toInt.get
      val bits = QueryProperties.PolygonDecompBits.toInt.get
      geometries.values.flatMap(GeometryUtils.bounds(_, multiplier, bits))
    }

    // calculate map of weeks to time intervals in that week
    val timesByBin = scala.collection.mutable.Map.empty[Short, (Double, Double)]
    val unboundedBins = Seq.newBuilder[(Short, Short)]

    def updateTime(bin: Short, lt: Double, ut: Double): Unit = {
      val times = timesByBin.get(bin) match {
        case None => (lt, ut)
        case Some((min, max)) => (math.min(min, lt), math.max(max, ut))
      }
      timesByBin(bin) = times
    }

    // note: intervals shouldn't have any overlaps
    intervals.foreach { interval =>
      val (lower, upper) = boundsToDates(interval.bounds)
      val BinnedTime(lb, lt) = dateToIndex(lower)
      val BinnedTime(ub, ut) = dateToIndex(upper)

      if (interval.isBoundedBothSides) {
        if (lb == ub) {
          updateTime(lb, lt, ut)
        } else {
          updateTime(lb, lt, sfc.zBounds._2)
          updateTime(ub, sfc.zBounds._1, ut)
          Range.inclusive(lb + 1, ub - 1).foreach(b => timesByBin(b.toShort) = sfc.zBounds)
        }
      } else if (interval.lower.value.isDefined) {
        updateTime(lb, lt, sfc.zBounds._2)
        unboundedBins += (((lb + 1).toShort, Short.MaxValue))
      } else if (interval.upper.value.isDefined) {
        updateTime(ub, sfc.zBounds._1, ut)
        unboundedBins += ((0, (ub - 1).toShort))
      }
    }

    // make our underlying index values available to other classes in the pipeline for processing
    XZ3IndexValues(sfc, geometries, xy, intervals, timesByBin.toMap, unboundedBins.result())
  }

  override def getRanges(values: XZ3IndexValues, multiplier: Int): Iterator[ScanRange[Z3IndexKey]] = {
    val XZ3IndexValues(sfc, _, xy, _, timesByBin, unboundedBins) = values

    // note: `target` will always be Some, as ScanRangesTarget has a default value
    val target = QueryProperties.ScanRangesTarget.option.map { t =>
      math.max(1, if (timesByBin.isEmpty) { t.toInt } else { t.toInt / timesByBin.size } / multiplier)
    }

    def toZRanges(t: (Double, Double)): Seq[IndexRange] =
      sfc.ranges(xy.map { case (xmin, ymin, xmax, ymax) => (xmin, ymin, t._1, xmax, ymax, t._2) }, target)

    lazy val wholePeriodRanges = toZRanges(sfc.zBounds)

    val bounded = timesByBin.iterator.flatMap { case (bin, times) =>
      val zs = if (times.eq(sfc.zBounds)) { wholePeriodRanges } else { toZRanges(times) }
      zs.map(r => BoundedRange(Z3IndexKey(bin, r.lower), Z3IndexKey(bin, r.upper)))
    }

    val unbounded = unboundedBins.iterator.map {
      case (lower, Short.MaxValue) => LowerBoundedRange(Z3IndexKey(lower, 0L))
      case (0, upper)              => UpperBoundedRange(Z3IndexKey(upper, Long.MaxValue))
      case (lower, upper) =>
        logger.error(s"Unexpected unbounded bin endpoints: $lower:$upper")
        UnboundedRange(Z3IndexKey(0, 0L))
    }

    bounded ++ unbounded
  }

  override def getRangeBytes(ranges: Iterator[ScanRange[Z3IndexKey]], tier: Boolean): Iterator[ByteRange] = {
    if (sharding.length == 0) {
      ranges.map {
        case BoundedRange(lo, hi) =>
          BoundedByteRange(ByteArrays.toBytes(lo.bin, lo.z), ByteArrays.toBytesFollowingPrefix(hi.bin, hi.z))

        case LowerBoundedRange(lo) =>
          BoundedByteRange(ByteArrays.toBytes(lo.bin, lo.z), ByteRange.UnboundedUpperRange)

        case UpperBoundedRange(hi) =>
          BoundedByteRange(ByteRange.UnboundedLowerRange, ByteArrays.toBytesFollowingPrefix(hi.bin, hi.z))

        case UnboundedRange(_) =>
          BoundedByteRange(ByteRange.UnboundedLowerRange, ByteRange.UnboundedUpperRange)

        case r =>
          throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    } else {
      ranges.flatMap {
        case BoundedRange(lo, hi) =>
          val lower = ByteArrays.toBytes(lo.bin, lo.z)
          val upper = ByteArrays.toBytesFollowingPrefix(hi.bin, hi.z)
          sharding.shards.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

        case LowerBoundedRange(lo) =>
          val lower = ByteArrays.toBytes(lo.bin, lo.z)
          val upper = ByteRange.UnboundedUpperRange
          sharding.shards.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

        case UpperBoundedRange(hi) =>
          val lower = ByteRange.UnboundedLowerRange
          val upper = ByteArrays.toBytesFollowingPrefix(hi.bin, hi.z)
          sharding.shards.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

        case UnboundedRange(_) =>
          Seq(BoundedByteRange(ByteRange.UnboundedLowerRange, ByteRange.UnboundedUpperRange))

        case r =>
          throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    }
  }

  // always apply the full filter to xz queries
  override def useFullFilter(values: Option[XZ3IndexValues],
                             config: Option[GeoMesaDataStoreConfig],
                             hints: Hints): Boolean = true
}

object XZ3IndexKeySpace extends IndexKeySpaceFactory[XZ3IndexValues, Z3IndexKey] {

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    attributes.lengthCompare(2) == 0 && attributes.forall(sft.indexOf(_) != -1) &&
        classOf[Geometry].isAssignableFrom(sft.getDescriptor(attributes.head).getType.getBinding) &&
        classOf[Date].isAssignableFrom(sft.getDescriptor(attributes.last).getType.getBinding)

  override def apply(sft: SimpleFeatureType, attributes: Seq[String], tier: Boolean): XZ3IndexKeySpace = {
    val shards = if (tier) { NoShardStrategy } else { ZShardStrategy(sft) }
    new XZ3IndexKeySpace(sft, shards, attributes.head, attributes.last)
  }
}
