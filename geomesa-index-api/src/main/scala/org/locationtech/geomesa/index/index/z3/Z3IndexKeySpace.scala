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
import org.locationtech.geomesa.curve.{BinnedTime, Z3SFC}
import org.locationtech.geomesa.filter.FilterValues
import org.locationtech.geomesa.index.api.IndexKeySpace.IndexKeySpaceFactory
import org.locationtech.geomesa.index.api.ShardStrategy.{NoShardStrategy, ZShardStrategy}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.QueryHints.LOOSE_BBOX
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

class Z3IndexKeySpace(val sft: SimpleFeatureType,
                      val sharding: ShardStrategy,
                      geomField: String,
                      dtgField: String) extends IndexKeySpace[Z3IndexValues, Z3IndexKey] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  require(classOf[Point].isAssignableFrom(sft.getDescriptor(geomField).getType.getBinding),
    s"Expected field $geomField to have a point binding, but instead it has: " +
        sft.getDescriptor(geomField).getType.getBinding.getSimpleName)
  require(classOf[Date].isAssignableFrom(sft.getDescriptor(dtgField).getType.getBinding),
    s"Expected field $dtgField to have a date binding, but instead it has: " +
        sft.getDescriptor(dtgField).getType.getBinding.getSimpleName)

  protected val sfc = Z3SFC(sft.getZ3Interval)

  protected val geomIndex: Int = sft.indexOf(geomField)
  protected val dtgIndex: Int = sft.indexOf(dtgField)

  protected val timeToIndex: TimeToBinnedTime = BinnedTime.timeToBinnedTime(sft.getZ3Interval)

  private val dateToIndex = BinnedTime.dateToBinnedTime(sft.getZ3Interval)
  private val boundsToDates = BinnedTime.boundsToIndexableDates(sft.getZ3Interval)

  override val attributes: Seq[String] = Seq(geomField, dtgField)

  override val indexKeyByteLength: Right[(Array[Byte], Int, Int) => Int, Int] = Right(10 + sharding.length)

  override val sharing: Array[Byte] = Array.empty

  override def toIndexKey(writable: WritableFeature,
                          tier: Array[Byte],
                          id: Array[Byte],
                          lenient: Boolean): RowKeyValue[Z3IndexKey] = {
    val geom = writable.getAttribute[Point](geomIndex)
    if (geom == null) {
      throw new IllegalArgumentException(s"Null geometry in feature ${writable.feature.getID}")
    }
    val dtg = writable.getAttribute[Date](dtgIndex)
    val time = if (dtg == null) { 0 } else { dtg.getTime }
    val BinnedTime(b, t) = timeToIndex(time)
    val z = try { sfc.index(geom.getX, geom.getY, t, lenient).z } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid z value from geometry/time: $geom,$dtg", e)
    }
    val shard = sharding(writable)

    // create the byte array - allocate a single array up front to contain everything
    // ignore tier, not used here
    val bytes = Array.ofDim[Byte](shard.length + 10 + id.length)

    if (shard.isEmpty) {
      ByteArrays.writeShort(b, bytes, 0)
      ByteArrays.writeLong(z, bytes, 2)
      System.arraycopy(id, 0, bytes, 10, id.length)
    } else {
      bytes(0) = shard.head // shard is only a single byte
      ByteArrays.writeShort(b, bytes, 1)
      ByteArrays.writeLong(z, bytes, 3)
      System.arraycopy(id, 0, bytes, 11, id.length)
    }

    SingleRowKeyValue(bytes, sharing, shard, Z3IndexKey(b, z), tier, id, writable.values)
  }

  override def getIndexValues(filter: Filter, explain: Explainer): Z3IndexValues = {

    import org.locationtech.geomesa.filter.FilterHelper._

    // standardize the two key query arguments:  polygon and date-range

    val geometries: FilterValues[Geometry] = {
      val extracted = extractGeometries(filter, geomField, intersect = true) // intersect since we have points
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
      return Z3IndexValues(sfc, geometries, Seq.empty, intervals, Map.empty, Seq.empty)
    }

    // compute our ranges based on the coarse bounds for our query
    val xy: Seq[(Double, Double, Double, Double)] = {
      val multiplier = QueryProperties.PolygonDecompMultiplier.toInt.get
      val bits = QueryProperties.PolygonDecompBits.toInt.get
      geometries.values.flatMap(GeometryUtils.bounds(_, multiplier, bits))
    }

    val minTime = sfc.time.min.toLong
    val maxTime = sfc.time.max.toLong

    // calculate map of weeks to time intervals in that week
    val timesByBin = scala.collection.mutable.Map.empty[Short, Seq[(Long, Long)]].withDefaultValue(Seq.empty)
    val unboundedBins = Seq.newBuilder[(Short, Short)]

    // note: intervals shouldn't have any overlaps
    intervals.foreach { interval =>
      val (lower, upper) = boundsToDates(interval.bounds)
      val BinnedTime(lb, lt) = dateToIndex(lower)
      val BinnedTime(ub, ut) = dateToIndex(upper)

      if (interval.isBoundedBothSides) {
        if (lb == ub) {
          timesByBin(lb) ++= Seq((lt, ut))
        } else {
          timesByBin(lb) ++= Seq((lt, maxTime))
          timesByBin(ub) ++= Seq((minTime, ut))
          Range.inclusive(lb + 1, ub - 1).foreach(b => timesByBin(b.toShort) = sfc.wholePeriod)
        }
      } else if (interval.lower.value.isDefined) {
        timesByBin(lb) ++= Seq((lt, maxTime))
        unboundedBins += (((lb + 1).toShort, Short.MaxValue))
      } else if (interval.upper.value.isDefined) {
        timesByBin(ub) ++= Seq((minTime, ut))
        unboundedBins += ((0, (ub - 1).toShort))
      }
    }

    Z3IndexValues(sfc, geometries, xy, intervals, timesByBin.toMap, unboundedBins.result())
  }

  override def getRanges(values: Z3IndexValues, multiplier: Int): Iterator[ScanRange[Z3IndexKey]] = {
    val Z3IndexValues(z3, _, xy, _, timesByBin, unboundedBins) = values

    // note: `target` will always be Some, as ScanRangesTarget has a default value
    val target = QueryProperties.ScanRangesTarget.option.map { t =>
      math.max(1, if (timesByBin.isEmpty) { t.toInt } else { t.toInt / timesByBin.size } / multiplier)
    }

    def toZRanges(t: Seq[(Long, Long)]): Seq[IndexRange] = z3.ranges(xy, t, 64, target)

    lazy val wholePeriodRanges = toZRanges(z3.wholePeriod)

    val bounded = timesByBin.iterator.flatMap { case (bin, times) =>
      val zs = if (times.eq(z3.wholePeriod)) { wholePeriodRanges } else { toZRanges(times) }
      zs.map(range => BoundedRange(Z3IndexKey(bin, range.lower), Z3IndexKey(bin, range.upper)))
    }

    val unbounded = unboundedBins.iterator.map {
      case (0, Short.MaxValue)     => UnboundedRange(Z3IndexKey(0, 0L))
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

  override def useFullFilter(values: Option[Z3IndexValues],
                             config: Option[GeoMesaDataStoreConfig],
                             hints: Hints): Boolean = {
    // if the user has requested strict bounding boxes, we apply the full filter
    // if we have a complicated geometry predicate, we need to pass it through to be evaluated
    // if we have unbounded dates, we need to pass them through as we don't have z-values for all periods

    // if the spatial predicate is rectangular (e.g. a bbox), the index is fine enough that we
    // don't need to apply the filter on top of it. this may cause some minor errors at extremely
    // fine resolutions, but the performance is worth it
    val looseBBox = Option(hints.get(LOOSE_BBOX)).map(Boolean.unbox).getOrElse(config.forall(_.looseBBox))
    def unboundedDates: Boolean = values.exists(_.temporalUnbounded.nonEmpty)
    def complexGeoms: Boolean = values.exists(_.geometries.values.exists(g => !GeometryUtils.isRectangular(g)))
    !looseBBox || unboundedDates || complexGeoms
  }
}

object Z3IndexKeySpace extends IndexKeySpaceFactory[Z3IndexValues, Z3IndexKey] {

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    attributes.lengthCompare(2) == 0 && attributes.forall(sft.indexOf(_) != -1) &&
        classOf[Point].isAssignableFrom(sft.getDescriptor(attributes.head).getType.getBinding) &&
        classOf[Date].isAssignableFrom(sft.getDescriptor(attributes.last).getType.getBinding)

  override def apply(sft: SimpleFeatureType, attributes: Seq[String], tier: Boolean): Z3IndexKeySpace = {
    val shards = if (tier) { NoShardStrategy } else { ZShardStrategy(sft) }
    new Z3IndexKeySpace(sft, shards, attributes.head, attributes.last)
  }
}
