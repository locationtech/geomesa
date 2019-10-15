/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.s3

import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.curve.BinnedTime.TimeToBinnedTime
import org.locationtech.geomesa.curve.{BinnedTime, S3SFC}
import org.locationtech.geomesa.filter.FilterValues
import org.locationtech.geomesa.index.api
import org.locationtech.geomesa.index.api.IndexKeySpace.IndexKeySpaceFactory
import org.locationtech.geomesa.index.api.ShardStrategy.{NoShardStrategy, ZShardStrategy}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.QueryHints.LOOSE_BBOX
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.jts.geom.{Geometry, Point}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

/**
  * @author sunyabo 2019年08月01日 09:26
  * @version V1.0
  */
class S3IndexKeySpace(val sft: SimpleFeatureType,
                      val sharding: ShardStrategy,
                      geomField: String,
                      dtgField: String) extends IndexKeySpace[S3IndexValues, S3IndexKey] with LazyLogging {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  require(classOf[Point].isAssignableFrom(sft.getDescriptor(geomField).getType.getBinding),
    s"Expected field $geomField to have a point binding, but instead it has: " +
      sft.getDescriptor(geomField).getType.getBinding.getSimpleName)
  require(classOf[Date].isAssignableFrom(sft.getDescriptor(dtgField).getType.getBinding),
    s"Expected field $dtgField to have a date binding, but instead it has: " +
      sft.getDescriptor(dtgField).getType.getBinding.getSimpleName)

  protected val sfc = S3SFC(QueryProperties.S2MinLevel, QueryProperties.S2MaxLevel,
    QueryProperties.S2LevelMod, QueryProperties.S2MaxCells, sft.getS3Interval)

  protected val geomIndex: Int = sft.indexOf(geomField)
  protected val dtgIndex: Int = sft.indexOf(dtgField)

  protected val timeToIndex: TimeToBinnedTime = BinnedTime.timeToBinnedTime(sft.getS3Interval)

  private val dateToIndex = BinnedTime.dateToBinnedTime(sft.getS3Interval)
  private val boundsToDates = BinnedTime.boundsToIndexableDates(sft.getS3Interval)

  /**
    * The attributes used to create the index keys
    *
    * @return
    */
  override def attributes: Seq[String] = Seq(geomField, dtgField)

  /**
    * Length of an index key. If static (general case), will return a Right with the length. If dynamic,
    * will return Left with a function to determine the length from a given (row, offset, length)
    *
    * @return
    */
  override def indexKeyByteLength: Right[(Array[Byte], Int, Int) => Int, Int] = Right(10 + sharding.length)

  /**
    * Table sharing
    *
    * @return
    */
  override def sharing: Array[Byte] = Array.empty

  /**
    * Index key from the attributes of a simple feature
    *
    * @param writable simple feature with cached values
    * @param tier    tier bytes
    * @param id      feature id bytes
    * @param lenient if input values should be strictly checked, or normalized instead
    * @return
    */
  override def toIndexKey(writable: WritableFeature, tier: Array[Byte],
                          id: Array[Byte], lenient: Boolean): api.RowKeyValue[S3IndexKey] = {
    val geom = writable.getAttribute[Point](geomIndex)
    if (geom == null) {
      throw new IllegalArgumentException(s"Null geometry in feature ${writable.feature.getID}")
    }
    val dtg = writable.getAttribute[Date](dtgIndex)
    val time = if (dtg == null) { 0 } else { dtg.getTime }
    val BinnedTime(b, t) = timeToIndex(time)
    val s = try { sfc.index(geom.getX, geom.getY, lenient) } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid s value from geometry/time: $geom,$dtg", e)
    }
    val shard = sharding(writable)

    // create the byte array - allocate a single array up front to contain everything
    // ignore tier, not used here
    val bytes = Array.ofDim[Byte](shard.length + 14 + id.length)

    if (shard.isEmpty) {
      ByteArrays.writeShort(b, bytes, 0)
      ByteArrays.writeLong(s.id(), bytes, 2)
      ByteArrays.writeInt(t.toInt, bytes, 10)
      System.arraycopy(id, 0, bytes, 14, id.length)
    } else {
      bytes(0) = shard.head // shard is only a single byte
      ByteArrays.writeShort(b, bytes, 1)
      ByteArrays.writeLong(s.id(), bytes, 3)
      ByteArrays.writeInt(t.toInt, bytes, 11)
      System.arraycopy(id, 0, bytes, 15, id.length)
    }

    SingleRowKeyValue(bytes, sharing, shard, S3IndexKey(b, s.id(), t.toInt), tier, id, writable.values)
  }

  /**
    * Extracts values out of the filter used for range and push-down predicate creation
    *
    * @param filter  query filter
    * @param explain explainer
    * @return
    */
  override def getIndexValues(filter: Filter, explain: Explainer): S3IndexValues = {

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
      return S3IndexValues(sfc, geometries, Seq.empty, intervals, Map.empty, Seq.empty)
    }

    // compute our ranges based on the coarse bounds for our query
    val xy: Seq[(Double, Double, Double, Double)] = {
      val multiplier = QueryProperties.PolygonDecompMultiplier.toInt.get
      val bits = QueryProperties.PolygonDecompBits.toInt.get
      geometries.values.flatMap(GeometryUtils.bounds(_, multiplier, bits))
    }

    val minTime = 0
    val maxTime = sfc.time.toInt

    // calculate map of weeks to time intervals in that week
    val timesByBin = scala.collection.mutable.Map.empty[Short, Seq[(Int, Int)]].withDefaultValue(Seq.empty)
    val unboundedBins = Seq.newBuilder[(Short, Short)]

    // note: intervals shouldn't have any overlaps
    intervals.foreach { interval =>
      val (lower, upper) = boundsToDates(interval.bounds)
      val BinnedTime(lb, lt) = dateToIndex(lower)
      val BinnedTime(ub, ut) = dateToIndex(upper)

      if (interval.isBoundedBothSides) {
        if (lb == ub) {
          timesByBin(lb) ++= Seq((lt.toInt, ut.toInt))
        } else {
          timesByBin(lb) ++= Seq((lt.toInt, maxTime))
          timesByBin(ub) ++= Seq((minTime, ut.toInt))
          Range.inclusive(lb + 1, ub - 1).foreach(b => timesByBin(b.toShort) = sfc.wholePeriod)
        }
      } else if (interval.lower.value.isDefined) {
        timesByBin(lb) ++= Seq((lt.toInt, maxTime))
        unboundedBins += (((lb + 1).toShort, Short.MaxValue))
      } else if (interval.upper.value.isDefined) {
        timesByBin(ub) ++= Seq((minTime, ut.toInt))
        unboundedBins += ((0, (ub - 1).toShort))
      }
    }

    S3IndexValues(sfc, geometries, xy, intervals, timesByBin.toMap, unboundedBins.result())
  }

  /**
    * Creates ranges over the index keys
    *
    * @param values     index values @see getIndexValues
    * @param multiplier hint for how many times the ranges will be multiplied. can be used to
    *                   inform the number of ranges generated
    * @return
    */
  override def getRanges(values: S3IndexValues, multiplier: Int): Iterator[ScanRange[S3IndexKey]] = {

    val S3IndexValues(s3, _, xy, _, timesByBin, unboundedBins) = values

    // note: `target` will always be Some, as ScanRangesTarget has a default value
    val target = QueryProperties.ScanRangesTarget.option.map { t =>
      math.max(1, if (timesByBin.isEmpty) { t.toInt } else { t.toInt / timesByBin.size } / multiplier)
    }

    /*lazy val wholePeriodRanges = s3.wholePeriod*/

    val s2CellId = s3.ranges(xy, target)
    import scala.collection.JavaConversions._
    val bounded = timesByBin.iterator.flatMap { case (bin, times) => {
      val s3ScanRanges = ListBuffer.empty[ScanRange[S3IndexKey]]
      times.foreach(time => s2CellId.foreach(item=> s3ScanRanges
        .add(BoundedRange(S3IndexKey(bin, item.rangeMin().id(), time._1),
        S3IndexKey(bin, item.rangeMax().id(), time._2)))))
      s3ScanRanges
      }
    }

    val unbounded = unboundedBins.iterator.map {
      case (0, Short.MaxValue)     => UnboundedRange(S3IndexKey(0, 0L, 0))
      case (lower, Short.MaxValue) => LowerBoundedRange(S3IndexKey(lower, 0L, 0))
      case (0, upper)              => UpperBoundedRange(S3IndexKey(upper, 0L, Int.MaxValue))
      case (lower, upper) =>
        logger.error(s"Unexpected unbounded bin endpoints: $lower:$upper")
        UnboundedRange(S3IndexKey(0, 0L, 0))
    }

    bounded ++ unbounded
  }

  /**
    * Creates bytes from ranges
    *
    * @param ranges typed scan ranges. @see `getRanges`
    * @param tier   will the ranges have tiered ranges appended, or not
    * @return
    */
  override def getRangeBytes(ranges: Iterator[api.ScanRange[S3IndexKey]], tier: Boolean): Iterator[api.ByteRange] = {

    if (sharding.length == 0) {
      ranges.map {
        case BoundedRange(lo, hi) =>
          BoundedByteRange(ByteArrays.toS3Bytes(lo.bin, lo.s, lo.offset),
            ByteArrays.toS3Bytes(hi.bin, hi.s, hi.offset))

        case LowerBoundedRange(lo) =>
          BoundedByteRange(ByteArrays.toS3Bytes(lo.bin, lo.s, lo.offset), ByteRange.UnboundedUpperRange)

        case UpperBoundedRange(hi) =>
          BoundedByteRange(ByteRange.UnboundedLowerRange, ByteArrays.toS3Bytes(hi.bin, hi.s, hi.offset))

        case UnboundedRange(_) =>
          BoundedByteRange(ByteRange.UnboundedLowerRange, ByteRange.UnboundedUpperRange)

        case r =>
          throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    } else {
      ranges.flatMap {
        case BoundedRange(lo, hi) =>
          val lower = ByteArrays.toS3Bytes(lo.bin, lo.s, lo.offset)
          val upper = ByteArrays.toS3Bytes(hi.bin, hi.s, hi.offset)
          sharding.shards.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

        case LowerBoundedRange(lo) =>
          val lower = ByteArrays.toS3Bytes(lo.bin, lo.s, lo.offset)
          val upper = ByteRange.UnboundedUpperRange
          sharding.shards.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

        case UpperBoundedRange(hi) =>
          val lower = ByteRange.UnboundedLowerRange
          val upper = ByteArrays.toS3Bytes(hi.bin, hi.s, hi.offset)
          sharding.shards.map(p => BoundedByteRange(ByteArrays.concat(p, lower), ByteArrays.concat(p, upper)))

        case UnboundedRange(_) =>
          Seq(BoundedByteRange(ByteRange.UnboundedLowerRange, ByteRange.UnboundedUpperRange))

        case r =>
          throw new IllegalArgumentException(s"Unexpected range type $r")
      }
    }
  }

  /**
    * Determines if the ranges generated by `getRanges` are sufficient to fulfill the query,
    * or if additional filtering needs to be done
    *
    * @param config data store config
    * @param values index values @see getIndexValues
    * @param hints  query hints
    * @return
    */
  override def useFullFilter(values: Option[S3IndexValues],
                             config: Option[GeoMesaDataStoreFactory.GeoMesaDataStoreConfig],
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

object S3IndexKeySpace extends IndexKeySpaceFactory[S3IndexValues, S3IndexKey]{

  override def supports(sft: SimpleFeatureType, attributes: Seq[String]): Boolean =
    attributes.lengthCompare(2) == 0 && attributes.forall(sft.indexOf(_) != -1) &&
      classOf[Point].isAssignableFrom(sft.getDescriptor(attributes.head).getType.getBinding) &&
      classOf[Date].isAssignableFrom(sft.getDescriptor(attributes.last).getType.getBinding)

  override def apply(sft: SimpleFeatureType, attributes: Seq[String], tier: Boolean): S3IndexKeySpace = {
    val shards = if (tier) { NoShardStrategy } else { ZShardStrategy(sft) }
    new S3IndexKeySpace(sft, shards, attributes.head, attributes.last)
  }
}
