/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.z3

import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Mutation, Range => aRange}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators.{Z3DensityIterator, _}
import org.locationtech.geomesa.accumulo.{AccumuloFeatureIndexType, AccumuloFilterStrategyType}
import org.locationtech.geomesa.curve.{BinnedTime, Z3SFC}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.strategies.SpatioTemporalFilterStrategy
import org.locationtech.geomesa.index.utils.{Explainer, KryoLazyStatsUtils, SplitArrays}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.SimpleFeatureType

trait Z3QueryableIndex extends AccumuloFeatureIndexType
    with SpatioTemporalFilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation]
    with LazyLogging {

  writable: Z3WritableIndex =>

  def hasSplits: Boolean

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: AccumuloDataStore,
                            filter: AccumuloFilterStrategyType,
                            hints: Hints,
                            explain: Explainer): AccumuloQueryPlan = {

    import AccumuloFeatureIndex.{BinColumnFamily, FullColumnFamily}
    import Z3IndexV2.GEOM_Z_NUM_BYTES
    import org.locationtech.geomesa.filter.FilterHelper._
    import org.locationtech.geomesa.index.conf.QueryHints.{LOOSE_BBOX, RichHints}

    // note: z3 requires a date field
    val dtgField = sft.getDtgField.getOrElse {
      throw new RuntimeException("Trying to execute a z3 query but the schema does not have a date")
    }

    if (filter.primary.isEmpty) {
      filter.secondary.foreach { f =>
        logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
      }
    }

    // standardize the two key query arguments:  polygon and date-range

    val geometries = filter.primary.map(extractGeometries(_, sft.getGeomField, sft.isPoints))
        .filter(_.nonEmpty).getOrElse(FilterValues(Seq(WholeWorldPolygon)))

    // since we don't apply a temporal filter, we pass handleExclusiveBounds to
    // make sure we exclude the non-inclusive endpoints of a during filter.
    // note that this isn't completely accurate, as we only index down to the second
    val intervals =
      filter.primary.map(extractIntervals(_, dtgField, handleExclusiveBounds = true)).getOrElse(FilterValues.empty)

    explain(s"Geometries: $geometries")
    explain(s"Intervals: $intervals")

    if (geometries.disjoint || intervals.disjoint) {
      explain("Disjoint geometries or dates extracted, short-circuiting to empty query")
      return EmptyPlan(filter)
    }

    val looseBBox = if (hints.containsKey(LOOSE_BBOX)) Boolean.unbox(hints.get(LOOSE_BBOX)) else ds.config.looseBBox

    // if the user has requested strict bounding boxes, we apply the full filter
    // if this is a non-point geometry type, the index is coarse-grained, so we apply the full filter
    // if the spatial predicate is rectangular (e.g. a bbox), the index is fine enough that we
    // don't need to apply the filter on top of it. this may cause some minor errors at extremely
    // fine resolutions, but the performance is worth it
    // if we have a complicated geometry predicate, we need to pass it through to be evaluated
    val ecql = if (looseBBox && sft.isPoints && geometries.values.forall(GeometryUtils.isRectangular)) {
      filter.secondary
    } else {
      filter.filter
    }

    val (iterators, kvsToFeatures, reduce, colFamily, hasDupes) = if (hints.isBinQuery) {
      // if possible, use the pre-computed values
      // can't use if there are non-st filters or if custom fields are requested
      val (iters, cf) =
        if (filter.secondary.isEmpty && BinAggregatingIterator.canUsePrecomputedBins(sft, hints)) {
          (Seq(BinAggregatingIterator.configurePrecomputed(sft, this, ecql, hints, sft.nonPoints)), BinColumnFamily)
        } else {
          val iter = BinAggregatingIterator.configureDynamic(sft, this, ecql, hints, sft.nonPoints)
          (Seq(iter), FullColumnFamily)
        }
      (iters, BinAggregatingIterator.kvsToFeatures(), None, cf, false)
    } else if (hints.isDensityQuery) {
      val iter = Z3DensityIterator.configure(sft, this, ecql, hints)
      (Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), None, FullColumnFamily, false)
    } else if (hints.isStatsQuery) {
      val iter = KryoLazyStatsIterator.configure(sft, this, ecql, hints, sft.nonPoints)
      val reduce = Some(KryoLazyStatsUtils.reduceFeatures(sft, hints)(_))
      (Seq(iter), KryoLazyStatsIterator.kvsToFeatures(sft), reduce, FullColumnFamily, false)
    } else if (hints.isMapAggregatingQuery) {
      val iter = KryoLazyMapAggregatingIterator.configure(sft, this, ecql, hints, sft.nonPoints)
      val reduce = Some(KryoLazyMapAggregatingIterator.reduceMapAggregationFeatures(hints)(_))
      (Seq(iter), entriesToFeatures(sft, hints.getReturnSft), reduce, FullColumnFamily, false)
    } else {
      val iters = KryoLazyFilterTransformIterator.configure(sft, this, ecql, hints).toSeq
      (iters, entriesToFeatures(sft, hints.getReturnSft), None, FullColumnFamily, sft.nonPoints)
    }

    val z3table = getTableName(sft.getTypeName, ds)
    val numThreads = ds.config.queryThreads

    val sfc = Z3SFC(sft.getZ3Interval)
    val minTime = sfc.time.min.toLong
    val maxTime = sfc.time.max.toLong
    val wholePeriod = Seq((minTime, maxTime))

    // compute our accumulo ranges based on the coarse bounds for our query
    val (ranges, zIterator) = if (filter.primary.isEmpty) { (Seq(new aRange()), None) } else {
      val xy = geometries.values.map(GeometryUtils.bounds)

      // calculate map of weeks to time intervals in that week
      val timesByBin = scala.collection.mutable.Map.empty[Short, Seq[(Long, Long)]].withDefaultValue(Seq.empty)
      val dateToIndex = BinnedTime.dateToBinnedTime(sft.getZ3Interval)
      // note: intervals shouldn't have any overlaps
      intervals.foreach { interval =>
        val BinnedTime(lb, lt) = dateToIndex(interval._1)
        val BinnedTime(ub, ut) = dateToIndex(interval._2)
        if (lb == ub) {
          timesByBin(lb) ++= Seq((lt, ut))
        } else {
          timesByBin(lb) ++= Seq((lt, maxTime))
          timesByBin(ub) ++= Seq((minTime, ut))
          Range.inclusive(lb + 1, ub - 1).foreach(b => timesByBin(b.toShort) = wholePeriod)
        }
      }

      val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)
      def toZRanges(t: Seq[(Long, Long)]): Seq[(Array[Byte], Array[Byte])] = if (sft.isPoints) {
        sfc.ranges(xy, t, 64, rangeTarget).map(r => (Longs.toByteArray(r.lower), Longs.toByteArray(r.upper)))
      } else {
        sfc.ranges(xy, t, 8 * GEOM_Z_NUM_BYTES, rangeTarget).map { r =>
          (Longs.toByteArray(r.lower).take(GEOM_Z_NUM_BYTES), Longs.toByteArray(r.upper).take(GEOM_Z_NUM_BYTES))
        }
      }

      lazy val wholePeriodRanges = toZRanges(wholePeriod)

      val ranges = timesByBin.flatMap { case (b, times) =>
        val zs = if (times.eq(wholePeriod)) wholePeriodRanges else toZRanges(times)
        val binBytes = Shorts.toByteArray(b)
        val prefixes = if (hasSplits) {
          SplitArrays.apply(sft.getZShards).map(Bytes.concat(_, binBytes))
        } else {
          Seq(binBytes)
        }
        prefixes.flatMap { prefix =>
          zs.map { case (lo, hi) =>
            val start = Bytes.concat(prefix, lo)
            val end = Bytes.concat(prefix, hi)
            new aRange(new Text(start), true, aRange.followingPrefix(new Text(end)), false)
          }
        }
      }

      // we know we're only going to scan appropriate periods, so leave out whole periods
      val filteredTimes = timesByBin.filter(_._2 != wholePeriod).toMap
      val zIter = Z3Iterator.configure(sfc, xy, filteredTimes, sft.isPoints, hasSplits, isSharing = false, Z3Index.Z3IterPriority)
      (ranges.toSeq, Some(zIter))
    }

    val perAttributeIter = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   => Seq.empty
      case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(sft))
    }
    val cf = if (perAttributeIter.isEmpty) colFamily else AccumuloFeatureIndex.AttributeColumnFamily

    val iters = perAttributeIter ++ zIterator.toSeq ++ iterators
    BatchScanPlan(filter, z3table, ranges, iters, Seq(cf), kvsToFeatures, reduce, numThreads, hasDupes)
  }
}
