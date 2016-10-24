/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.z3

import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Mutation, Range => aRange}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.QueryProperties
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, WritableFeature}
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.{AccumuloFeatureIndex, AccumuloFilterStrategy}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.curve.{BinnedTime, XZ3SFC}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.index.strategies.SpatioTemporalFilterStrategy
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.SimpleFeatureType

trait XZ3QueryableIndex extends AccumuloFeatureIndex
    with SpatioTemporalFilterStrategy[AccumuloDataStore, WritableFeature, Seq[Mutation], QueryPlan]
    with LazyLogging {

  writable: AccumuloWritableIndex =>

  override def getQueryPlan(sft: SimpleFeatureType,
                            ops: AccumuloDataStore,
                            filter:AccumuloFilterStrategy,
                            hints: Hints,
                            explain: Explainer): QueryPlan = {
    import QueryHints.RichHints
    import org.locationtech.geomesa.filter.FilterHelper.{logger => _, _}

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
        .filter(_.nonEmpty).getOrElse(Seq(WholeWorldPolygon))

    // since we don't apply a temporal filter, we pass handleExclusiveBounds to
    // make sure we exclude the non-inclusive endpoints of a during filter.
    // note that this isn't completely accurate, as we only index down to the second
    val intervals = filter.primary.map(extractIntervals(_, dtgField, handleExclusiveBounds = true)).getOrElse(Seq.empty)

    explain(s"Geometries: $geometries")
    explain(s"Intervals: $intervals")

    if (geometries == DisjointGeometries || intervals == DisjointInterval) {
      explain("Disjoint geometries or dates extracted, short-circuiting to empty query")
      return EmptyPlan(filter)
    }

    val ecql = filter.filter

    val (iterators, kvsToFeatures, colFamily) = if (hints.isBinQuery) {
      // if possible, use the pre-computed values
      // can't use if there are non-st filters or if custom fields are requested
      val (iters, cf) =
        if (filter.secondary.isEmpty && BinAggregatingIterator.canUsePrecomputedBins(sft, hints)) {
          val iter = BinAggregatingIterator.configurePrecomputed(sft, this, ecql, hints, deduplicate = false)
          (Seq(iter), AccumuloWritableIndex.BinColumnFamily)
        } else {
          val iter = BinAggregatingIterator.configureDynamic(sft, this, ecql, hints, deduplicate = false)
          (Seq(iter), AccumuloWritableIndex.FullColumnFamily)
        }
      (iters, BinAggregatingIterator.kvsToFeatures(), cf)
    } else if (hints.isDensityQuery) {
      val iter = KryoLazyDensityIterator.configure(sft, this, ecql, hints)
      (Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), AccumuloWritableIndex.FullColumnFamily)
    } else if (hints.isStatsIteratorQuery) {
      val iter = KryoLazyStatsIterator.configure(sft, this, ecql, hints, deduplicate = false)
      (Seq(iter), KryoLazyStatsIterator.kvsToFeatures(sft), AccumuloWritableIndex.FullColumnFamily)
    } else if (hints.isMapAggregatingQuery) {
      val iter = KryoLazyMapAggregatingIterator.configure(sft, this, ecql, hints, deduplicate = false)
      (Seq(iter), entriesToFeatures(sft, hints.getReturnSft), AccumuloWritableIndex.FullColumnFamily)
    } else {
      val iters = KryoLazyFilterTransformIterator.configure(sft, this, ecql, hints).toSeq
      (iters, entriesToFeatures(sft, hints.getReturnSft), AccumuloWritableIndex.FullColumnFamily)
    }

    val table = ops.getTableName(sft.getTypeName, this)
    val numThreads = ops.getSuggestedThreads(sft.getTypeName, this)

    val sfc = XZ3SFC(sft.getXZPrecision, sft.getZ3Interval)
    val minTime = 0.0
    val maxTime = BinnedTime.maxOffset(sft.getZ3Interval).toDouble
    val wholePeriod = (minTime, maxTime)

    val tableSharing = sft.getTableSharingBytes

    // compute our accumulo ranges based on the coarse bounds for our query
    val ranges = if (filter.primary.isEmpty) { Seq(aRange.prefix(new Text(tableSharing))) } else {
      val xy = geometries.map(GeometryUtils.bounds)

      // calculate map of weeks to time intervals in that week
      val timesByBin = scala.collection.mutable.Map.empty[Short, (Double, Double)]
      val dateToIndex = BinnedTime.dateToBinnedTime(sft.getZ3Interval)

      def updateTime(week: Short, lt: Double, ut: Double): Unit = {
        val times = timesByBin.get(week) match {
          case None => (lt, ut)
          case Some((min, max)) => (math.min(min, lt), math.max(max, ut))
        }
        timesByBin(week) = times
      }

      // note: intervals shouldn't have any overlaps
      intervals.foreach { interval =>
        val BinnedTime(lb, lt) = dateToIndex(interval._1)
        val BinnedTime(ub, ut) = dateToIndex(interval._2)
        if (lb == ub) {
          updateTime(lb, lt, ut)
        } else {
          updateTime(lb, lt, maxTime)
          updateTime(ub, minTime, ut)
          Range.inclusive(lb + 1, ub - 1).foreach(b => timesByBin(b.toShort) = wholePeriod)
        }
      }

      val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)

      def toZRanges(t: (Double, Double)): Seq[(Array[Byte], Array[Byte])] = {
        val query = xy.map { case (xmin, ymin, xmax, ymax) => (xmin, ymin, t._1, xmax, ymax, t._2) }
        sfc.ranges(query, rangeTarget).map(r => (Longs.toByteArray(r.lower), Longs.toByteArray(r.upper)))
      }

      lazy val wholePeriodRanges = toZRanges(wholePeriod)

      val ranges = timesByBin.flatMap { case (b, times) =>
        val zs = if (times.eq(wholePeriod)) wholePeriodRanges else toZRanges(times)
        val binBytes = Shorts.toByteArray(b)
        val prefixes = AccumuloWritableIndex.DefaultSplitArrays.map(Bytes.concat(tableSharing, _, binBytes))
        prefixes.flatMap { prefix =>
          zs.map { case (lo, hi) =>
            val start = Bytes.concat(prefix, lo)
            val end = Bytes.concat(prefix, hi)
            new aRange(new Text(start), true, aRange.followingPrefix(new Text(end)), false)
          }
        }
      }

      ranges.toSeq
    }

    val perAttributeIter = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   => Seq.empty
      case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(sft))
    }
    val cf = if (perAttributeIter.isEmpty) colFamily else AccumuloWritableIndex.AttributeColumnFamily

    val iters = perAttributeIter ++ iterators
    BatchScanPlan(filter, table, ranges, iters, Seq(cf), kvsToFeatures, numThreads, hasDuplicates = false)
  }
}
