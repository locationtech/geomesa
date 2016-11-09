/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.google.common.primitives.{Bytes, Longs, Shorts}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Range => aRange}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.QueryProperties
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaStats
import org.locationtech.geomesa.accumulo.data.tables.{GeoMesaTable, XZ3Table}
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.curve.{BinnedTime, XZ3SFC}
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.SimpleFeatureType

class XZ3IdxStrategy(val filter: QueryFilter) extends Strategy with LazyLogging with IndexFilterHelpers  {

  /**
   * Plans the query - strategy implementations need to define this
   */
  override def getQueryPlan(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType): QueryPlan = {

    import QueryHints.RichHints
    import org.locationtech.geomesa.filter.FilterHelper._

    val ds  = queryPlanner.ds
    val sft = queryPlanner.sft

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

    output(s"Geometries: $geometries")
    output(s"Intervals: $intervals")

    if (geometries == DisjointGeometries || intervals == DisjointInterval) {
      output("Disjoint geometries or dates extracted, short-circuiting to empty query")
      return EmptyPlan(filter)
    }

    val ecql = filter.filter

    val (iterators, kvsToFeatures, colFamily) = if (hints.isBinQuery) {
      // if possible, use the pre-computed values
      // can't use if there are non-st filters or if custom fields are requested
      val (iters, cf) =
        if (filter.secondary.isEmpty && BinAggregatingIterator.canUsePrecomputedBins(sft, hints)) {
          (Seq(BinAggregatingIterator.configurePrecomputed(sft, XZ3Table, ecql, hints, deduplicate = false)), GeoMesaTable.BinColumnFamily)
        } else {
          val iter = BinAggregatingIterator.configureDynamic(sft, XZ3Table, ecql, hints, deduplicate = false)
          (Seq(iter), GeoMesaTable.FullColumnFamily)
        }
      (iters, BinAggregatingIterator.kvsToFeatures(), cf)
    } else if (hints.isDensityQuery) {
      val iter = KryoLazyDensityIterator.configure(sft, XZ3Table, ecql, hints)
      (Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), GeoMesaTable.FullColumnFamily)
    } else if (hints.isStatsIteratorQuery) {
      val iter = KryoLazyStatsIterator.configure(sft, XZ3Table, ecql, hints, deduplicate = false)
      (Seq(iter), KryoLazyStatsIterator.kvsToFeatures(sft), GeoMesaTable.FullColumnFamily)
    } else if (hints.isMapAggregatingQuery) {
      val iter = KryoLazyMapAggregatingIterator.configure(sft, XZ3Table, ecql, hints, deduplicate = false)
      (Seq(iter), queryPlanner.kvsToFeatures(sft, hints.getReturnSft, XZ3Table), GeoMesaTable.FullColumnFamily)
    } else {
      val iters = KryoLazyFilterTransformIterator.configure(sft, ecql, hints).toSeq
      (iters, queryPlanner.kvsToFeatures(sft, hints.getReturnSft, XZ3Table), GeoMesaTable.FullColumnFamily)
    }

    val table = ds.getTableName(sft.getTypeName, XZ3Table)
    val numThreads = ds.getSuggestedThreads(sft.getTypeName, XZ3Table)

    val sfc = XZ3SFC(XZ3Table.Precision, sft.getZ3Interval)
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
        val prefixes = XZ3Table.SPLIT_ARRAYS.map(Bytes.concat(tableSharing, _, binBytes))
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
    val cf = if (perAttributeIter.isEmpty) colFamily else GeoMesaTable.AttributeColumnFamily

    val iters = perAttributeIter ++ iterators
    BatchScanPlan(filter, table, ranges, iters, Seq(cf), kvsToFeatures, numThreads, hasDuplicates = false)
  }
}

object XZ3IdxStrategy extends StrategyProvider {

  override protected def statsBasedCost(sft: SimpleFeatureType,
                                        filter: QueryFilter,
                                        transform: Option[SimpleFeatureType],
                                        stats: GeoMesaStats): Option[Long] = {
    // https://geomesa.atlassian.net/browse/GEOMESA-1166
    // TODO check date range and use z2 instead if too big
    // TODO also if very small bbox, z2 has ~10 more bits of lat/lon info
    filter.primary match {
      case Some(f) => stats.getCount(sft, f, exact = false)
      case None    => Some(Long.MaxValue)
    }
  }

  /**
    * More than id lookups (at 1), high-cardinality attributes (at 1).
    * Less than unknown cardinality attributes (at 999).
    * With a spatial component, less than z2, otherwise more than z2 (at 400)
    */
  override protected def indexBasedCost(sft: SimpleFeatureType,
                                        filter: QueryFilter,
                                        transform: Option[SimpleFeatureType]): Long = {
    if (filter.primary.exists(isSpatialFilter)) 200L else 401L
  }
}
