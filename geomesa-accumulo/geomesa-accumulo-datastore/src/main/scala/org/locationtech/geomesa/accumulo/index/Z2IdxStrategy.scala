/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.nio.charset.StandardCharsets

import com.google.common.primitives.{Bytes, Longs}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Range => aRange}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaStats
import org.locationtech.geomesa.accumulo.data.tables.{GeoMesaTable, Z2Table}
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.utils.geotools.WholeWorldPolygon
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.locationtech.sfcurve.zorder.Z2
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter
import org.opengis.filter.spatial._

class Z2IdxStrategy(val filter: QueryFilter) extends Strategy with LazyLogging with IndexFilterHelpers {

  /**
    * Plans the query - strategy implementations need to define this
    */
  override def getQueryPlan(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType): QueryPlan = {

    import QueryHints.{LOOSE_BBOX, RichHints}
    import Z2IdxStrategy._
    import org.locationtech.geomesa.filter.FilterHelper._
    import org.locationtech.geomesa.filter._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

    val ds  = queryPlanner.ds
    val sft = queryPlanner.sft

    val isInclude = QueryFilterSplitter.isFullTableScan(filter)

    if (isInclude) {
      // allow for full table scans - we use the z2 index for queries that can't be satisfied elsewhere
      filter.secondary.foreach { f =>
        logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
      }
    }

    // TODO GEOMESA-1215 this can handle OR'd geoms, but the query splitter won't currently send them
    val geometryToCover =
      filter.singlePrimary.flatMap(extractSingleGeometry(_, sft.getGeomField)).getOrElse(WholeWorldPolygon)

    output(s"GeomsToCover: $geometryToCover")

    val looseBBox = if (hints.containsKey(LOOSE_BBOX)) Boolean.unbox(hints.get(LOOSE_BBOX)) else ds.config.looseBBox

    val ecql: Option[Filter] = if (isInclude || !looseBBox || sft.nonPoints) {
      // if this is a full table scan, we can just use the filter option to get the secondary ecql
      // if the user has requested strict bounding boxes, we apply the full filter
      // if this is a non-point geometry, the index is coarse-grained, so we apply the full filter
      filter.filter
    } else {
      // for normal bboxes, the index is fine enough that we don't need to apply the filter on top of it
      // this may cause some minor errors at extremely fine resolution, but the performance is worth it
      // if we have a complicated geometry predicate, we need to pass it through to be evaluated
      val complexGeomFilter = filterListAsAnd(filter.primary.filter(isComplicatedSpatialFilter))
      (complexGeomFilter, filter.secondary) match {
        case (Some(gf), Some(fs)) => filterListAsAnd(Seq(gf, fs))
        case (None, fs)           => fs
        case (gf, None)           => gf
      }
    }

    val (iterators, kvsToFeatures, colFamily, hasDupes) = if (hints.isBinQuery) {
      // if possible, use the pre-computed values
      // can't use if there are non-st filters or if custom fields are requested
      val (iters, cf) =
        if (filter.secondary.isEmpty && BinAggregatingIterator.canUsePrecomputedBins(sft, hints)) {
          // TODO GEOMESA-1254 per-attribute vis + bins
          val idOffset = Z2Table.getIdRowOffset(sft)
          (Seq(BinAggregatingIterator.configurePrecomputed(sft, ecql, hints, idOffset, sft.nonPoints)), Z2Table.BIN_CF)
        } else {
          val iter = BinAggregatingIterator.configureDynamic(sft, ecql, hints, sft.nonPoints)
          (Seq(iter), Z2Table.FULL_CF)
        }
      (iters, BinAggregatingIterator.kvsToFeatures(), cf, false)
    } else if (hints.isDensityQuery) {
      val iter = Z2DensityIterator.configure(sft, ecql, hints)
      (Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), Z2Table.FULL_CF, false)
    } else if (hints.isStatsIteratorQuery) {
      val iter = KryoLazyStatsIterator.configure(sft, ecql, hints, sft.nonPoints)
      (Seq(iter), KryoLazyStatsIterator.kvsToFeatures(sft), Z2Table.FULL_CF, false)
    } else if (hints.isMapAggregatingQuery) {
      val iter = KryoLazyMapAggregatingIterator.configure(sft, ecql, hints, sft.nonPoints)
      (Seq(iter), queryPlanner.defaultKVsToFeatures(hints), Z2Table.FULL_CF, false)
    } else {
      val iters = KryoLazyFilterTransformIterator.configure(sft, ecql, hints).toSeq
      (iters, queryPlanner.defaultKVsToFeatures(hints), Z2Table.FULL_CF, sft.nonPoints)
    }

    val z2table = ds.getTableName(sft.getTypeName, Z2Table)
    val numThreads = ds.getSuggestedThreads(sft.getTypeName, Z2Table)

    val (ranges, z2Iter) = if (isInclude) {
      val range = if (sft.isTableSharing) {
        aRange.prefix(new Text(sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)))
      } else {
        new aRange()
      }
      (Seq(range), None)
    } else {
      // setup Z2 iterator
      val env = geometryToCover.getEnvelopeInternal
      val (lx, ly, ux, uy) = (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)

      val getRanges: (Seq[Array[Byte]], (Double, Double), (Double, Double)) => Seq[aRange] =
        if (sft.isPoints) getPointRanges else getGeomRanges

      val prefixes = if (sft.isTableSharing) {
        val ts = sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
        Z2Table.SPLIT_ARRAYS.map(ts ++ _)
      } else {
        Z2Table.SPLIT_ARRAYS
      }
      val ranges = getRanges(prefixes, (lx, ux), (ly, uy))

      // index space values for comparing in the iterator
      def decode(x: Double, y: Double): (Int, Int) = if (sft.isPoints) {
        Z2SFC.index(x, y).decode
      } else {
        Z2(Z2SFC.index(x, y).z & Z2Table.GEOM_Z_MASK).decode
      }

      val (xmin, ymin) = decode(lx, ly)
      val (xmax, ymax) = decode(ux, uy)

      val zIter = Z2Iterator.configure(sft.isPoints, sft.isTableSharing, xmin, xmax, ymin, ymax, Z2IdxStrategy.Z2_ITER_PRIORITY)

      (ranges, Some(zIter))
    }

    val perAttributeIter = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   => Seq.empty
      case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(sft, Z2Table))
    }
    val cf = if (perAttributeIter.isEmpty) colFamily else GeoMesaTable.AttributeColumnFamily

    val iters = perAttributeIter ++ iterators ++ z2Iter
    BatchScanPlan(filter, z2table, ranges, iters, Seq(cf), kvsToFeatures, numThreads, hasDupes)
  }

  def getPointRanges(prefixes: Seq[Array[Byte]], x: (Double, Double), y: (Double, Double)): Seq[aRange] = {
    Z2SFC.ranges(x, y).flatMap { case indexRange =>
      val startBytes = Longs.toByteArray(indexRange.lower)
      val endBytes = Longs.toByteArray(indexRange.upper)
      prefixes.map { prefix =>
        val start = new Text(Bytes.concat(prefix, startBytes))
        val end = aRange.followingPrefix(new Text(Bytes.concat(prefix, endBytes)))
        new aRange(start, true, end, false)
      }
    }
  }

  def getGeomRanges(prefixes: Seq[Array[Byte]], x: (Double, Double), y: (Double, Double)): Seq[aRange] = {
    Z2SFC.ranges(x, y, 8 * Z2Table.GEOM_Z_NUM_BYTES).flatMap { indexRange =>
      val startBytes = Longs.toByteArray(indexRange.lower).take(Z2Table.GEOM_Z_NUM_BYTES)
      val endBytes = Longs.toByteArray(indexRange.upper).take(Z2Table.GEOM_Z_NUM_BYTES)
      prefixes.map { prefix =>
        val start = new Text(Bytes.concat(prefix, startBytes))
        val end = aRange.followingPrefix(new Text(Bytes.concat(prefix, endBytes)))
        new aRange(start, true, end, false)
      }
    }
  }
}

object Z2IdxStrategy extends StrategyProvider {

  val Z2_ITER_PRIORITY = 23
  val FILTERING_ITER_PRIORITY = 25

  override protected def statsBasedCost(sft: SimpleFeatureType,
                                        filter: QueryFilter,
                                        transform: Option[SimpleFeatureType],
                                        stats: GeoMesaStats): Option[Long] = {
    filter.singlePrimary match {
      case None => Some(Long.MaxValue)
      // add one so that we prefer the z3 index even if geometry is the limiting factor, resulting in the same count
      case Some(f) => stats.getCount(sft, f, exact = false).map(c => if (c == 0L) 0L else c + 1L)
    }
  }

  /**
    * More than id lookups (at 1), high-cardinality attributes (at 1), z3 (at 200).
    * Less than spatial-only z3 (at 401), unknown cardinality attributes (at 999).
    */
  override protected def indexBasedCost(sft: SimpleFeatureType,
                                        filter: QueryFilter,
                                        transform: Option[SimpleFeatureType]): Long = 400L

  def isComplicatedSpatialFilter(f: Filter): Boolean = {
    f match {
      case _: BBOX => false
      case _: DWithin => true
      case _: Contains => true
      case _: Crosses => true
      case _: Intersects => true
      case _: Overlaps => true
      case _: Within => true
      case _ => false        // Beyond, Disjoint, DWithin, Equals, Touches
    }
  }

}

