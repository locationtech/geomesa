/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.google.common.primitives.{Bytes, Longs}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Range => aRange}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.QueryProperties
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaStats
import org.locationtech.geomesa.accumulo.data.tables.{GeoMesaTable, XZ2Table}
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.SimpleFeatureType

class XZ2IdxStrategy(val filter: QueryFilter) extends Strategy with LazyLogging with IndexFilterHelpers {

  /**
    * Plans the query - strategy implementations need to define this
    */
  override def getQueryPlan(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType): QueryPlan = {

    import QueryHints.RichHints
    import org.locationtech.geomesa.filter.FilterHelper._
    import org.locationtech.geomesa.filter._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

    val ds  = queryPlanner.ds
    val sft = queryPlanner.sft

    if (filter.primary.isEmpty) {
      filter.secondary.foreach { f =>
        logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
      }
    }

    val geometries = filter.primary.map(extractGeometries(_, sft.getGeomField, sft.isPoints))
        .filter(_.nonEmpty).getOrElse(Seq(WholeWorldPolygon))

    output(s"Geometries: $geometries")

    val ecql = filter.filter

    val (iterators, kvsToFeatures, colFamily, hasDupes) = if (hints.isBinQuery) {
      // if possible, use the pre-computed values
      // can't use if there are non-st filters or if custom fields are requested
      val (iters, cf) =
        if (filter.secondary.isEmpty && BinAggregatingIterator.canUsePrecomputedBins(sft, hints)) {
          (Seq(BinAggregatingIterator.configurePrecomputed(sft, XZ2Table, ecql, hints, deduplicate = false)), GeoMesaTable.BinColumnFamily)
        } else {
          val iter = BinAggregatingIterator.configureDynamic(sft, XZ2Table, ecql, hints, deduplicate = false)
          (Seq(iter), GeoMesaTable.FullColumnFamily)
        }
      (iters, BinAggregatingIterator.kvsToFeatures(), cf, false)
    } else if (hints.isDensityQuery) {
      val iter = KryoLazyDensityIterator.configure(sft, XZ2Table, ecql, hints)
      (Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), GeoMesaTable.FullColumnFamily, false)
    } else if (hints.isStatsIteratorQuery) {
      val iter = KryoLazyStatsIterator.configure(sft, XZ2Table, ecql, hints, sft.nonPoints)
      (Seq(iter), KryoLazyStatsIterator.kvsToFeatures(sft), GeoMesaTable.FullColumnFamily, false)
    } else if (hints.isMapAggregatingQuery) {
      val iter = KryoLazyMapAggregatingIterator.configure(sft, XZ2Table, ecql, hints, sft.nonPoints)
      (Seq(iter), queryPlanner.kvsToFeatures(sft, hints.getReturnSft, XZ2Table), GeoMesaTable.FullColumnFamily, false)
    } else {
      val iters = KryoLazyFilterTransformIterator.configure(sft, ecql, hints).toSeq
      (iters, queryPlanner.kvsToFeatures(sft, hints.getReturnSft, XZ2Table), GeoMesaTable.FullColumnFamily, false)
    }

    val table = ds.getTableName(sft.getTypeName, XZ2Table)
    val numThreads = ds.getSuggestedThreads(sft.getTypeName, XZ2Table)

    val ranges = if (filter.primary.isEmpty) {
      if (sft.isTableSharing) {
        Seq(aRange.prefix(new Text(sft.getTableSharingBytes)))
      } else {
        Seq(new aRange())
      }
    } else {
      // setup Z2 iterator
      val xy = geometries.map(GeometryUtils.bounds)
      val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)
      val zRanges = XZ2Table.SFC.ranges(xy, rangeTarget).map { range =>
        (Longs.toByteArray(range.lower), Longs.toByteArray(range.upper))
      }

      val prefixes = if (sft.isTableSharing) {
        val ts = sft.getTableSharingBytes
        XZ2Table.SPLIT_ARRAYS.map(ts ++ _)
      } else {
        XZ2Table.SPLIT_ARRAYS
      }

      prefixes.flatMap { prefix =>
        zRanges.map { case (lo, hi) =>
          val start = new Text(Bytes.concat(prefix, lo))
          val end = aRange.followingPrefix(new Text(Bytes.concat(prefix, hi)))
          new aRange(start, true, end, false)
        }
      }
    }

    val perAttributeIter = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   => Seq.empty
      case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(sft))
    }
    val cf = if (perAttributeIter.isEmpty) colFamily else GeoMesaTable.AttributeColumnFamily

    val iters = perAttributeIter ++ iterators
    BatchScanPlan(filter, table, ranges, iters, Seq(cf), kvsToFeatures, numThreads, hasDupes)
  }
}

object XZ2IdxStrategy extends StrategyProvider {

  override protected def statsBasedCost(sft: SimpleFeatureType,
                                        filter: QueryFilter,
                                        transform: Option[SimpleFeatureType],
                                        stats: GeoMesaStats): Option[Long] = {
    filter.primary match {
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
}

