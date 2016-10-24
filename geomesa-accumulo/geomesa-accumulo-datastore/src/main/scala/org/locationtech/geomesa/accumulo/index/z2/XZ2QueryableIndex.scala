/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index.z2

import com.google.common.primitives.{Bytes, Longs}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Mutation, Range => aRange}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties.QueryProperties
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, WritableFeature}
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex._
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.curve.XZ2SFC
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, WholeWorldPolygon}
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.SimpleFeatureType

trait XZ2QueryableIndex extends AccumuloFeatureIndex
    with SpatialFilterStrategy[AccumuloDataStore, WritableFeature, Seq[Mutation], QueryPlan]
    with LazyLogging {

  writable: AccumuloWritableIndex =>

  override def getQueryPlan(sft: SimpleFeatureType,
                            ops: AccumuloDataStore,
                            filter:AccumuloFilterStrategy,
                            hints: Hints,
                            explain: Explainer): QueryPlan = {

    import QueryHints.RichHints
    import org.locationtech.geomesa.filter.FilterHelper.{logger => _, _}
    import org.locationtech.geomesa.filter._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._

    if (filter.primary.isEmpty) {
      filter.secondary.foreach { f =>
        logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
      }
    }

    val geometries = filter.primary.map(extractGeometries(_, sft.getGeomField, sft.isPoints))
        .filter(_.nonEmpty).getOrElse(Seq(WholeWorldPolygon))

    explain(s"Geometries: $geometries")

    if (geometries == DisjointGeometries) {
      explain("Non-intersecting geometries extracted, short-circuiting to empty query")
      return EmptyPlan(filter)
    }

    val ecql = filter.filter

    val (iterators, kvsToFeatures, colFamily, hasDupes) = if (hints.isBinQuery) {
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
      (iters, BinAggregatingIterator.kvsToFeatures(), cf, false)
    } else if (hints.isDensityQuery) {
      val iter = KryoLazyDensityIterator.configure(sft, this, ecql, hints)
      (Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), AccumuloWritableIndex.FullColumnFamily, false)
    } else if (hints.isStatsIteratorQuery) {
      val iter = KryoLazyStatsIterator.configure(sft, this, ecql, hints, deduplicate = false)
      (Seq(iter), KryoLazyStatsIterator.kvsToFeatures(sft), AccumuloWritableIndex.FullColumnFamily, false)
    } else if (hints.isMapAggregatingQuery) {
      val iter = KryoLazyMapAggregatingIterator.configure(sft, this, ecql, hints, deduplicate = false)
      (Seq(iter), entriesToFeatures(sft, hints.getReturnSft), AccumuloWritableIndex.FullColumnFamily, false)
    } else {
      val iters = KryoLazyFilterTransformIterator.configure(sft, this, ecql, hints).toSeq
      (iters, entriesToFeatures(sft, hints.getReturnSft), AccumuloWritableIndex.FullColumnFamily, false)
    }

    val table = ops.getTableName(sft.getTypeName, this)
    val numThreads = ops.getSuggestedThreads(sft.getTypeName, this)

    val ranges = if (filter.primary.isEmpty) {
      if (sft.isTableSharing) {
        Seq(aRange.prefix(new Text(sft.getTableSharingBytes)))
      } else {
        Seq(new aRange())
      }
    } else {
      // determine the ranges using the XZ curve
      val xy = geometries.map(GeometryUtils.bounds)
      val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)
      val sfc = XZ2SFC(sft.getXZPrecision)
      val zRanges = sfc.ranges(xy, rangeTarget).map { range =>
        (Longs.toByteArray(range.lower), Longs.toByteArray(range.upper))
      }

      val prefixes = if (sft.isTableSharing) {
        val ts = sft.getTableSharingBytes
        AccumuloWritableIndex.DefaultSplitArrays.map(ts ++ _)
      } else {
        AccumuloWritableIndex.DefaultSplitArrays
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
    val cf = if (perAttributeIter.isEmpty) colFamily else AccumuloWritableIndex.AttributeColumnFamily

    val iters = perAttributeIter ++ iterators
    BatchScanPlan(filter, table, ranges, iters, Seq(cf), kvsToFeatures, numThreads, hasDupes)
  }
}
