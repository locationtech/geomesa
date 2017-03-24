/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.z2

import java.nio.charset.StandardCharsets

import com.google.common.primitives.{Bytes, Longs}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Mutation, Range => aRange}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.AccumuloFilterStrategyType
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators.{Z2DensityIterator, _}
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.index.conf.QueryProperties
import org.locationtech.geomesa.index.strategies.SpatialFilterStrategy
import org.locationtech.geomesa.index.utils.{Explainer, SplitArrays}
import org.locationtech.geomesa.utils.geotools._
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.SimpleFeatureType

trait Z2QueryableIndex extends AccumuloFeatureIndex
    with SpatialFilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation]
    with LazyLogging {

  writable: Z2WritableIndex =>

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: AccumuloDataStore,
                            filter: AccumuloFilterStrategyType,
                            hints: Hints,
                            explain: Explainer): AccumuloQueryPlan = {

    import AccumuloFeatureIndex.{BinColumnFamily, FullColumnFamily}
    import org.locationtech.geomesa.filter.FilterHelper._
    import org.locationtech.geomesa.filter._
    import org.locationtech.geomesa.index.conf.QueryHints._
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    if (filter.primary.isEmpty) {
      filter.secondary.foreach { f =>
        logger.warn(s"Running full table scan for schema ${sft.getTypeName} with filter ${filterToString(f)}")
      }
    }

    val geometries = filter.primary.map(extractGeometries(_, sft.getGeomField, sft.isPoints))
        .filter(_.nonEmpty).getOrElse(FilterValues(Seq(WholeWorldPolygon)))

    explain(s"Geometries: $geometries")

    if (geometries.disjoint) {
      explain("Non-intersecting geometries extracted, short-circuiting to empty query")
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
      val iter = Z2DensityIterator.configure(sft, this, ecql, hints)
      (Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), None, FullColumnFamily, false)
    } else if (hints.isStatsIteratorQuery) {
      val iter = KryoLazyStatsIterator.configure(sft, this, ecql, hints, sft.nonPoints)
      val reduce = Some(KryoLazyStatsIterator.reduceFeatures(sft, hints)(_))
      (Seq(iter), KryoLazyStatsIterator.kvsToFeatures(sft), reduce, FullColumnFamily, false)
    } else if (hints.isMapAggregatingQuery) {
      val iter = KryoLazyMapAggregatingIterator.configure(sft, this, ecql, hints, sft.nonPoints)
      val reduce = Some(KryoLazyMapAggregatingIterator.reduceMapAggregationFeatures(hints)(_))
      (Seq(iter), entriesToFeatures(sft, hints.getReturnSft), reduce, FullColumnFamily, false)
    } else {
      val iters = KryoLazyFilterTransformIterator.configure(sft, this, ecql, hints).toSeq
      (iters, entriesToFeatures(sft, hints.getReturnSft), None, FullColumnFamily, sft.nonPoints)
    }

    val z2table = getTableName(sft.getTypeName, ds)
    val numThreads = ds.config.queryThreads

    val (ranges, z2Iter) = if (filter.primary.isEmpty) {
      val range = if (sft.isTableSharing) {
        aRange.prefix(new Text(sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)))
      } else {
        new aRange()
      }
      (Seq(range), None)
    } else {
      // setup Z2 iterator
      import org.locationtech.geomesa.accumulo.index.legacy.z2.Z2IndexV1.GEOM_Z_NUM_BYTES
      val xy = geometries.values.map(GeometryUtils.bounds)
      val rangeTarget = QueryProperties.SCAN_RANGES_TARGET.option.map(_.toInt)
      val zRanges = if (sft.isPoints) {
        Z2SFC.ranges(xy, 64, rangeTarget).map(r => (Longs.toByteArray(r.lower), Longs.toByteArray(r.upper)))
      } else {
        Z2SFC.ranges(xy, 8 * GEOM_Z_NUM_BYTES, rangeTarget).map { r =>
          (Longs.toByteArray(r.lower).take(GEOM_Z_NUM_BYTES), Longs.toByteArray(r.upper).take(GEOM_Z_NUM_BYTES))
        }
      }

      val prefixes = if (sft.isTableSharing) {
        val ts = sft.getTableSharingPrefix.getBytes(StandardCharsets.UTF_8)
        SplitArrays.apply(sft.getZShards).map(ts ++ _)
      } else {
        SplitArrays.apply(sft.getZShards)
      }

      val ranges = prefixes.flatMap { prefix =>
        zRanges.map { case (lo, hi) =>
          val start = new Text(Bytes.concat(prefix, lo))
          val end = aRange.followingPrefix(new Text(Bytes.concat(prefix, hi)))
          new aRange(start, true, end, false)
        }
      }

      val zIter = Z2Iterator.configure(sft, xy, Z2Index.Z2IterPriority)

      (ranges, Some(zIter))
    }

    val perAttributeIter = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   => Seq.empty
      case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(sft))
    }
    val cf = if (perAttributeIter.isEmpty) colFamily else AccumuloFeatureIndex.AttributeColumnFamily

    val iters = perAttributeIter ++ iterators ++ z2Iter
    BatchScanPlan(filter, z2table, ranges, iters, Seq(cf), kvsToFeatures, reduce, numThreads, hasDupes)
  }
}