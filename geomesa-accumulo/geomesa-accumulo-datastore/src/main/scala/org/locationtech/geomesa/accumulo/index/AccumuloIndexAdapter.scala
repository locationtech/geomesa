/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Map.Entry

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Mutation, Range, Value}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeature}
import org.locationtech.geomesa.accumulo.index.AccumuloIndexAdapter.ScanConfig
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan}
import org.locationtech.geomesa.index.index.IndexAdapter
import org.locationtech.geomesa.index.utils.KryoLazyStatsUtils
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait AccumuloIndexAdapter extends IndexAdapter[AccumuloDataStore, AccumuloFeature, Mutation, Range, ScanConfig] {

  this: AccumuloFeatureIndex =>

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  protected def hasDuplicates(sft: SimpleFeatureType, filter: Option[Filter]): Boolean = false

  protected def queryThreads(ds: AccumuloDataStore): Int = ds.config.queryThreads

  override protected def createInsert(row: Array[Byte], feature: AccumuloFeature): Mutation = {
    val mutation = new Mutation(row)
    feature.fullValues.foreach(v => mutation.put(v.cf, v.cq, v.vis, v.value))
    if (hasPrecomputedBins) {
      feature.binValues.foreach(v => mutation.put(v.cf, v.cq, v.vis, v.value))
    }
    mutation
  }

  override protected def createDelete(row: Array[Byte], feature: AccumuloFeature): Mutation = {
    val mutation = new Mutation(row)
    feature.fullValues.foreach(v => mutation.putDelete(v.cf, v.cq, v.vis))
    if (hasPrecomputedBins) {
      feature.binValues.foreach(v => mutation.putDelete(v.cf, v.cq, v.vis))
    }
    mutation
  }

  override protected def range(start: Array[Byte], end: Array[Byte]): Range = {
    // index api defines empty start/end for open-ended range - in accumulo, it's indicated with null
    val startKey = if (start.length == 0) { null } else { new Key(new Text(start)) }
    val endKey = if (end.length == 0) { null } else { new Key(new Text(end)) }
    // index api defines start row inclusive, end row exclusive
    new Range(startKey, true, endKey, false)
  }

  override protected def rangeExact(row: Array[Byte]): Range = Range.exact(new Text(row))

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: AccumuloDataStore,
                                  filter: FilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation],
                                  config: ScanConfig): QueryPlan[AccumuloDataStore, AccumuloFeature, Mutation] = {
    if (config.ranges.isEmpty) { EmptyPlan(filter) } else {
      val table = getTableName(sft.getTypeName, ds)
      val numThreads = queryThreads(ds)
      val ScanConfig(ranges, cf, iters, eToF, reduce, dedupe) = config
      BatchScanPlan(filter, table, ranges, iters, Seq(cf), eToF, reduce, numThreads, dedupe)
    }
  }

  override protected def scanConfig(sft: SimpleFeatureType,
                                    ds: AccumuloDataStore,
                                    filter: FilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation],
                                    ranges: Seq[Range],
                                    ecql: Option[Filter],
                                    hints: Hints): ScanConfig = {
    import AccumuloFeatureIndex.{AttributeColumnFamily, BinColumnFamily, FullColumnFamily}
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val dedupe = hasDuplicates(sft, filter.primary)

    // if possible, use the pre-computed values for bin queries
    // can't use if there are non-st filters or if custom fields are requested
    val isPrecomputedBins = hints.isBinQuery && hasPrecomputedBins && ecql.isEmpty &&
        BinAggregatingIterator.canUsePrecomputedBins(sft, hints)

    val config = if (isPrecomputedBins) {
      val iter = BinAggregatingIterator.configurePrecomputed(sft, this, ecql, hints, dedupe)
      ScanConfig(ranges, BinColumnFamily, Seq(iter), BinAggregatingIterator.kvsToFeatures(), None, duplicates = false)
    } else if (hints.isBinQuery) {
      val iter = BinAggregatingIterator.configureDynamic(sft, this, ecql, hints, dedupe)
      ScanConfig(ranges, FullColumnFamily, Seq(iter), BinAggregatingIterator.kvsToFeatures(), None, duplicates = false)
    } else if (hints.isArrowQuery) {
      val (iter, reduce) = ArrowIterator.configure(sft, this, ds.stats, filter.filter, ecql, hints, dedupe)
      ScanConfig(ranges, FullColumnFamily, Seq(iter), ArrowIterator.kvsToFeatures(), Some(reduce), duplicates = false)
    } else if (hints.isDensityQuery) {
      val iter = KryoLazyDensityIterator.configure(sft, this, ecql, hints, dedupe)
      ScanConfig(ranges, FullColumnFamily, Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), None, duplicates = false)
    } else if (hints.isStatsQuery) {
      val iter = KryoLazyStatsIterator.configure(sft, this, ecql, hints, dedupe)
      val reduce = Some(KryoLazyStatsUtils.reduceFeatures(sft, hints)(_))
      ScanConfig(ranges, FullColumnFamily, Seq(iter), KryoLazyStatsIterator.kvsToFeatures(), reduce, duplicates = false)
    } else if (hints.isMapAggregatingQuery) {
      val iter = KryoLazyMapAggregatingIterator.configure(sft, this, ecql, hints, dedupe)
      val reduce = Some(KryoLazyMapAggregatingIterator.reduceMapAggregationFeatures(hints)(_))
      ScanConfig(ranges, FullColumnFamily, Seq(iter), entriesToFeatures(sft, hints.getReturnSft), reduce, duplicates = false)
    } else {
      val iter = KryoLazyFilterTransformIterator.configure(sft, this, ecql, hints).toSeq
      ScanConfig(ranges, FullColumnFamily, iter, entriesToFeatures(sft, hints.getReturnSft), None, dedupe)
    }

    // note: bin col family has appropriate visibility for attribute level vis and doesn't need the extra iter
    if (isPrecomputedBins || sft.getVisibilityLevel != VisibilityLevel.Attribute) { config } else {
      // switch to the attribute col family and add the attribute iterator
      val visibility = KryoVisibilityRowEncoder.configure(sft)
      config.copy(columnFamily = AttributeColumnFamily, iterators = config.iterators :+ visibility)
    }
  }
}

object AccumuloIndexAdapter {
  case class ScanConfig(ranges: Seq[Range],
                        columnFamily: Text,
                        iterators: Seq[IteratorSetting],
                        entriesToFeatures: (Entry[Key, Value]) => SimpleFeature,
                        reduce: Option[(CloseableIterator[SimpleFeature]) => CloseableIterator[SimpleFeature]],
                        duplicates: Boolean)
}