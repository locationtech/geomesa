/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.index.iterators.StatsScan
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

  override protected def createRange(start: Array[Byte], end: Array[Byte]): Range = {
    // index api defines empty start/end for open-ended range - in accumulo, it's indicated with null
    val startKey = if (start.length == 0) { null } else { new Key(new Text(start)) }
    val endKey = if (end.length == 0) { null } else { new Key(new Text(end)) }
    // index api defines start row inclusive, end row exclusive
    new Range(startKey, true, endKey, false)
  }

  override protected def createRange(row: Array[Byte]): Range = Range.exact(new Text(row))

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: AccumuloDataStore,
                                  filter: FilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation],
                                  config: ScanConfig): QueryPlan[AccumuloDataStore, AccumuloFeature, Mutation] = {
    if (config.ranges.isEmpty) { EmptyPlan(filter) } else {
      val numThreads = queryThreads(ds)
      val tables = getTablesForQuery(sft, ds, filter.filter)
      val ScanConfig(ranges, cf, iters, eToF, reduce, dedupe) = config
      BatchScanPlan(filter, tables, ranges, iters, Seq(cf), eToF, reduce, numThreads, dedupe)
    }
  }

  override protected def scanConfig(sft: SimpleFeatureType,
                                    ds: AccumuloDataStore,
                                    filter: FilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation],
                                    ranges: Seq[Range],
                                    ecql: Option[Filter],
                                    hints: Hints): ScanConfig = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val dedupe = hasDuplicates(sft, filter.primary)

    // if possible, use the pre-computed values for bin queries
    // can't use if there are non-st filters or if custom fields are requested
    val isPrecomputedBins = hints.isBinQuery && hasPrecomputedBins && ecql.isEmpty &&
        BinAggregatingIterator.canUsePrecomputedBins(sft, hints)

    // note: column groups aren't supported for attribute level vis
    val isAttributeLevelVis = sft.getVisibilityLevel == VisibilityLevel.Attribute

    val (colFamily, schema) = hints.getTransformDefinition match {
      case _ if isPrecomputedBins   => (AccumuloColumnGroups.BinColumnFamily, sft)
      case _ if isAttributeLevelVis => (AccumuloColumnGroups.AttributeColumnFamily, sft)
      case Some(tdefs)              =>  AccumuloColumnGroups.group(sft, tdefs, ecql)
      case None                     => (AccumuloColumnGroups.default, sft)
    }

    val config = if (isPrecomputedBins) {
      val iter = BinAggregatingIterator.configurePrecomputed(schema, this, ecql, hints, dedupe)
      ScanConfig(ranges, colFamily, Seq(iter), BinAggregatingIterator.kvsToFeatures(), None, duplicates = false)
    } else if (hints.isBinQuery) {
      val iter = BinAggregatingIterator.configureDynamic(schema, this, ecql, hints, dedupe)
      ScanConfig(ranges, colFamily, Seq(iter), BinAggregatingIterator.kvsToFeatures(), None, duplicates = false)
    } else if (hints.isArrowQuery) {
      val (iter, reduce) = ArrowIterator.configure(schema, this, ds.stats, filter.filter, ecql, hints, dedupe)
      ScanConfig(ranges, colFamily, Seq(iter), ArrowIterator.kvsToFeatures(), Some(reduce), duplicates = false)
    } else if (hints.isDensityQuery) {
      val iter = KryoLazyDensityIterator.configure(schema, this, ecql, hints, dedupe)
      ScanConfig(ranges, colFamily, Seq(iter), KryoLazyDensityIterator.kvsToFeatures(), None, duplicates = false)
    } else if (hints.isStatsQuery) {
      val iter = KryoLazyStatsIterator.configure(schema, this, ecql, hints, dedupe)
      val reduce = Some(StatsScan.reduceFeatures(schema, hints)(_))
      ScanConfig(ranges, colFamily, Seq(iter), KryoLazyStatsIterator.kvsToFeatures(), reduce, duplicates = false)
    } else if (hints.isMapAggregatingQuery) {
      val iter = KryoLazyMapAggregatingIterator.configure(schema, this, ecql, hints, dedupe)
      val reduce = Some(KryoLazyMapAggregatingIterator.reduceMapAggregationFeatures(hints)(_))
      ScanConfig(ranges, colFamily, Seq(iter), entriesToFeatures(schema, hints.getReturnSft), reduce, duplicates = false)
    } else {
      val iter = KryoLazyFilterTransformIterator.configure(schema, this, ecql, hints).toSeq
      ScanConfig(ranges, colFamily, iter, entriesToFeatures(schema, hints.getReturnSft), None, dedupe)
    }

    // note: bin col family has appropriate visibility for attribute level vis and doesn't need the extra iter
    if (isPrecomputedBins || !isAttributeLevelVis) { config } else {
      // add the attribute iterator
      config.copy(iterators = config.iterators :+ KryoVisibilityRowEncoder.configure(schema))
    }
  }
}

object AccumuloIndexAdapter {
  case class ScanConfig(ranges: Seq[Range],
                        columnFamily: Text,
                        iterators: Seq[IteratorSetting],
                        entriesToFeatures: Entry[Key, Value] => SimpleFeature,
                        reduce: Option[CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature]],
                        duplicates: Boolean)
}
