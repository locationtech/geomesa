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
import org.locationtech.geomesa.index.iterators.ArrowBatchScan
import org.locationtech.geomesa.index.utils.KryoLazyStatsUtils
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait AccumuloIndexAdapter extends IndexAdapter[AccumuloDataStore, AccumuloFeature, Mutation, Range] {

  this: AccumuloFeatureIndex =>

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

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
                                  hints: Hints,
                                  ranges: Seq[Range],
                                  ecql: Option[Filter]): QueryPlan[AccumuloDataStore, AccumuloFeature, Mutation] = {
    if (ranges.isEmpty) { EmptyPlan(filter) } else {
      val table = getTableName(sft.getTypeName, ds)
      val numThreads = queryThreads(ds)
      val dedupe = hasDuplicates(sft, filter.primary)
      val ScanConfig(iters, cf, eToF, reduce) = scanConfig(sft, ds, filter, hints, ecql, dedupe)
      BatchScanPlan(filter, table, ranges, iters, Seq(cf), eToF, reduce, numThreads, dedupe)
    }
  }

  protected def hasDuplicates(sft: SimpleFeatureType, filter: Option[Filter]): Boolean = false

  protected def queryThreads(ds: AccumuloDataStore): Int = ds.config.queryThreads

  /**
    * Sets up everything needed to execute the scan - iterators, column families, deserialization, etc
    *
    * @param sft simple feature type
    * @param ds data store
    * @param filter filter strategy
    * @param hints query hints
    * @param ecql secondary filter being applied, if any
    * @param dedupe scan may have duplicate results or not
    * @return
    */
  protected def scanConfig(sft: SimpleFeatureType,
                           ds: AccumuloDataStore,
                           filter: FilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation],
                           hints: Hints,
                           ecql: Option[Filter],
                           dedupe: Boolean): ScanConfig = {
    import AccumuloFeatureIndex.{AttributeColumnFamily, BinColumnFamily, FullColumnFamily}
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    // if possible, use the pre-computed values for bin queries
    // can't use if there are non-st filters or if custom fields are requested
    val isPrecomputedBins = hints.isBinQuery && hasPrecomputedBins && ecql.isEmpty &&
        BinAggregatingIterator.canUsePrecomputedBins(sft, hints)

    val config = if (isPrecomputedBins) {
      val iter = BinAggregatingIterator.configurePrecomputed(sft, this, ecql, hints, dedupe)
      ScanConfig(Seq(iter), BinColumnFamily, BinAggregatingIterator.kvsToFeatures(), None)
    } else if (hints.isBinQuery) {
      val iter = BinAggregatingIterator.configureDynamic(sft, this, ecql, hints, dedupe)
      ScanConfig(Seq(iter), FullColumnFamily, BinAggregatingIterator.kvsToFeatures(), None)
    } else if (hints.isArrowQuery) {
      val dictionaryFields = hints.getArrowDictionaryFields
      val providedDictionaries = hints.getArrowDictionaryEncodedValues
      if (hints.getArrowSort.isDefined || hints.isArrowComputeDictionaries ||
          dictionaryFields.forall(providedDictionaries.contains)) {
        val dictionaries = ArrowBatchScan.createDictionaries(ds, sft, filter.filter, dictionaryFields, providedDictionaries)
        val iter = ArrowBatchIterator.configure(sft, this, ecql, dictionaries, hints, dedupe)
        val reduce = Some(ArrowBatchScan.reduceFeatures(hints.getTransformSchema.getOrElse(sft), hints, dictionaries))
        ScanConfig(Seq(iter), FullColumnFamily, ArrowBatchIterator.kvsToFeatures(), reduce)
      } else {
        val iter = ArrowFileIterator.configure(sft, this, ecql, dictionaryFields, hints, dedupe)
        ScanConfig(Seq(iter), FullColumnFamily, ArrowFileIterator.kvsToFeatures(), None)
      }
    } else if (hints.isDensityQuery) {
      val iter = KryoLazyDensityIterator.configure(sft, this, ecql, hints, dedupe)
      ScanConfig(Seq(iter), FullColumnFamily, KryoLazyDensityIterator.kvsToFeatures(), None)
    } else if (hints.isStatsIteratorQuery) {
      val iter = KryoLazyStatsIterator.configure(sft, this, ecql, hints, dedupe)
      val reduce = Some(KryoLazyStatsUtils.reduceFeatures(sft, hints)(_))
      ScanConfig(Seq(iter), FullColumnFamily, KryoLazyStatsIterator.kvsToFeatures(sft), reduce)
    } else if (hints.isMapAggregatingQuery) {
      val iter = KryoLazyMapAggregatingIterator.configure(sft, this, ecql, hints, dedupe)
      val reduce = Some(KryoLazyMapAggregatingIterator.reduceMapAggregationFeatures(hints)(_))
      ScanConfig(Seq(iter), FullColumnFamily, entriesToFeatures(sft, hints.getReturnSft), reduce)
    } else {
      val iter = KryoLazyFilterTransformIterator.configure(sft, this, ecql, hints).toSeq
      ScanConfig(iter, FullColumnFamily, entriesToFeatures(sft, hints.getReturnSft), None)
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
  case class ScanConfig(iterators: Seq[IteratorSetting],
                        columnFamily: Text,
                        entriesToFeatures: (Entry[Key, Value]) => SimpleFeature,
                        reduce: Option[(CloseableIterator[SimpleFeature]) => CloseableIterator[SimpleFeature]])
}