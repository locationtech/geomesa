/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{Filter => HFilter}
import org.geotools.factory.Hints
import org.locationtech.geomesa.hbase.coprocessor.aggregators._
import org.locationtech.geomesa.hbase.coprocessor.utils.CoprocessorConfig
import org.locationtech.geomesa.hbase.data.{EmptyPlan, HBaseDataStore, HBaseFeature, HBaseQueryPlan}
import org.locationtech.geomesa.hbase.filters.JSimpleFeatureFilter
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex.ScanConfig
import org.locationtech.geomesa.hbase.{HBaseFeatureIndexType, HBaseFilterStrategyType, HBaseQueryPlanType}
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.index.index.{ClientSideFiltering, IndexAdapter}
import org.locationtech.geomesa.index.utils.KryoLazyStatsUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait HBaseIndexAdapter[K] extends HBaseFeatureIndexType
    with IndexAdapter[HBaseDataStore, HBaseFeature, Mutation, Query, K] with ClientSideFiltering[Result] {

  import HBaseFeatureIndex.{DataColumnFamily, DataColumnQualifier}

  override def rowAndValue(result: Result): RowAndValue = {
    val cell = result.rawCells()(0)
    RowAndValue(cell.getRowArray, cell.getRowOffset, cell.getRowLength,
      cell.getValueArray, cell.getValueOffset, cell.getValueLength)
  }

  override protected def createInsert(row: Array[Byte], feature: HBaseFeature): Mutation = {
    val put = new Put(row).addImmutable(feature.fullValue.cf, feature.fullValue.cq, feature.fullValue.value)
    feature.fullValue.vis.foreach(put.setCellVisibility)
    put
  }

  override protected def createDelete(row: Array[Byte], feature: HBaseFeature): Mutation = {
    val del = new Delete(row).addFamily(feature.fullValue.cf)
    feature.fullValue.vis.foreach(del.setCellVisibility)
    del
  }

  override protected def range(start: Array[Byte], end: Array[Byte]): Query =
    new Scan(start, end).addColumn(DataColumnFamily, DataColumnQualifier)

  override protected def rangeExact(row: Array[Byte]): Query =
    new Get(row).addColumn(DataColumnFamily, DataColumnQualifier)

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: HBaseDataStore,
                                  filter: HBaseFilterStrategyType,
                                  indexValues: Option[K],
                                  ranges: Seq[Query],
                                  ecql: Option[Filter],
                                  hints: Hints): HBaseQueryPlanType = {
    if (ranges.isEmpty) { EmptyPlan(filter) } else {
      val table = TableName.valueOf(getTableName(sft.getTypeName, ds))
      val dedupe = hasDuplicates(sft, filter.primary)
      val ScanConfig(hbaseFilters, coprocessor, toFeatures) = scanConfig(ds, sft, filter, indexValues, ecql, hints, dedupe)
      buildPlatformScanPlan(ds, sft, filter, hints, ranges, table, hbaseFilters, coprocessor, toFeatures)
    }
  }

  /**
    * Sets up everything needed to execute the scan - iterators, column families, deserialization, etc
    *
    * @param ds     data store
    * @param sft    simple feature type
    * @param filter hbase filter strategy type
    * @param hints  query hints
    * @param ecql   secondary filter being applied, if any
    * @param dedupe scan may have duplicate results or not
    * @return
    */
  protected def scanConfig(ds: HBaseDataStore,
                           sft: SimpleFeatureType,
                           filter: HBaseFilterStrategyType,
                           indexValues: Option[K],
                           ecql: Option[Filter],
                           hints: Hints,
                           dedupe: Boolean): ScanConfig = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val transform: Option[(String, SimpleFeatureType)] = hints.getTransform

    if (!ds.config.remoteFilter) {
      // everything is done client side
      ScanConfig(Seq.empty, None, resultsToFeatures(sft, ecql, transform))
    } else {

      val (remoteTdefArg, returnSchema) = transform.getOrElse(("", sft))

      val additionalFilters = createPushDownFilters(ds, sft, filter, indexValues, transform)
      // TODO not actually used for coprocessors
      val toFeatures = resultsToFeatures(returnSchema, None, None)

      val coprocessorConfig = if (hints.isDensityQuery) {
        val options = HBaseDensityAggregator.configure(sft, this, ecql, hints)
        Some(CoprocessorConfig(options, HBaseDensityAggregator.bytesToFeatures))
      } else if (hints.isArrowQuery) {
        val (options, reduce) = HBaseArrowAggregator.configure(sft, this, ds.stats, filter.filter, ecql, hints)
        Some(CoprocessorConfig(options, HBaseArrowAggregator.bytesToFeatures, reduce))
      } else if (hints.isStatsQuery) {
        val options = HBaseStatsAggregator.configure(sft, filter.index, ecql, hints)
        val reduce = KryoLazyStatsUtils.reduceFeatures(returnSchema, hints)(_)
        Some(CoprocessorConfig(options, HBaseStatsAggregator.bytesToFeatures, reduce))
      } else if (hints.isBinQuery) {
        val options = HBaseBinAggregator.configure(sft, filter.index, ecql, hints)
        Some(CoprocessorConfig(options, HBaseBinAggregator.bytesToFeatures))
      } else {
        None
      }

      // if there is a coprocessorConfig it handles filter/transform
      val filters = if (coprocessorConfig.isDefined || (ecql.isEmpty && transform.isEmpty)) {
        Seq.empty
      } else {
        val remoteCQLFilter: Filter = ecql.getOrElse(Filter.INCLUDE)
        val encodedSft = SimpleFeatureTypes.encodeType(returnSchema)
        val filter = new JSimpleFeatureFilter(sft, remoteCQLFilter, remoteTdefArg, encodedSft)
        Seq((JSimpleFeatureFilter.Priority, filter))
      }

      ScanConfig(filters ++ additionalFilters, coprocessorConfig, toFeatures)
    }
  }

  protected def hasDuplicates(sft: SimpleFeatureType, filter: Option[Filter]): Boolean = false

  // default implementation does nothing, override in subclasses
  protected def createPushDownFilters(ds: HBaseDataStore,
                                      sft: SimpleFeatureType,
                                      filter: HBaseFilterStrategyType,
                                      indexValues: Option[K],
                                      transform: Option[(String, SimpleFeatureType)]): Seq[(Int, HFilter)] = Seq.empty

  protected def buildPlatformScanPlan(ds: HBaseDataStore,
                                      sft: SimpleFeatureType,
                                      filter: HBaseFilterStrategyType,
                                      hints: Hints,
                                      ranges: Seq[Query],
                                      table: TableName,
                                      hbaseFilters: Seq[(Int, HFilter)],
                                      coprocessor: Option[CoprocessorConfig],
                                      toFeatures: (Iterator[Result]) => Iterator[SimpleFeature]): HBaseQueryPlan
}
