/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
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
import org.geotools.filter.identity.FeatureIdImpl
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.hbase.coprocessor.aggregators._
import org.locationtech.geomesa.hbase.coprocessor.utils.CoprocessorConfig
import org.locationtech.geomesa.hbase.data.{EmptyPlan, HBaseDataStore, HBaseFeature, HBaseQueryPlan}
import org.locationtech.geomesa.hbase.filters.JSimpleFeatureFilter
import org.locationtech.geomesa.hbase.index.HBaseIndexAdapter.ScanConfig
import org.locationtech.geomesa.hbase.{HBaseFeatureIndexType, HBaseFilterStrategyType, HBaseQueryPlanType}
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.index.index.{ClientSideFiltering, IndexAdapter}
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait HBaseIndexAdapter extends HBaseFeatureIndexType
    with IndexAdapter[HBaseDataStore, HBaseFeature, Mutation, Query, ScanConfig] with ClientSideFiltering[Result] {

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

  override protected def createRange(start: Array[Byte], end: Array[Byte]): Query =
    new Scan(start, end).addColumn(DataColumnFamily, DataColumnQualifier)

  override protected def createRange(row: Array[Byte]): Query =
    new Get(row).addColumn(DataColumnFamily, DataColumnQualifier)

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: HBaseDataStore,
                                  filter: HBaseFilterStrategyType,
                                  config: ScanConfig): HBaseQueryPlanType = {
    if (config.ranges.isEmpty) { EmptyPlan(filter) } else {
      val table = TableName.valueOf(getTableName(sft.getTypeName, ds))
      val ScanConfig(ranges, hbaseFilters, coprocessor, toFeatures) = config
      buildPlatformScanPlan(ds, sft, filter, ranges, table, hbaseFilters, coprocessor, toFeatures)
    }
  }

  override protected def scanConfig(sft: SimpleFeatureType,
                                    ds: HBaseDataStore,
                                    filter: HBaseFilterStrategyType,
                                    ranges: Seq[Query],
                                    ecql: Option[Filter],
                                    hints: Hints): ScanConfig = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val transform: Option[(String, SimpleFeatureType)] = hints.getTransform

    if (!ds.config.remoteFilter) {
      // everything is done client side
      ScanConfig(ranges, Seq.empty, None, resultsToFeatures(sft, ecql, transform))
    } else {

      val (remoteTdefArg, returnSchema) = transform.getOrElse(("", sft))

      // TODO not actually used for coprocessors
      val toFeatures = resultsToFeatures(sft, returnSchema)

      val coprocessorConfig = if (hints.isDensityQuery) {
        val options = HBaseDensityAggregator.configure(sft, this, ecql, hints)
        Some(CoprocessorConfig(options, HBaseDensityAggregator.bytesToFeatures))
      } else if (hints.isArrowQuery) {
        val (options, reduce) = HBaseArrowAggregator.configure(sft, this, ds.stats, filter.filter, ecql, hints)
        Some(CoprocessorConfig(options, HBaseArrowAggregator.bytesToFeatures, reduce))
      } else if (hints.isStatsQuery) {
        val options = HBaseStatsAggregator.configure(sft, filter.index, ecql, hints)
        val reduce = StatsScan.reduceFeatures(returnSchema, hints)(_)
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

      ScanConfig(ranges, filters, coprocessorConfig, toFeatures)
    }
  }

  protected def hasDuplicates(sft: SimpleFeatureType, filter: Option[Filter]): Boolean = false

  protected def buildPlatformScanPlan(ds: HBaseDataStore,
                                      sft: SimpleFeatureType,
                                      filter: HBaseFilterStrategyType,
                                      ranges: Seq[Query],
                                      table: TableName,
                                      hbaseFilters: Seq[(Int, HFilter)],
                                      coprocessor: Option[CoprocessorConfig],
                                      toFeatures: (Iterator[Result]) => Iterator[SimpleFeature]): HBaseQueryPlan

  /**
    * Turns hbase results into simple features
    *
    * @param sft simple feature type
    * @param returnSft return simple feature type (transform, etc)
    * @return
    */
  private [hbase] def resultsToFeatures(sft: SimpleFeatureType,
                                        returnSft: SimpleFeatureType): Iterator[Result] => Iterator[SimpleFeature] = {
    // Perform a projecting decode of the simple feature
    val getId = getIdFromRow(sft)
    val deserializer = KryoFeatureSerializer(returnSft, SerializationOptions.withoutId)
    resultsToFeatures(deserializer, getId)
  }

  private def resultsToFeatures(deserializer: KryoFeatureSerializer,
                                getId: (Array[Byte], Int, Int, SimpleFeature) => String)
                               (results: Iterator[Result]): Iterator[SimpleFeature] = {
    results.map { result =>
      val RowAndValue(row, rowOffset, rowLength, value, valueOffset, valueLength) = rowAndValue(result)
      val sf = deserializer.deserialize(value, valueOffset, valueLength)
      sf.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row, rowOffset, rowLength, sf))
      sf
    }
  }
}

object HBaseIndexAdapter {
  case class ScanConfig(ranges: Seq[Query],
                        filters: Seq[(Int, HFilter)],
                        coprocessor: Option[CoprocessorConfig],
                        entriesToFeatures: Iterator[Result] => Iterator[SimpleFeature])
}