/*
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.bigtable.data

import com.google.cloud.bigtable.hbase.BigtableExtendedScan
import com.google.common.collect.Lists
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{Filter, MultiRowRangeFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, TableName}
import org.geotools.factory.Hints
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.HBaseDataStoreConfig
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex.ScanConfig
import org.locationtech.geomesa.hbase.index._
import org.locationtech.geomesa.hbase.utils.HBaseBatchScan
import org.locationtech.geomesa.hbase.{HBaseFilterStrategyType, HBaseIndexManagerType}
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditProvider, AuditWriter}
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter

class BigtableDataStoreFactory extends HBaseDataStoreFactory {

  override def getDisplayName: String = BigtableDataStoreFactory.DisplayName
  override def getDescription: String = BigtableDataStoreFactory.Description

  override def buildDataStore(catalog: String,
                              generateStats: Boolean,
                              audit: Option[(AuditWriter, AuditProvider, String)],
                              queryThreads: Int,
                              queryTimeout: Option[Long],
                              looseBBox: Boolean,
                              caching: Boolean,
                              authsProvider: Option[AuthorizationsProvider],
                              connection: Connection): BigtableDataStore = {
    val config = HBaseDataStoreConfig(
      catalog,
      generateStats,
      audit,
      queryThreads,
      queryTimeout,
      looseBBox,
      caching,
      authsProvider
    )
    new BigtableDataStore(connection, config)
  }

  override def canProcess(params: java.util.Map[java.lang.String,java.io.Serializable]): Boolean = {
    params.containsKey(HBaseDataStoreParams.BigTableNameParam.key) &&
        Option(HBaseConfiguration.create().get(HBaseDataStoreFactory.BigTableParamCheck)).exists(_.trim.nonEmpty)
  }
}

object BigtableDataStoreFactory {
  val DisplayName = "Google Bigtable (GeoMesa)"
  val Description = "Google Bigtable\u2122 distributed key/value store"
}

class BigtableDataStore(connection: Connection, config: HBaseDataStoreConfig) extends HBaseDataStore(connection, config) {
  override def manager: HBaseIndexManagerType = BigtableFeatureIndex
}

object BigtableFeatureIndex extends HBaseIndexManagerType {

  // note: keep in priority order for running full table scans
  override val AllIndices: Seq[HBaseFeatureIndex] =
    Seq(BigtableZ3Index, HBaseXZ3Index, BigtableZ2Index, HBaseXZ2Index, HBaseIdIndex, HBaseAttributeIndex, HBaseAttributeDateIndex)

  override val CurrentIndices: Seq[HBaseFeatureIndex] =
    Seq(BigtableZ3Index, HBaseXZ3Index, BigtableZ2Index, HBaseXZ2Index, HBaseIdIndex, HBaseAttributeIndex)

  override def indices(sft: SimpleFeatureType, mode: IndexMode): Seq[HBaseFeatureIndex] =
    super.indices(sft, mode).asInstanceOf[Seq[HBaseFeatureIndex]]
  override def index(identifier: String): HBaseFeatureIndex =
    super.index(identifier).asInstanceOf[HBaseFeatureIndex]

  val DataColumnFamily: Array[Byte] = Bytes.toBytes("d")
  val DataColumnFamilyDescriptor = new HColumnDescriptor(DataColumnFamily)

  val DataColumnQualifier: Array[Byte] = Bytes.toBytes("d")
  val DataColumnQualifierDescriptor = new HColumnDescriptor(DataColumnQualifier)

}

trait BigtablePlatform extends HBasePlatform {
  override def buildPlatformScanPlan(filter: HBaseFilterStrategyType,
                                     ranges: Seq[Query],
                                     table: TableName,
                                     hbaseFilters: Seq[Filter],
                                     toFeatures: (Iterator[Result]) => Iterator[SimpleFeature]): HBaseQueryPlan = {
    BigtableExtendedScanPlan(filter, table, ranges.asInstanceOf[Seq[Scan]], Nil, toFeatures)
  }
}

case object BigtableZ2Index extends HBaseLikeZ2Index with BigtablePlatform

case object BigtableZ3Index extends HBaseLikeZ3Index with BigtablePlatform {

  /**
    * Sets up everything needed to execute the scan - iterators, column families, deserialization, etc
    *
    * @param sft    simple feature type
    * @param filter hbase filter strategy type
    * @param hints  query hints
    * @param ecql   secondary filter being applied, if any
    * @param dedupe scan may have duplicate results or not
    * @return
    */
  override def scanConfig(sft: SimpleFeatureType,
                           filter: HBaseFilterStrategyType,
                           hints: Hints,
                           ecql: Option[org.opengis.filter.Filter],
                           dedupe: Boolean): ScanConfig = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    /** This function is used to implement custom client filters for HBase **/
    val transform = hints.getTransform // will eventually be used to support remote transforms
    val feature = sft // will eventually be used to support remote transforms

    // ECQL is now pushed down in HBase so don't need to apply it client side
    // However, the transform is not yet pushed down
    val toFeatures = resultsToFeatures(feature, ecql, transform)

    configurePushDownFilters(ScanConfig(Nil, toFeatures), ecql, sft)
  }

  override def configurePushDownFilters(config: HBaseFeatureIndex.ScanConfig,
                                        ecql: Option[filter.Filter],
                                        sft: SimpleFeatureType): HBaseFeatureIndex.ScanConfig = {
    config
  }
}

case class BigtableExtendedScanPlan(filter: HBaseFilterStrategyType,
                                    table: TableName,
                                    ranges: Seq[Scan],
                                    remoteFilters: Seq[Filter] = Nil,
                                    resultsToFeatures: Iterator[Result] => Iterator[SimpleFeature]) extends HBaseQueryPlan {


  /**
    * Runs the query plain against the underlying database, returning the raw entries
    *
    * @param ds data store - provides connection object and metadata
    * @return
    */
  override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = {
    import scala.collection.JavaConversions._
    val rowRanges = Lists.newArrayList[RowRange]()
    ranges.foreach { r =>
      rowRanges.add(new RowRange(r.getStartRow, true, r.getStopRow, false))
    }
    val sortedRowRanges = MultiRowRangeFilter.sortAndMerge(rowRanges)
    val numRanges = sortedRowRanges.size()
    val numThreads = ds.config.queryThreads
    // TODO: parameterize this?
    val rangesPerThread = math.min(100, math.max(1,math.ceil(numRanges/numThreads*2).toInt))
    // TODO: align partitions with region boundaries
    val groupedRanges = Lists.partition(sortedRowRanges, rangesPerThread)

    // group scans into batches to achieve some client side parallelism
    val groupedScans = groupedRanges.map { localRanges =>
      // TODO: FIX
      // currently, this constructor will call sortAndMerge a second time
      // this is unnecessary as we have already sorted and merged above
      val scan = new BigtableExtendedScan

      localRanges.foreach { r => scan.addRange(r.getStartRow, r.getStopRow) }
      scan
    }

    val results = new HBaseBatchScan(ds.connection, table, groupedScans, ds.config.queryThreads, 100000, Nil)
    SelfClosingIterator(resultsToFeatures(results), results.close)
  }
}