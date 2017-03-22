/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{KeyOnlyFilter, Filter => HBaseFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.geotools.factory.Hints
import org.locationtech.geomesa.hbase._
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex.ScanConfig
import org.opengis.feature.simple.SimpleFeature
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.index.index.{ClientSideFiltering, IndexAdapter}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

object HBaseFeatureIndex extends HBaseIndexManagerType {

  // note: keep in priority order for running full table scans
  override val AllIndices: Seq[HBaseFeatureIndex] =
    Seq(HBaseZ3Index, HBaseXZ3Index, HBaseZ2Index, HBaseXZ2Index, HBaseIdIndex, HBaseAttributeIndex, HBaseAttributeDateIndex)

  override val CurrentIndices: Seq[HBaseFeatureIndex] =
    Seq(HBaseZ3Index, HBaseXZ3Index, HBaseZ2Index, HBaseXZ2Index, HBaseIdIndex, HBaseAttributeIndex)

  override def indices(sft: SimpleFeatureType, mode: IndexMode): Seq[HBaseFeatureIndex] =
    super.indices(sft, mode).asInstanceOf[Seq[HBaseFeatureIndex]]
  override def index(identifier: String): HBaseFeatureIndex =
    super.index(identifier).asInstanceOf[HBaseFeatureIndex]

  val DataColumnFamily = Bytes.toBytes("d")
  val DataColumnFamilyDescriptor = new HColumnDescriptor(DataColumnFamily)

  val DataColumnQualifier = Bytes.toBytes("d")
  val DataColumnQualifierDescriptor = new HColumnDescriptor(DataColumnQualifier)

  case class ScanConfig(hbaseFilters: Seq[HBaseFilter],
                        entriesToFeatures: Iterator[Result] => Iterator[SimpleFeature])

}

trait HBaseFeatureIndex extends HBaseFeatureIndexType
    with IndexAdapter[HBaseDataStore, HBaseFeature, Mutation, Query] with ClientSideFiltering[Result] {

  import HBaseFeatureIndex.{DataColumnFamily, DataColumnQualifier}

  override def configure(sft: SimpleFeatureType, ds: HBaseDataStore): Unit = {
    super.configure(sft, ds)
    val name = TableName.valueOf(getTableName(sft.getTypeName, ds))
    val admin = ds.connection.getAdmin
    try {
      if (!admin.tableExists(name)) {
        val descriptor = new HTableDescriptor(name)
        descriptor.addFamily(HBaseFeatureIndex.DataColumnFamilyDescriptor)
        admin.createTable(descriptor, getSplits(sft).toArray)
      }
    } finally {
      admin.close()
    }
  }

  override def delete(sft: SimpleFeatureType, ds: HBaseDataStore, shared: Boolean): Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    import scala.collection.JavaConversions._

    if (shared) {
      val table = ds.connection.getTable(TableName.valueOf(getTableName(sft.getTypeName, ds)))
      try {
        val scan = table.getScanner(new Scan().setRowPrefixFilter(sft.getTableSharingBytes).setFilter(new KeyOnlyFilter))
        try {
          scan.iterator.grouped(10000).foreach { result =>
            val deletes = result.map(r => new Delete(r.getRow))
            table.delete(deletes)
          }
        } finally {
          scan.close()
        }
      } finally {
        table.close()
      }
    } else {
      val table = TableName.valueOf(getTableName(sft.getTypeName, ds))
      val admin = ds.connection.getAdmin
      try {
        admin.disableTable(table)
        admin.deleteTable(table)
      } finally {
        admin.close()
      }
    }
  }

  override protected def createInsert(row: Array[Byte], feature: HBaseFeature): Mutation =
    new Put(row).addImmutable(feature.fullValue.cf, feature.fullValue.cq, feature.fullValue.value)

  override protected def createDelete(row: Array[Byte], feature: HBaseFeature): Mutation =
    new Delete(row).addFamily(feature.fullValue.cf)

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: HBaseDataStore,
                                  filter: HBaseFilterStrategyType,
                                  hints: Hints,
                                  ranges: Seq[Query],
                                  ecql: Option[Filter]): HBaseQueryPlanType = {
    if (ranges.isEmpty) { EmptyPlan(filter) } else {
      val table = TableName.valueOf(getTableName(sft.getTypeName, ds))
      val dedupe = hasDuplicates(sft, filter.primary)
      val ScanConfig(hbaseFilters, toFeatures) = scanConfig(sft, filter, hints, ecql, dedupe)

      if (ranges.head.isInstanceOf[Get]) {
        GetPlan(filter, table, ranges.asInstanceOf[Seq[Get]], hbaseFilters, toFeatures)
      } else {
        // we want to ensure some parallelism in our batch scanning
        // as not all scans will take the same amount of time, we want to have multiple per-thread
        // since scans are executed by a thread pool, that should balance the work and keep all threads occupied
        val scansPerThread = 3
        val scans = ranges.asInstanceOf[Seq[Scan]]
        val minScans = if (ds.config.queryThreads == 1) { 1 } else { ds.config.queryThreads * scansPerThread }
        if (scans.length >= minScans) {
          ScanPlan(filter, table, scans, hbaseFilters, toFeatures)
        } else {
          // split up the scans so that we get some parallelism
          val multiplier = math.ceil(minScans.toDouble / scans.length).toInt
          val splitScans = scans.flatMap { scan =>
            val splits = IndexAdapter.splitRange(scan.getStartRow, scan.getStopRow, multiplier)
            splits.map { case (start, stop) => new Scan(scan).setStartRow(start).setStopRow(stop) }
          }
          ScanPlan(filter, table, splitScans, hbaseFilters, toFeatures)
        }
      }
    }
  }

  override protected def range(start: Array[Byte], end: Array[Byte]): Query =
    new Scan(start, end).addColumn(DataColumnFamily, DataColumnQualifier)

  override protected def rangeExact(row: Array[Byte]): Query =
    new Get(row).addColumn(DataColumnFamily, DataColumnQualifier)

  override def rowAndValue(result: Result): RowAndValue = {
    val cell = result.rawCells()(0)
    RowAndValue(cell.getRowArray, cell.getRowOffset, cell.getRowLength,
      cell.getValueArray, cell.getValueOffset, cell.getValueLength)
  }

  protected def hasDuplicates(sft: SimpleFeatureType, filter: Option[Filter]): Boolean = false


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
  protected def scanConfig(sft: SimpleFeatureType,
                           filter: HBaseFilterStrategyType,
                           hints: Hints,
                           ecql: Option[Filter],
                           dedupe: Boolean): ScanConfig = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    /** This function is used to implement custom client filters for HBase **/
      val toFeatures = resultsToFeatures(sft, ecql, hints.getTransform)
      ScanConfig(Nil, toFeatures)

  }
}
