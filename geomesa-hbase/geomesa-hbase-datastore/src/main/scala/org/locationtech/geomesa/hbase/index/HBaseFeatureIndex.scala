/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.hbase.index

import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{KeyOnlyFilter, Filter => HBaseFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.geotools.factory.Hints
import org.locationtech.geomesa.hbase._
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex.ScanConfig
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.index.index.{ClientSideFiltering, IndexAdapter}
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
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

  val DataColumnFamily: Array[Byte] = Bytes.toBytes("d")
  val DataColumnFamilyDescriptor = new HColumnDescriptor(DataColumnFamily)

  val DataColumnQualifier: Array[Byte] = Bytes.toBytes("d")
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
        val scan = new Scan()
          .setRowPrefixFilter(sft.getTableSharingBytes)
          .setFilter(new KeyOnlyFilter)
        ds.applySecurity(scan)
        val scanner = table.getScanner(scan)
        try {
          scanner.iterator.grouped(10000).foreach { result =>
            // TODO set delete visibilities
            val deletes = result.map(r => new Delete(r.getRow))
            table.delete(deletes)
          }
        } finally {
          scanner.close()
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

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: HBaseDataStore,
                                  filter: HBaseFilterStrategyType,
                                  hints: Hints,
                                  ranges: Seq[Query],
                                  ecql: Option[Filter]): HBaseQueryPlanType = {
    if (ranges.isEmpty) EmptyPlan(filter)
    else {
      val table = TableName.valueOf(getTableName(sft.getTypeName, ds))
      val dedupe = hasDuplicates(sft, filter.primary)
      val ScanConfig(hbaseFilters, toFeatures) = scanConfig(sft, filter, hints, ecql, dedupe)

      if (ranges.head.isInstanceOf[Get]) {
        // TODO: when do we have a GET plan?  only on IDs?
        GetPlan(filter, table, ranges.asInstanceOf[Seq[Get]], hbaseFilters, toFeatures)
      } else {
        buildPlatformScanPlan(ds, filter, ranges, table, hbaseFilters, toFeatures)
      }
    }
  }

  def buildPlatformScanPlan(ds: HBaseDataStore, filter: HBaseFilterStrategyType, ranges: Seq[Query], table: TableName, hbaseFilters: Seq[HBaseFilter], toFeatures: (Iterator[Result]) => Iterator[SimpleFeature]): HBaseQueryPlan

  // default implementation does nothing, override in subclasses
  def configurePushDownFilters(config: ScanConfig,
                               ecql: Option[Filter],
                               transform: Option[(String, SimpleFeatureType)],
                               sft: SimpleFeatureType): ScanConfig = {
    val remoteFilters =
      if (ecql.isDefined || transform.isDefined) {
        val (tform, tSchema) = transform.getOrElse(("", null))
        val tSchemaString = Option(tSchema).map(SimpleFeatureTypes.encodeType(_)).getOrElse("")
        Seq(new JSimpleFeatureFilter(sft, ecql.getOrElse(Filter.INCLUDE), tform, tSchemaString))
      } else {
        Seq.empty
      }
    config.copy(hbaseFilters = config.hbaseFilters ++ remoteFilters)
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
    val transform = hints.getTransform // will eventually be used to support remote transforms
    val schema = transform.map(_._2).getOrElse(sft) // schema coming back from server

    // ECQL is now pushed down in HBase so don't need to apply it client side
    val toFeatures = resultsToFeatures(schema, None, None)

    configurePushDownFilters(ScanConfig(Nil, toFeatures), ecql, transform, sft)
  }
}
