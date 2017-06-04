/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.apache.hadoop.hbase.filter.{KeyOnlyFilter, Filter => HFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.geotools.factory.Hints
import org.locationtech.geomesa.hbase._
import org.locationtech.geomesa.hbase.coprocessor.{KryoLazyDensityCoprocessor, coprocessorList}
import org.locationtech.geomesa.hbase.data._
import org.locationtech.geomesa.hbase.filters.JSimpleFeatureFilter
import org.locationtech.geomesa.hbase.index.HBaseFeatureIndex.ScanConfig
import org.locationtech.geomesa.index.index.ClientSideFiltering.RowAndValue
import org.locationtech.geomesa.index.index.{ClientSideFiltering, IndexAdapter}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.IndexMode.IndexMode
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.slf4j.LoggerFactory

object HBaseFeatureIndex extends HBaseIndexManagerType {

  // note: keep in priority order for running full table scans
  override val AllIndices: Seq[HBaseFeatureIndex] =
    Seq(HBaseZ3Index, HBaseXZ3Index, HBaseZ2Index, HBaseXZ2Index, HBaseIdIndex, HBaseAttributeIndex,
      HBaseAttributeIndexV2, HBaseAttributeDateIndex)

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

  case class ScanConfig(filters: Seq[(Int, HFilter)],
                        coprocessor: Option[Coprocessor],
                        entriesToFeatures: Iterator[Result] => Iterator[SimpleFeature])
}

trait HBaseFeatureIndex extends HBaseFeatureIndexType
  with IndexAdapter[HBaseDataStore, HBaseFeature, Mutation, Query] with ClientSideFiltering[Result] with LazyLogging {

  import HBaseFeatureIndex.{DataColumnFamily, DataColumnQualifier}

  override def configure(sft: SimpleFeatureType, ds: HBaseDataStore): Unit = {
    super.configure(sft, ds)

    val name = TableName.valueOf(getTableName(sft.getTypeName, ds))
    val admin = ds.connection.getAdmin
    val coproUrl: Option[Path] = ds.config.coprocessorUrl.orElse{
      GeoMesaSystemProperties.SystemProperty("geomesa.hbase.coprocessor.path", null).option.map(new Path(_))
    }

    def addCoprocessors(desc: HTableDescriptor): Unit =
      coprocessorList.foreach(c => addCoprocessor(c, desc))

    def addCoprocessor(clazz: Class[_ <: Coprocessor], desc: HTableDescriptor): Unit = {
      val name = clazz.getCanonicalName
      if (!desc.getCoprocessors.contains(name)) { // should always be true
        coproUrl match {
          // TODO: Warn if the path given is different from paths registered in other coprocessors. This is to warn if another table needs updated.
          case Some(path) => desc.addCoprocessor(name, path, Coprocessor.PRIORITY_USER, null)
          case None       => desc.addCoprocessor(name)
        }
      }
    }

    try {
      if (!admin.tableExists(name)) {
        logger.debug(s"Creating table $name")
        val descriptor = new HTableDescriptor(name)
        descriptor.addFamily(HBaseFeatureIndex.DataColumnFamilyDescriptor)
        Option(admin.getConfiguration.get(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY)) match {
          // if the coprocessors are installed site-wide don't register them in the table descriptor
          case Some(value) =>
            val installedCoprocessors = value.split(":").toSeq
            coprocessorList.foreach { c =>
              if (!installedCoprocessors.contains(c.getCanonicalName)) {
                addCoprocessor(c, descriptor)
              }
            }
          case None => addCoprocessors(descriptor)
        }
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

  override protected def range(start: Array[Byte], end: Array[Byte]): Query =
    new Scan(start, end).addColumn(DataColumnFamily, DataColumnQualifier)

  override protected def rangeExact(row: Array[Byte]): Query =
    new Get(row).addColumn(DataColumnFamily, DataColumnQualifier)

  override def rowAndValue(result: Result): RowAndValue = {
    val cell = result.rawCells()(0)
    RowAndValue(cell.getRowArray, cell.getRowOffset, cell.getRowLength,
      cell.getValueArray, cell.getValueOffset, cell.getValueLength)
  }

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: HBaseDataStore,
                                  filter: HBaseFilterStrategyType,
                                  hints: Hints,
                                  ranges: Seq[Query],
                                  ecql: Option[Filter]): HBaseQueryPlanType = {
    if (ranges.isEmpty) { EmptyPlan(filter) } else {
      val table = TableName.valueOf(getTableName(sft.getTypeName, ds))
      val dedupe = hasDuplicates(sft, filter.primary)
      val ScanConfig(hbaseFilters, coprocessor, toFeatures) = scanConfig(ds, sft, filter, hints, ecql, dedupe)
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
                           hints: Hints,
                           ecql: Option[Filter],
                           dedupe: Boolean): ScanConfig = {

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    val transform: Option[(String, SimpleFeatureType)] = hints.getTransform

    if (!ds.config.remoteFilter) {
      // everything is done client side
      ScanConfig(Seq.empty, None, resultsToFeatures(sft, ecql, transform))
    } else {

      val coprocessor: Option[Coprocessor] = if (hints.isDensityQuery) {
        Some(new KryoLazyDensityCoprocessor)
      } else {
        None
      }

      val (remoteTdefArg, returnSchema) = transform.getOrElse(("", sft))
      val toFeatures = resultsToFeatures(returnSchema, None, None)
      val filterTransform: Seq[(Int, HFilter)] = if (ecql.isEmpty && transform.isEmpty) { Seq.empty } else {
        // transforms and filters are applied server-side
        val remoteCQLFilter: Filter = ecql.getOrElse(Filter.INCLUDE)
        val encodedSft = SimpleFeatureTypes.encodeType(returnSchema)
        val filter = new JSimpleFeatureFilter(sft, remoteCQLFilter, remoteTdefArg, encodedSft)
        Seq((JSimpleFeatureFilter.Priority, filter))
      }

      val additionalFilters = createPushDownFilters(ds, sft, filter, transform)
      ScanConfig(filterTransform ++ additionalFilters, coprocessor, toFeatures)
    }
  }

  protected def hasDuplicates(sft: SimpleFeatureType, filter: Option[Filter]): Boolean = false

  // default implementation does nothing, override in subclasses
  protected def createPushDownFilters(ds: HBaseDataStore,
                                      sft: SimpleFeatureType,
                                      filter: HBaseFilterStrategyType,
                                      transform: Option[(String, SimpleFeatureType)]): Seq[(Int, HFilter)] = Seq.empty

  protected def buildPlatformScanPlan(ds: HBaseDataStore,
                                      sft: SimpleFeatureType,
                                      filter: HBaseFilterStrategyType,
                                      hints: Hints,
                                      ranges: Seq[Query],
                                      table: TableName,
                                      hbaseFilters: Seq[(Int, HFilter)],
                                      coprocessor: Option[Coprocessor],
                                      toFeatures: (Iterator[Result]) => Iterator[SimpleFeature]): HBaseQueryPlan
}
