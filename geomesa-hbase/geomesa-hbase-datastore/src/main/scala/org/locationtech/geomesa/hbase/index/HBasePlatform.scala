/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index

import com.google.common.collect.Lists
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{FilterList, MultiRowRangeFilter, Filter => HFilter}
import org.locationtech.geomesa.hbase.HBaseFilterStrategyType
import org.locationtech.geomesa.hbase.coprocessor.utils.CoprocessorConfig
import org.locationtech.geomesa.hbase.data.{CoprocessorPlan, HBaseDataStore, HBaseQueryPlan, ScanPlan}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait HBasePlatform extends HBaseIndexAdapter {

  override protected def buildPlatformScanPlan(ds: HBaseDataStore,
                                               sft: SimpleFeatureType,
                                               filter: HBaseFilterStrategyType,
                                               ranges: Seq[Scan],
                                               colFamily: Array[Byte],
                                               table: TableName,
                                               hbaseFilters: Seq[(Int, HFilter)],
                                               coprocessor: Option[CoprocessorConfig],
                                               toFeatures: (Iterator[Result]) => Iterator[SimpleFeature]): HBaseQueryPlan = {
    val filterList = hbaseFilters.sortBy(_._1).map(_._2)
    coprocessor match {
      case None =>
        // optimize the scans
        val scans = if (ranges.head.isSmall) {
          configureSmallScans(ds, ranges, colFamily, filterList)
        } else {
          configureMultiRowRangeFilter(ds, ranges, colFamily, filterList)
        }
        ScanPlan(filter, table, scans, toFeatures)

      case Some(coprocessorConfig) =>
        val scan = configureCoprocessorScan(ds, ranges, colFamily, filterList)
        CoprocessorPlan(filter, table, ranges, scan, coprocessorConfig)
    }
  }

  private def configureSmallScans(ds: HBaseDataStore,
                                  ranges: Seq[Scan],
                                  colFamily: Array[Byte],
                                  hbaseFilters: Seq[HFilter]): Seq[Scan] = {
    val filterList = if (hbaseFilters.isEmpty) { None } else { Some(new FilterList(hbaseFilters: _*)) }
    ranges.foreach { r =>
      r.addColumn(colFamily, HBaseColumnGroups.default)
      filterList.foreach(r.setFilter)
      ds.applySecurity(r)
    }
    ranges
  }

  private def configureMultiRowRangeFilter(ds: HBaseDataStore,
                                           originalRanges: Seq[Scan],
                                           colFamily: Array[Byte],
                                           hbaseFilters: Seq[HFilter]): Seq[Scan] = {
    import scala.collection.JavaConversions._

    val rowRanges = new java.util.ArrayList[RowRange](originalRanges.length)
    originalRanges.foreach(r => rowRanges.add(new RowRange(r.getStartRow, true, r.getStopRow, false)))

    val sortedRowRanges = MultiRowRangeFilter.sortAndMerge(rowRanges)
    val numRanges = sortedRowRanges.length
    val numThreads = ds.config.queryThreads
    // TODO GEOMESA-1806 parameterize this?
    val rangesPerThread = math.min(ds.config.maxRangesPerExtendedScan, math.max(1, math.ceil(numRanges / numThreads * 2).toInt))
    // TODO GEOMESA-1806 align partitions with region boundaries
    val groupedRanges = Lists.partition(sortedRowRanges, rangesPerThread)

    // group scans into batches to achieve some client side parallelism
    val groupedScans = groupedRanges.map { localRanges =>
      // TODO GEOMESA-1806
      // currently, this constructor will call sortAndMerge a second time
      // this is unnecessary as we have already sorted and merged above
      val mrrf = new MultiRowRangeFilter(localRanges)
      // note: mrrf first priority
      val filterList = new FilterList(hbaseFilters.+:(mrrf): _*)

      val s = new Scan()
      s.setStartRow(localRanges.head.getStartRow)
      s.setStopRow(localRanges.get(localRanges.length - 1).getStopRow)
      s.setFilter(filterList)
      s.addColumn(colFamily, HBaseColumnGroups.default)
      // TODO GEOMESA-1806 parameterize cache size
      s.setCaching(1000)
      s.setCacheBlocks(true)
      s
    }

    // Apply Visibilities
    groupedScans.foreach(ds.applySecurity)
    groupedScans
  }

  private def configureCoprocessorScan(ds: HBaseDataStore,
                                       ranges: Seq[Scan],
                                       colFamily: Array[Byte],
                                       hbaseFilters: Seq[HFilter]): Scan = {
    val rowRanges = new java.util.ArrayList[RowRange](ranges.length)
    ranges.foreach(r => rowRanges.add(new RowRange(r.getStartRow, true, r.getStopRow, false)))

    val sortedRowRanges = MultiRowRangeFilter.sortAndMerge(rowRanges)
    val mrrf = new MultiRowRangeFilter(sortedRowRanges)
    // note: mrrf first priority
    val filterList = new FilterList(hbaseFilters.+:(mrrf): _*)

    val scan = new Scan()
    scan.addColumn(colFamily, HBaseColumnGroups.default)
    scan.setFilter(filterList)
    ds.applySecurity(scan)
    scan
  }
}
