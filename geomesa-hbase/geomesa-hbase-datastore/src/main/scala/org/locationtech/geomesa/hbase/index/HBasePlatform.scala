/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.index

import java.util.Collections

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{FilterList, MultiRowRangeFilter, Filter => HFilter}
import org.locationtech.geomesa.hbase.coprocessor.utils.CoprocessorConfig
import org.locationtech.geomesa.hbase.data.{CoprocessorPlan, HBaseDataStore, HBaseQueryPlan, ScanPlan}
import org.locationtech.geomesa.hbase.{HBaseFilterStrategyType, HBaseSystemProperties}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait HBasePlatform extends HBaseIndexAdapter with LazyLogging {

  override protected def buildPlatformScanPlan(ds: HBaseDataStore,
                                               sft: SimpleFeatureType,
                                               filter: HBaseFilterStrategyType,
                                               ranges: Seq[Scan],
                                               colFamily: Array[Byte],
                                               table: TableName,
                                               hbaseFilters: Seq[(Int, HFilter)],
                                               coprocessor: Option[CoprocessorConfig],
                                               toFeatures: Iterator[Result] => Iterator[SimpleFeature]): HBaseQueryPlan = {
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
    import scala.collection.JavaConverters._

    val cacheSize = HBaseSystemProperties.ScannerCaching.toInt
    val cacheBlocks = HBaseSystemProperties.ScannerBlockCaching.toBoolean.get // has a default value so .get is safe

    logger.debug(s"HBase client scanner: block caching: $cacheBlocks, caching: $cacheSize")

    val rowRanges = new java.util.ArrayList[RowRange](originalRanges.length)
    originalRanges.foreach(r => rowRanges.add(new RowRange(r.getStartRow, true, r.getStopRow, false)))

    val sortedRowRanges = MultiRowRangeFilter.sortAndMerge(rowRanges)
    val numRanges = sortedRowRanges.size()
    val numThreads = ds.config.queryThreads
    // TODO GEOMESA-1806 parameterize this?
    val rangesPerThread = math.min(ds.config.maxRangesPerExtendedScan, math.max(1, math.ceil(numRanges / numThreads * 2).toInt))

    // group scans into batches to achieve some client side parallelism
    val groupedScans = new java.util.ArrayList[Scan](sortedRowRanges.size() / rangesPerThread + 1)

    // TODO GEOMESA-1806 align partitions with region boundaries

    var i = 0
    var start: Int = 0

    while (i < sortedRowRanges.size()) {
      val mod = i % rangesPerThread
      if (mod == 0) {
        start = i
      }
      if (mod == rangesPerThread - 1 || i == sortedRowRanges.size() - 1) {
        // TODO GEOMESA-1806
        // currently, this constructor will call sortAndMerge a second time
        // this is unnecessary as we have already sorted and merged above
        val mrrf = new MultiRowRangeFilter(sortedRowRanges.subList(start, i + 1))
        // note: mrrf first priority
        val filterList = new FilterList(hbaseFilters.+:(mrrf): _*)

        val s = new Scan()
        s.setStartRow(sortedRowRanges.get(start).getStartRow)
        s.setStopRow(sortedRowRanges.get(i).getStopRow)
        s.setFilter(filterList)
        s.addColumn(colFamily, HBaseColumnGroups.default)
        s.setCacheBlocks(cacheBlocks)
        cacheSize.foreach(s.setCaching)

        // apply visibilities
        ds.applySecurity(s)

        groupedScans.add(s)
      }
      i += 1
    }

    // shuffle the ranges, otherwise our threads will tend to all hit the same region server at once
    Collections.shuffle(groupedScans)

    groupedScans.asScala
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
