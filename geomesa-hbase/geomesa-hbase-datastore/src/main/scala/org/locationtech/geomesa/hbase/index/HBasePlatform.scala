/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
                                               tables: Seq[TableName],
                                               hbaseFilters: Seq[(Int, HFilter)],
                                               coprocessor: Option[CoprocessorConfig],
                                               toFeatures: Iterator[Result] => Iterator[SimpleFeature]): HBaseQueryPlan = {
    val filterList = if (hbaseFilters.isEmpty) { None } else { Some(hbaseFilters.sortBy(_._1).map(_._2)) }
    coprocessor match {
      case None =>
        // optimize the scans
        val scans = if (ranges.head.isSmall) {
          configureSmallScans(ds, ranges, colFamily, filterList)
        } else {
          configureMultiRowRangeFilter(ds, ranges, colFamily, filterList)
        }
        ScanPlan(filter, tables, ranges, scans, toFeatures)

      case Some(coprocessorConfig) =>
        val scan = configureCoprocessorScan(ds, ranges, colFamily, filterList)
        CoprocessorPlan(filter, tables, ranges, scan, coprocessorConfig)
    }
  }

  private def configureSmallScans(ds: HBaseDataStore,
                                  ranges: Seq[Scan],
                                  colFamily: Array[Byte],
                                  filterList: Option[Seq[HFilter]]): Seq[Scan] = {
    val filter = filterList.map {
      case Seq(f)  => f
      case filters => new FilterList(filters: _*)
    }
    ranges.foreach { r =>
      r.addColumn(colFamily, HBaseColumnGroups.default)
      filter.foreach(r.setFilter)
      ds.applySecurity(r)
    }
    ranges
  }

  private def configureMultiRowRangeFilter(ds: HBaseDataStore,
                                           originalRanges: Seq[Scan],
                                           colFamily: Array[Byte],
                                           filterList: Option[Seq[HFilter]]): Seq[Scan] = {
    import scala.collection.JavaConverters._

    val cacheBlocks = HBaseSystemProperties.ScannerBlockCaching.toBoolean.get // has a default value so .get is safe
    val cacheSize = HBaseSystemProperties.ScannerCaching.toInt

    logger.debug(s"HBase client scanner: block caching: $cacheBlocks, caching: $cacheSize")

    val rowRanges = HBasePlatform.sortAndMerge(originalRanges)

    // TODO GEOMESA-1806 parameterize this?
    val rangesPerThread = math.min(ds.config.maxRangesPerExtendedScan,
      math.max(1, math.ceil(rowRanges.size() / ds.config.queryThreads * 2).toInt))

    // group scans into batches to achieve some client side parallelism
    // we double the initial size to account for extra groupings based on the shard byte
    val groupedScans = new java.util.ArrayList[Scan]((rowRanges.size() / rangesPerThread + 1) * 2)

    def addGroup(group: java.util.List[RowRange]): Unit = {
      val s = new Scan()
      s.setStartRow(group.get(0).getStartRow)
      s.setStopRow(group.get(group.size() - 1).getStopRow)
      if (group.size() > 1) {
        // TODO GEOMESA-1806
        // currently, the MultiRowRangeFilter constructor will call sortAndMerge a second time
        // this is unnecessary as we have already sorted and merged
        val mrrf = new MultiRowRangeFilter(group)
        // note: mrrf first priority
        s.setFilter(filterList.map(f => new FilterList(f.+:(mrrf): _*)).getOrElse(mrrf))
      } else {
        filterList.foreach {
          case Seq(f)  => s.setFilter(f)
          case filters => s.setFilter(new FilterList(filters: _*))
        }
      }

      s.addColumn(colFamily, HBaseColumnGroups.default)
      s.setCacheBlocks(cacheBlocks)
      cacheSize.foreach(s.setCaching)

      // apply visibilities
      ds.applySecurity(s)

      groupedScans.add(s)
    }

    // TODO GEOMESA-1806 align partitions with region boundaries

    if (!rowRanges.isEmpty) {
      var i = 1
      var groupStart = 0
      var groupCount = 1
      var groupFirstByte: Byte =
        if (rowRanges.get(0).getStartRow.isEmpty) { 0 } else { rowRanges.get(0).getStartRow()(0) }

      while (i < rowRanges.size()) {
        val nextRange = rowRanges.get(i)
        // add the group if we hit our group size or if we transition the first byte (i.e. our shard byte)
        if (groupCount == rangesPerThread ||
            (nextRange.getStartRow.length > 0 && groupFirstByte != nextRange.getStartRow()(0))) {
          // note: excludes current range we're checking
          addGroup(rowRanges.subList(groupStart, i))
          groupFirstByte = if (nextRange.getStopRow.isEmpty) { Byte.MaxValue } else { nextRange.getStopRow()(0) }
          groupStart = i
          groupCount = 1
        } else {
          groupCount += 1
        }
        i += 1
      }

      // add the final group - there will always be at least one remaining range
      addGroup(rowRanges.subList(groupStart, i))
    }

    // shuffle the ranges, otherwise our threads will tend to all hit the same region server at once
    Collections.shuffle(groupedScans)

    groupedScans.asScala
  }

  private def configureCoprocessorScan(ds: HBaseDataStore,
                                       ranges: Seq[Scan],
                                       colFamily: Array[Byte],
                                       filterList: Option[Seq[HFilter]]): Scan = {
    val scan = new Scan()
    scan.addColumn(colFamily, HBaseColumnGroups.default)
    // note: mrrf first priority
    val mrrf = new MultiRowRangeFilter(HBasePlatform.sortAndMerge(ranges))
    // note: our coprocessors always expect a filter list
    scan.setFilter(new FilterList(filterList.map(f => f.+:(mrrf)).getOrElse(Seq(mrrf)): _*))
    ds.applySecurity(scan)
    scan
  }
}

object HBasePlatform {

  /**
    * Scala convenience method for org.apache.hadoop.hbase.filter.MultiRowRangeFilter#sortAndMerge(java.util.List)
    *
    * @param ranges scan ranges
    * @return
    */
  def sortAndMerge(ranges: Seq[Scan]): java.util.List[RowRange] = {
    val rowRanges = new java.util.ArrayList[RowRange](ranges.length)
    ranges.foreach(r => rowRanges.add(new RowRange(r.getStartRow, true, r.getStopRow, false)))
    MultiRowRangeFilter.sortAndMerge(rowRanges)
  }
}
