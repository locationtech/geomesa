/***********************************************************************
* Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.index

import com.google.common.collect.Lists
import org.apache.hadoop.hbase.{Coprocessor, TableName}
import org.apache.hadoop.hbase.client.{Get, Query, Result, Scan}
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{FilterList, MultiRowRangeFilter, Filter => HFilter}
import org.geotools.factory.Hints
import org.locationtech.geomesa.hbase.HBaseFilterStrategyType
import org.locationtech.geomesa.hbase.data.{CoprocessorPlan, HBaseDataStore, HBaseQueryPlan, ScanPlan}
import org.locationtech.geomesa.index.index.IndexAdapter
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait HBasePlatform extends HBaseFeatureIndex {

  override protected def buildPlatformScanPlan(ds: HBaseDataStore,
                                               sft: SimpleFeatureType,
                                               filter: HBaseFilterStrategyType,
                                               hints: Hints,
                                               originalRanges: Seq[Query],
                                               table: TableName,
                                               hbaseFilters: Seq[HFilter],
                                               coprocessor: Option[Coprocessor],
                                               toFeatures: (Iterator[Result]) => Iterator[SimpleFeature]): HBaseQueryPlan = {
    // check if these Scans or Gets
    // Only in the case of 'ID IN ()' queries will this be Gets
    val scans = originalRanges.head match {
      case t: Get  => configureGet(originalRanges, hbaseFilters)
      case t: Scan => configureMultiRowRangeFilter(ds, originalRanges, hbaseFilters)
    }

    coprocessor match {
      case Some(processor) => CoprocessorPlan(sft, filter, hints, table, scans, toFeatures)
      case None => ScanPlan(filter, table, scans, toFeatures)
    }
  }

  private def configureGet(originalRanges: Seq[Query], hbaseFilters: Seq[HFilter]): Seq[Scan] = {
    val filterList = new FilterList(hbaseFilters: _*)
    // convert Gets to Scans for Spark SQL compatibility
    originalRanges.map { r =>
      val g = r.asInstanceOf[Get]
      val start = g.getRow
      val end = IndexAdapter.rowFollowingRow(start)
      new Scan(g).setStartRow(start).setStopRow(end).setFilter(filterList).setSmall(true)
    }
  }

  private def configureMultiRowRangeFilter(ds: HBaseDataStore, originalRanges: Seq[Query], hbaseFilters: Seq[HFilter]) = {
    import scala.collection.JavaConversions._
    val rowRanges = Lists.newArrayList[RowRange]()
    originalRanges.foreach { r =>
      rowRanges.add(new RowRange(r.asInstanceOf[Scan].getStartRow, true, r.asInstanceOf[Scan].getStopRow, false))
    }
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
      val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, mrrf)
      hbaseFilters.foreach { f => filterList.addFilter(f) }

      val s = new Scan()
      s.setStartRow(localRanges.head.getStartRow)
      s.setStopRow(localRanges.get(localRanges.length - 1).getStopRow)
      s.setFilter(filterList)
      // TODO GEOMESA-1806 parameterize cache size
      s.setCaching(1000)
      s.setCacheBlocks(true)
      s
    }

    // Apply Visibilities
    groupedScans.foreach(ds.applySecurity)
    groupedScans
  }
}
