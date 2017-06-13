/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.util

import com.google.common.collect.Lists
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.filter.{FilterList, MultiRowRangeFilter, Filter => HFilter}
import org.locationtech.geomesa.hbase.coprocessor.utils.CoprocessorConfig
import org.locationtech.geomesa.hbase.utils.HBaseBatchScan
import org.locationtech.geomesa.hbase.{HBaseFilterStrategyType, HBaseQueryPlanType}
import org.locationtech.geomesa.index.index.IndexAdapter
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeature

sealed trait HBaseQueryPlan extends HBaseQueryPlanType {
  def filter: HBaseFilterStrategyType
  def table: TableName
  def ranges: Seq[Query]

  override def explain(explainer: Explainer, prefix: String = ""): Unit =
    HBaseQueryPlan.explain(this, explainer, prefix)
}

object HBaseQueryPlan {

  def explain(plan: HBaseQueryPlan, explainer: Explainer, prefix: String): Unit = {
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getName}")
    explainer(s"Table: ${Option(plan.table).orNull}")
    explainer(s"Ranges (${plan.ranges.size}): { ${plan.ranges.take(5).map(rangeToString).mkString(", ")} }")
    explainer(s"Filter: ${plan.filter.toString}")
    explainer.popLevel()
  }

  private def rangeToString(range: Query): String = {
    range match {
      case r: Scan => s"[${r.getStartRow.mkString("")},${r.getStopRow.mkString("")}]"
      case r: Get => s"[${r.getRow.mkString("")},${r.getRow.mkString("")}]"
    }
  }
}

// plan that will not actually scan anything
case class EmptyPlan(filter: HBaseFilterStrategyType) extends HBaseQueryPlan {
  override val table: TableName = null
  override val ranges: Seq[Query] = Seq.empty
  override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = CloseableIterator.empty
}

case class ScanPlan(filter: HBaseFilterStrategyType,
                    table: TableName,
                    ranges: Seq[Scan],
                    resultsToFeatures: Iterator[Result] => Iterator[SimpleFeature]) extends HBaseQueryPlan {
  override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = {
    ranges.foreach(ds.applySecurity)
    val results = new HBaseBatchScan(ds.connection, table, ranges, ds.config.queryThreads, 100000)
    SelfClosingIterator(resultsToFeatures(results), results.close())
  }
}

case class CoprocessorPlan(filter: HBaseFilterStrategyType,
                           table: TableName,
                           ranges: Seq[Query],
                           remoteFilters: Seq[(Int, HFilter)],
                           coprocessorConfig: CoprocessorConfig) extends HBaseQueryPlan  {

  /**
    * Runs the query plain against the underlying database, returning the raw entries
    *
    * @param ds data store - provides connection object and metadata
    * @return
    */
  override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = {
    // TODO: Refactor this logical into HBasePlatform?
    val (scan, filterList) = calculateScanAndFilterList(ranges, remoteFilters)
    val hbaseTable = ds.connection.getTable(table)

    import org.locationtech.geomesa.hbase.coprocessor._
    ds.applySecurity(scan)
    val byteArray = serializeOptions(coprocessorConfig.configureScanAndFilter(scan, filterList))

    val result = GeoMesaCoprocessor.execute(hbaseTable, byteArray)
    val results = result.toIterator.filter(_.size() != 0).map(r => coprocessorConfig.bytesToFeatures(r.toByteArray))
    coprocessorConfig.reduce(results)
  }

  def calculateScanAndFilterList(ranges: Seq[Query],
                                 remoteFilters: Seq[(Int, HFilter)]): (Scan, FilterList) = {
    val rowRanges = Lists.newArrayList[RowRange]()
    ranges.foreach {
      case g: Get =>
        rowRanges.add(new RowRange(g.getRow, true, IndexAdapter.rowFollowingRow(g.getRow), false))
      case s: Scan =>
        rowRanges.add(new RowRange(s.getStartRow, true, s.getStopRow, false))
    }
    val sortedRowRanges: util.List[RowRange] = MultiRowRangeFilter.sortAndMerge(rowRanges)
    val mrrf = new MultiRowRangeFilter(sortedRowRanges)
    // note: mrrf first priority
    val filterList = new FilterList(remoteFilters.sortBy(_._1).map(_._2).+:(mrrf): _*)

    val scan = new Scan()
    scan.setFilter(filterList)
    (scan, filterList)
  }

  override def explain(explainer: Explainer, prefix: String): Unit = {
    super.explain(explainer, prefix)
    val filterString = remoteFilters.sortBy(_._1).map( f => s"${f._1}[${f._2.getClass.getName}]" ).mkString("{", ", ", "}")
    explainer.pushLevel("Remote Filters: " + filterString)
    explainer("Coprocessor Options: " + coprocessor.options.map( m => s"[${m._1}:${m._2}]").mkString("{", ", ", "}"))
    explainer.popLevel()
  }
}
