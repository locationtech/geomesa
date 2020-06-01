/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.util.Bytes
import org.locationtech.geomesa.hbase.HBaseSystemProperties
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan.{TableScan, filterToString, rangeToString, scanToString}
import org.locationtech.geomesa.hbase.utils.{CoprocessorBatchScan, HBaseBatchScan}
import org.locationtech.geomesa.index.PartitionParallelScan
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.index.utils.ThreadManagement.Timeout
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.index.ByteArrays

sealed trait HBaseQueryPlan extends QueryPlan[HBaseDataStore] {

  /**
    * Ranges being scanned
    *
    * @return
    */
  def ranges: Seq[RowRange]

  /**
    * Scans to be executed
    *
    * @return
    */
  def scans: Seq[TableScan]

  override def scan(ds: HBaseDataStore): CloseableIterator[Results] = {
    // convert the relative timeout to an absolute timeout up front
    val timeout = ds.config.queries.timeout.map(Timeout.apply)
    val iter = scans.iterator.map(singleTableScan(_, ds.connection, threads(ds), timeout))
    if (PartitionParallelScan.toBoolean.contains(true)) {
      // kick off all the scans at once
      iter.foldLeft(CloseableIterator.empty[Results])(_ ++ _)
    } else {
      // kick off the scans sequentially as they finish
      SelfClosingIterator(iter).flatMap(s => s)
    }
  }

  override def explain(explainer: Explainer, prefix: String = ""): Unit = {
    explainer.pushLevel(s"${prefix}Plan: ${getClass.getSimpleName}")
    explainer(s"Tables: ${scans.map(_.table).mkString(", ")}")
    explainer(s"Ranges (${ranges.size}): ${ranges.take(5).map(rangeToString).mkString(", ")}")
    explainer(s"Scans (${scans.headOption.map(_.scans.size).getOrElse(0)}): ${scans.headOption.toSeq.flatMap(_.scans.take(5)).map(scanToString).mkString(", ")}")
    explainer(s"Column families: ${scans.headOption.flatMap(_.scans.headOption).flatMap(r => Option(r.getFamilies)).getOrElse(Array.empty).map(Bytes.toString).mkString(",")}")
    explainer(s"Remote filters: ${scans.headOption.flatMap(_.scans.headOption).flatMap(r => Option(r.getFilter)).map(filterToString).getOrElse("none")}")
    explain(explainer)
    explainer.popLevel()
  }

  protected def threads(ds: HBaseDataStore): Int

  protected def singleTableScan(
      scan: TableScan,
      connection: Connection,
      threads: Int,
      timeout: Option[Timeout]): CloseableIterator[Results]

  // additional explaining, if any
  protected def explain(explainer: Explainer): Unit = {}
}

object HBaseQueryPlan {

  import scala.collection.JavaConverters._

  private def rangeToString(range: RowRange): String =
    s"[${ByteArrays.printable(range.getStartRow)}::${ByteArrays.printable(range.getStopRow)}]"

  private def scanToString(scan: Scan): String =
    s"[${ByteArrays.printable(scan.getStartRow)}::${ByteArrays.printable(scan.getStopRow)}]"

  private def filterToString(filter: org.apache.hadoop.hbase.filter.Filter): String = {
    filter match {
      case f: FilterList => f.getFilters.asScala.map(filterToString).mkString(", ")
      case f             => f.toString
    }
  }

  case class TableScan(table: TableName, scans: Seq[Scan])

  // plan that will not actually scan anything
  case class EmptyPlan(filter: FilterStrategy, reducer: Option[FeatureReducer] = None) extends HBaseQueryPlan {
    override type Results = Result
    override val ranges: Seq[RowRange] = Seq.empty
    override val scans: Seq[TableScan] = Seq.empty
    override val resultsToFeatures: ResultsToFeatures[Result] = ResultsToFeatures.empty
    override val sort: Option[Seq[(String, Boolean)]] = None
    override val maxFeatures: Option[Int] = None
    override val projection: Option[QueryReferenceSystems] = None
    override def scan(ds: HBaseDataStore): CloseableIterator[Result] = CloseableIterator.empty
    override protected def threads(ds: HBaseDataStore): Int = 0
    override protected def singleTableScan(
        scan: TableScan,
        connection: Connection,
        threads: Int,
        timeout: Option[Timeout]): CloseableIterator[Result] = CloseableIterator.empty
  }

  case class ScanPlan(
      filter: FilterStrategy,
      ranges: Seq[RowRange],
      scans: Seq[TableScan],
      resultsToFeatures: ResultsToFeatures[Result],
      reducer: Option[FeatureReducer],
      sort: Option[Seq[(String, Boolean)]],
      maxFeatures: Option[Int],
      projection: Option[QueryReferenceSystems]
    ) extends HBaseQueryPlan {

    override type Results = Result

    override protected def threads(ds: HBaseDataStore): Int = ds.config.queries.threads

    override protected def singleTableScan(
        scan: TableScan,
        connection: Connection,
        threads: Int,
        timeout: Option[Timeout]): CloseableIterator[Result] = {
      HBaseBatchScan(this, connection, scan.table, scan.scans, threads, timeout)
    }
  }

  case class CoprocessorPlan(
      filter: FilterStrategy,
      ranges: Seq[RowRange],
      scans: Seq[TableScan],
      coprocessorOptions: Map[String, String],
      resultsToFeatures: ResultsToFeatures[Array[Byte]],
      reducer: Option[FeatureReducer],
      maxFeatures: Option[Int],
      projection: Option[QueryReferenceSystems]
    ) extends HBaseQueryPlan {

    override type Results = Array[Byte]

    private lazy val maximizeThreads = HBaseSystemProperties.CoprocessorMaxThreads.toBoolean.get

    override def sort: Option[Seq[(String, Boolean)]] = None // client side sorting is not relevant for coprocessors

    override protected def threads(ds: HBaseDataStore): Int = ds.config.coprocessors.threads

    override protected def singleTableScan(
        scan: TableScan,
        connection: Connection,
        threads: Int,
        timeout: Option[Timeout]): CloseableIterator[Array[Byte]] = {
      val scanThreads = if (maximizeThreads) { scan.scans.length } else { threads * 2 }
      CoprocessorBatchScan(this, connection, scan.table, scan.scans, coprocessorOptions, scanThreads, threads, timeout)
    }

    override protected def explain(explainer: Explainer): Unit =
      explainer("Coprocessor options: " + coprocessorOptions.map(m => s"[${m._1}:${m._2}]").mkString(", "))
  }
}
