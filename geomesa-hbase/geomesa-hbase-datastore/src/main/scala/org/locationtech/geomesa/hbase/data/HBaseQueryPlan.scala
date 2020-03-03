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
import org.apache.hadoop.hbase.filter.{FilterList, Filter => HFilter}
import org.apache.hadoop.hbase.util.Bytes
import org.locationtech.geomesa.hbase.utils.{CoprocessorBatchScan, HBaseBatchScan}
import org.locationtech.geomesa.index.PartitionParallelScan
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}

sealed trait HBaseQueryPlan extends QueryPlan[HBaseDataStore] {

  /**
    * Tables being scanned
    *
    * @return
    */
  def tables: Seq[TableName]

  /**
    * Ranges being scanned. These may not correspond to the actual scans being executed
    *
    * @return
    */
  def ranges: Seq[Scan]

  /**
    * Scans to be executed
    *
    * @return
    */
  def scans: Seq[Scan]

  override def explain(explainer: Explainer, prefix: String = ""): Unit =
    HBaseQueryPlan.explain(this, explainer, prefix)

  // additional explaining, if any
  protected def explain(explainer: Explainer): Unit = {}
}

object HBaseQueryPlan {

  def explain(plan: HBaseQueryPlan, explainer: Explainer, prefix: String): Unit = {
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getSimpleName}")
    explainer(s"Tables: ${plan.tables.mkString(", ")}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(rangeToString).mkString(", ")}")
    explainer(s"Scans (${plan.scans.size}): ${plan.scans.take(5).map(rangeToString).mkString(", ")}")
    explainer(s"Column families: ${plan.scans.headOption.flatMap(r => Option(r.getFamilies)).getOrElse(Array.empty).map(Bytes.toString).mkString(",")}")
    explainer(s"Remote filters: ${plan.scans.headOption.flatMap(r => Option(r.getFilter)).map(filterToString).getOrElse("none")}")
    plan.explain(explainer)
    explainer.popLevel()
  }

  private [data] def rangeToString(range: Scan): String = {
    // based on accumulo's byte representation
    def printable(b: Byte): String = {
      val c = 0xff & b
      if (c >= 32 && c <= 126) { c.toChar.toString } else { f"%%$c%02x;" }
    }
    s"[${range.getStartRow.map(printable).mkString("")}::${range.getStopRow.map(printable).mkString("")}]"
  }

  private [data] def filterToString(filter: HFilter): String = {
    import scala.collection.JavaConversions._
    filter match {
      case f: FilterList => f.getFilters.map(filterToString).mkString(", ")
      case f             => f.toString
    }
  }

  // plan that will not actually scan anything
  case class EmptyPlan(filter: FilterStrategy, reducer: Option[FeatureReducer] = None) extends HBaseQueryPlan {
    override type Results = Result
    override val tables: Seq[TableName] = Seq.empty
    override val ranges: Seq[Scan] = Seq.empty
    override val scans: Seq[Scan] = Seq.empty
    override val resultsToFeatures: ResultsToFeatures[Result] = ResultsToFeatures.empty
    override val sort: Option[Seq[(String, Boolean)]] = None
    override val maxFeatures: Option[Int] = None
    override val projection: Option[QueryReferenceSystems] = None
    override def scan(ds: HBaseDataStore): CloseableIterator[Result] = CloseableIterator.empty
  }

  case class ScanPlan(
      filter: FilterStrategy,
      tables: Seq[TableName],
      ranges: Seq[Scan],
      scans: Seq[Scan],
      resultsToFeatures: ResultsToFeatures[Result],
      reducer: Option[FeatureReducer],
      sort: Option[Seq[(String, Boolean)]],
      maxFeatures: Option[Int],
      projection: Option[QueryReferenceSystems]
    ) extends HBaseQueryPlan {

    override type Results = Result

    override def scan(ds: HBaseDataStore): CloseableIterator[Result] = {
      // note: we have to copy the ranges for each scan, except the last one (since it won't conflict at that point)
      val iter = tables.iterator
      val scans = iter.map(singleTableScan(ds, _, copyScans = iter.hasNext))

      if (PartitionParallelScan.toBoolean.contains(true)) {
        // kick off all the scans at once
        scans.foldLeft(CloseableIterator.empty[Result])(_ ++ _)
      } else {
        // kick off the scans sequentially as they finish
        SelfClosingIterator(scans).flatMap(s => s)
      }
    }

    private def singleTableScan(
        ds: HBaseDataStore,
        table: TableName,
        copyScans: Boolean): CloseableIterator[Result] = {
      val s = if (copyScans) { scans.map(new Scan(_)) } else { scans }
      HBaseBatchScan(ds.connection, table, s, ds.config.queryThreads)
    }
  }

  case class CoprocessorPlan(
      filter: FilterStrategy,
      tables: Seq[TableName],
      ranges: Seq[Scan],
      scans: Seq[Scan],
      coprocessorOptions: Map[String, String],
      resultsToFeatures: ResultsToFeatures[Array[Byte]],
      reducer: Option[FeatureReducer],
      maxFeatures: Option[Int],
      projection: Option[QueryReferenceSystems]
    ) extends HBaseQueryPlan {

    override type Results = Array[Byte]

    override def sort: Option[Seq[(String, Boolean)]] = None // client side sorting is not relevant for coprocessors

    /**
      * Runs the query plain against the underlying database, returning the raw entries
      *
      * @param ds data store - provides connection object and metadata
      * @return
      */
    override def scan(ds: HBaseDataStore): CloseableIterator[Array[Byte]] = {
      // note: we have to copy the ranges for each scan, except the last one (since it won't conflict at that point)
      val iter = tables.iterator
      val scans = iter.map(singleTableScan(ds, _, copyScans = iter.hasNext))

      if (PartitionParallelScan.toBoolean.contains(true)) {
        // kick off all the scans at once
        scans.foldLeft(CloseableIterator.empty[Array[Byte]])(_ ++ _)
      } else {
        // kick off the scans sequentially as they finish
        SelfClosingIterator(scans).flatMap(s => s)
      }
    }

    override protected def explain(explainer: Explainer): Unit =
      explainer("Coprocessor options: " + coprocessorOptions.map(m => s"[${m._1}:${m._2}]").mkString(", "))

    private def singleTableScan(
        ds: HBaseDataStore,
        table: TableName,
        copyScans: Boolean): CloseableIterator[Array[Byte]] = {
      val s = if (copyScans) { scans.map(new Scan(_)) } else { scans }
      CoprocessorBatchScan(ds.connection, table, s, coprocessorOptions, ds.config.queryThreads)
    }
  }
}
