/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.FilterList
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter.RowRange
import org.apache.hadoop.hbase.util.Bytes
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.hbase.HBaseSystemProperties
import org.locationtech.geomesa.hbase.data.HBaseIndexAdapter.HBaseResultsToFeatures
import org.locationtech.geomesa.hbase.data.HBaseQueryPlan.{TableScan, filterToString, rangeToString, scanToString}
import org.locationtech.geomesa.hbase.utils.{CoprocessorBatchScan, HBaseBatchScan}
import org.locationtech.geomesa.index.api.QueryPlan.ResultsToFeatures.IdentityResultsToFeatures
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, QueryStrategyPlan, ResultsToFeatures}
import org.locationtech.geomesa.index.api.QueryStrategy
import org.locationtech.geomesa.index.planning.LocalQueryRunner.LocalProcessor
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.index.utils.ThreadManagement.Timeout
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.ByteArrays

sealed trait HBaseQueryPlan extends QueryStrategyPlan {

  def ds: HBaseDataStore

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

  override def scan(): CloseableIterator[Results] = {
    // query guard hook - also handles full table scan checks
    strategy.runGuards(ds)
    // convert the relative timeout to an absolute timeout up front
    val timeout = ds.config.queries.timeout.map(Timeout.apply)
    val iter = scans.iterator.map(singleTableScan(_, ds.connection, threads, timeout))
    if (ds.config.queries.parallelPartitionScans) {
      // kick off all the scans at once
      iter.foldLeft(CloseableIterator.empty[Results])(_ concat _)
    } else {
      // kick off the scans sequentially as they finish
      SelfClosingIterator(iter).flatMap(s => s)
    }
  }

  override def explain(explainer: Explainer): Unit = {
    explainer.pushLevel(s"Plan: ${getClass.getSimpleName}")
    explainer(s"Tables: ${scans.map(_.table).mkString(", ")}")
    explainer(s"Ranges (${ranges.size}): ${ranges.take(5).map(rangeToString).mkString(", ")}")
    explainer(s"Scans (${scans.headOption.map(_.scans.size).getOrElse(0)}): ${scans.headOption.toSeq.flatMap(_.scans.take(5)).map(scanToString).mkString(", ")}")
    explainer(s"Column families: ${scans.headOption.flatMap(_.scans.headOption).flatMap(r => Option(r.getFamilies)).getOrElse(Array.empty).map(Bytes.toString).mkString(",")}")
    explainer(s"Remote filters: ${scans.headOption.flatMap(_.scans.headOption).flatMap(r => Option(r.getFilter)).map(filterToString).getOrElse("none")}")
    moreExplaining(explainer)
    explainer(s"Reduce: ${reducer.getOrElse("none")}")
    explainer.popLevel()
  }

  protected def threads: Int

  protected def singleTableScan(
      scan: TableScan,
      connection: Connection,
      threads: Int,
      timeout: Option[Timeout]): CloseableIterator[Results]

  // additional explaining, if any
  protected def moreExplaining(explainer: Explainer): Unit = {}
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
  case class EmptyPlan(ds: HBaseDataStore, strategy: QueryStrategy, processor: Option[LocalProcessor], reducer: Option[FeatureReducer])
      extends HBaseQueryPlan {
    override type Results = SimpleFeature
    override def ranges: Seq[RowRange] = Seq.empty
    override def scans: Seq[TableScan] = Seq.empty
    override def resultsToFeatures: ResultsToFeatures[SimpleFeature] =
      new IdentityResultsToFeatures(processor.map(_.sft).getOrElse(strategy.index.sft))
    override def sort: Option[Seq[(String, Boolean)]] = None
    override def maxFeatures: Option[Int] = None
    override def projection: Option[QueryReferenceSystems] = None
    override def scan(): CloseableIterator[SimpleFeature] = {
      val features = CloseableIterator.empty[SimpleFeature]
      // still need to apply the local processor (if any), for things like stats scans
      processor.fold(features)(_.apply(features))
    }

    override protected def threads: Int = 0
    override protected def singleTableScan(
        scan: TableScan,
        connection: Connection,
        threads: Int,
        timeout: Option[Timeout]): CloseableIterator[SimpleFeature] = CloseableIterator.empty
  }

  case class ScanPlan(
      ds: HBaseDataStore,
      strategy: QueryStrategy,
      ranges: Seq[RowRange],
      scans: Seq[TableScan],
      resultsToFeatures: ResultsToFeatures[Result],
      reducer: Option[FeatureReducer],
      sort: Option[Seq[(String, Boolean)]],
      maxFeatures: Option[Int],
      projection: Option[QueryReferenceSystems]
    ) extends HBaseQueryPlan {

    override type Results = Result

    override protected def threads: Int = ds.config.queries.threads

    override protected def singleTableScan(
        scan: TableScan,
        connection: Connection,
        threads: Int,
        timeout: Option[Timeout]): CloseableIterator[Result] = {
      HBaseBatchScan(this, connection, scan.table, scan.scans, threads, timeout)
    }
  }

  case class LocalProcessorScanPlan(
      ds: HBaseDataStore,
      strategy: QueryStrategy,
      ranges: Seq[RowRange],
      scans: Seq[TableScan],
      processor: LocalProcessor,
      resultsToFeatures: ResultsToFeatures[SimpleFeature],
      projection: Option[QueryReferenceSystems]
    ) extends HBaseQueryPlan {

    override type Results = SimpleFeature

    override def reducer: Option[FeatureReducer] = processor.reducer
    // handled by local processor
    override def sort: Option[Seq[(String, Boolean)]] = None
    override def maxFeatures: Option[Int] = None

    override protected def threads: Int = ds.config.queries.threads

    override protected def singleTableScan(
      scan: TableScan,
      connection: Connection,
      threads: Int,
      timeout: Option[Timeout]): CloseableIterator[SimpleFeature] = {
      val toFeatures = new HBaseResultsToFeatures(strategy.index, processor.sft)
      processor(HBaseBatchScan(this, connection, scan.table, scan.scans, threads, timeout).map(toFeatures.apply))
    }

    override protected def moreExplaining(explainer: Explainer): Unit = {
      import org.locationtech.geomesa.index.conf.QueryHints.RichHints
      // filter, transforms, sort, max features are all captured in the local processor so pull them out of the hints instead of the plan
      explainer(s"ECQL: ${processor.filter.fold("none")(FilterHelper.toString)}")
      explainer(s"Transform: ${strategy.hints.getTransform.fold("none")(t => s"${t._1} ${SimpleFeatureTypes.encodeType(t._2)}")}")
      explainer(s"Sort: ${strategy.hints.getSortFields.fold("none")(_.mkString(", "))}")
      explainer(s"Max Features: ${strategy.hints.getMaxFeatures.getOrElse("none")}")
    }
  }

  case class CoprocessorPlan(
      ds: HBaseDataStore,
      strategy: QueryStrategy,
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

    // client side processing is not relevant for coprocessors
    override def sort: Option[Seq[(String, Boolean)]] = None

    override protected def threads: Int = ds.config.coprocessors.threads

    override protected def singleTableScan(
        scan: TableScan,
        connection: Connection,
        threads: Int,
        timeout: Option[Timeout]): CloseableIterator[Array[Byte]] = {
      val scanThreads = if (maximizeThreads) { scan.scans.length } else { threads * 2 }
      CoprocessorBatchScan(this, connection, scan.table, scan.scans, coprocessorOptions, scanThreads, threads, timeout)
    }

    override protected def moreExplaining(explainer: Explainer): Unit =
      explainer("Coprocessor options: " + coprocessorOptions.map(m => s"[${m._1}:${m._2}]").mkString(", "))
  }
}
