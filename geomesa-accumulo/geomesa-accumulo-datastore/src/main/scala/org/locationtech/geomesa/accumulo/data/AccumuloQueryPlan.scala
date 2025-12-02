/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.ScannerBase.ConsistencyLevel
import org.apache.accumulo.core.client.{AccumuloClient, IteratorSetting, ScannerBase}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.accumulo.data.AccumuloIndexAdapter.AccumuloResultsToFeatures
import org.locationtech.geomesa.accumulo.util.BatchMultiScanner
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, QueryStrategyPlan, ResultsToFeatures}
import org.locationtech.geomesa.index.api.QueryStrategy
import org.locationtech.geomesa.index.planning.LocalQueryRunner.{LocalProcessor, LocalProcessorPlan}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.index.utils.ThreadManagement.{LowLevelScanner, ManagedScan, Timeout}
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}

import java.util.Map.Entry

/**
  * Accumulo-specific query plan
  */
sealed trait AccumuloQueryPlan extends QueryStrategyPlan {

  def tables: Seq[String]
  def columnFamily: Option[Text]
  def ranges: Seq[org.apache.accumulo.core.data.Range]
  def iterators: Seq[IteratorSetting]
  def numThreads: Int

  def join: Option[(AccumuloQueryPlan.JoinFunction, AccumuloQueryPlan)] = None

  override def explain(explainer: Explainer): Unit = AccumuloQueryPlan.explain(this, explainer)
}

object AccumuloQueryPlan extends LazyLogging {

  import scala.collection.JavaConverters._

  // scan result => range
  type JoinFunction = Entry[Key, Value] => org.apache.accumulo.core.data.Range

  def explain(plan: AccumuloQueryPlan, explainer: Explainer, prefix: String = ""): Unit = {
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getSimpleName}")
    explainer(s"Tables: ${plan.tables.mkString(", ")}")
    explainer(s"Column Families: ${plan.columnFamily.getOrElse("all")}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(rangeToString).mkString(", ")}")
    explainer(s"Iterators (${plan.iterators.size}):", plan.iterators.map(i => () => i.toString))
    plan.join.foreach { j => explain(j._2, explainer, "Join ") }
    explainer(s"Reduce: ${plan.reducer.getOrElse("none")}")
    explainer.popLevel()
  }

  // converts a range to a printable string - only includes the row
  private def rangeToString(r: org.apache.accumulo.core.data.Range): String = {
    val a = if (r.isStartKeyInclusive) "[" else "("
    val z = if (r.isEndKeyInclusive) "]" else ")"
    val start = if (r.isInfiniteStartKey) "-inf" else keyToString(r.getStartKey)
    val stop = if (r.isInfiniteStopKey) "+inf" else keyToString(r.getEndKey)
    s"$a$start::$stop$z"
  }

  // converts a key to a printable string - only includes the row
  private def keyToString(k: Key): String =
    Key.toPrintableString(k.getRow.getBytes, 0, k.getRow.getLength, k.getRow.getLength)

  // plan that will not actually scan anything
  case class EmptyPlan(strategy: QueryStrategy, reducer: Option[FeatureReducer] = None) extends AccumuloQueryPlan {
    override type Results = Entry[Key, Value]
    override def tables: Seq[String] = Seq.empty
    override def iterators: Seq[IteratorSetting] = Seq.empty
    override def ranges: Seq[org.apache.accumulo.core.data.Range] = Seq.empty
    override def columnFamily: Option[Text] = None
    override def numThreads: Int = 0
    override def resultsToFeatures: ResultsToFeatures[Entry[Key, Value]] = ResultsToFeatures.empty
    override def sort: Option[Seq[(String, Boolean)]] = None
    override def maxFeatures: Option[Int] = None
    override def projection: Option[QueryReferenceSystems] = None
    override def scan(): CloseableIterator[Entry[Key, Value]] = CloseableIterator.empty
  }


  // batch scan plan
  abstract class AbstractBatchScanPlan(
      tables: Seq[String],
      ranges: Seq[org.apache.accumulo.core.data.Range],
      iterators: Seq[IteratorSetting],
      columnFamily: Option[Text],
      numThreads: Int
    ) extends AccumuloQueryPlan {

    /**
     * Scan with pre-computed auths
     *
     * @param helper scan helper
     * @return
     */
    private[AccumuloQueryPlan] def scan(helper: ScanHelper): CloseableIterator[Entry[Key, Value]] = {
      if (helper.parallel) {
        // kick off all the scans at once
        tables.map(scanner(_, helper)).foldLeft(CloseableIterator.empty[Entry[Key, Value]])(_ concat _)
      } else {
        // kick off the scans sequentially as they finish
        SelfClosingIterator(tables.iterator).flatMap(scanner(_, helper))
      }
    }

    /**
     * Scan a table
     *
     * @param table table
     * @param helper scan helper
     * @return
     */
    private def scanner(table: String, helper: ScanHelper): CloseableIterator[Entry[Key, Value]] = {
      val scanner = helper.client.createBatchScanner(table, helper.auths, numThreads)
      scanner.setRanges(ranges.asJava)
      iterators.foreach(scanner.addScanIterator)
      columnFamily.foreach(scanner.fetchColumnFamily)
      helper.consistency.foreach(scanner.setConsistencyLevel)
      helper.timeout match {
        case None => new ScanIterator(scanner)
        case Some(t) => new ManagedScan(new AccumuloScanner(scanner), t, this)
      }
    }
  }

  // batch scan plan
  case class BatchScanPlan(
      ds: AccumuloDataStore,
      strategy: QueryStrategy,
      tables: Seq[String],
      ranges: Seq[org.apache.accumulo.core.data.Range],
      iterators: Seq[IteratorSetting],
      columnFamily: Option[Text],
      resultsToFeatures: ResultsToFeatures[Entry[Key, Value]],
      reducer: Option[FeatureReducer],
      sort: Option[Seq[(String, Boolean)]],
      maxFeatures: Option[Int],
      projection: Option[QueryReferenceSystems],
      numThreads: Int
    ) extends AbstractBatchScanPlan(tables, ranges, iterators, columnFamily, numThreads) {

    override type Results = Entry[Key, Value]

    override def scan(): CloseableIterator[Entry[Key, Value]] = {
      // query guard hook - also handles full table scan checks
      strategy.runGuards(ds)
      // note: calculate auths and convert the relative timeout to an absolute timeout up front
      scan(ScanHelper(ds))
    }
  }

  // batch scan plan
  case class BatchScanLocalProcessorPlan(
      ds: AccumuloDataStore,
      strategy: QueryStrategy,
      tables: Seq[String],
      ranges: Seq[org.apache.accumulo.core.data.Range],
      iterators: Seq[IteratorSetting],
      columnFamily: Option[Text],
      processor: LocalProcessor,
      projection: Option[QueryReferenceSystems],
      numThreads: Int
    ) extends AbstractBatchScanPlan(tables, ranges, iterators, columnFamily, numThreads) with LocalProcessorPlan {

    override def scan(): CloseableIterator[SimpleFeature] = {
      // query guard hook - also handles full table scan checks
      strategy.runGuards(ds)
      val toFeatures = AccumuloResultsToFeatures(strategy.index, processor.sft)
      // note: calculate auths and convert the relative timeout to an absolute timeout up front
      processor(scan(ScanHelper(ds)).map(toFeatures.apply))
    }
  }

  // join on multiple tables - requires multiple scans
  case class JoinPlan(
      ds: AccumuloDataStore,
      strategy: QueryStrategy,
      tables: Seq[String],
      ranges: Seq[org.apache.accumulo.core.data.Range],
      iterators: Seq[IteratorSetting],
      columnFamily: Option[Text],
      numThreads: Int,
      joinFunction: JoinFunction,
      joinQuery: BatchScanPlan,
      processor: LocalProcessor,
      projection: Option[QueryReferenceSystems],
    ) extends AccumuloQueryPlan with LocalProcessorPlan {

    override val join: Some[(JoinFunction, BatchScanPlan)] = Some((joinFunction, joinQuery))

    override def scan(): CloseableIterator[SimpleFeature] = {
      // query guard hook - also handles full table scan checks
      strategy.runGuards(ds)
      // calculate auths and convert the relative timeout to an absolute timeout up front
      val helper = ScanHelper(ds)
      val joinTables = joinQuery.tables.iterator
      val entries =
        if (helper.parallel) {
          // kick off all the scans at once
          tables.map(scanner( _, joinTables.next, helper)).foldLeft(CloseableIterator.empty[Entry[Key, Value]])(_ concat _)
        } else {
          // kick off the scans sequentially as they finish
          SelfClosingIterator(tables.iterator).flatMap(scanner(_, joinTables.next, helper))
        }
      val features = entries.map(joinQuery.resultsToFeatures.apply)
      processor(features)
    }

    private def scanner(table: String, joinTable: String, helper: ScanHelper): CloseableIterator[Entry[Key, Value]] = {
      val primary = if (ranges.lengthCompare(1) == 0) {
        val scanner = helper.client.createScanner(table, helper.auths)
        scanner.setRange(ranges.head)
        scanner
      } else {
        val scanner = helper.client.createBatchScanner(table, helper.auths, numThreads)
        scanner.setRanges(ranges.asJava)
        scanner
      }
      iterators.foreach(primary.addScanIterator)
      columnFamily.foreach(primary.fetchColumnFamily)
      helper.consistency.foreach(primary.setConsistencyLevel)
      val join: Seq[Entry[Key, Value]] => CloseableIterator[Entry[Key, Value]] =
        entries => joinQuery.copy(tables = Seq(joinTable), ranges = entries.map(joinFunction)).scan(helper)
      new BatchMultiScanner(primary, join)
    }
  }

  private case class ScanHelper(
    client: AccumuloClient, auths: Authorizations, timeout: Option[Timeout], parallel: Boolean, consistency: Option[ConsistencyLevel])

  private object ScanHelper {
    def apply(ds: AccumuloDataStore): ScanHelper = {
      // convert the relative timeout to an absolute timeout up front
      val timeout = ds.config.queries.timeout.map(Timeout.apply)
      // calculate authorizations up front so that multi-threading doesn't mess up auth providers
      ScanHelper(ds.client, ds.auths, timeout, ds.config.queries.parallelPartitionScans, ds.config.queries.consistency)
    }
  }

  private class ScanIterator(scanner: ScannerBase) extends CloseableIterator[Entry[Key, Value]] {
    private val iter = scanner.iterator.asScala
    override def hasNext: Boolean = iter.hasNext
    override def next(): Entry[Key, Value] = iter.next()
    override def close(): Unit = scanner.close()
  }

  private class AccumuloScanner(scanner: ScannerBase) extends LowLevelScanner[Entry[Key, Value]] {
    override def iterator: Iterator[Entry[Key, Value]] = scanner.iterator.asScala
    override def close(): Unit = scanner.close()
  }
}
