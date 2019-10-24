/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{Connector, IteratorSetting, ScannerBase}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.util.BatchMultiScanner
import org.locationtech.geomesa.index.PartitionParallelScan
import org.locationtech.geomesa.index.api.QueryPlan.Reducer
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeature

/**
  * Accumulo-specific query plan
  */
sealed trait AccumuloQueryPlan extends QueryPlan[AccumuloDataStore] {

  def tables: Seq[String]
  def columnFamily: Option[Text]
  def ranges: Seq[org.apache.accumulo.core.data.Range]
  def iterators: Seq[IteratorSetting]
  def numThreads: Int

  def join: Option[(AccumuloQueryPlan.JoinFunction, AccumuloQueryPlan)] = None

  override def explain(explainer: Explainer, prefix: String = ""): Unit =
    AccumuloQueryPlan.explain(this, explainer, prefix)

  protected def configure(scanner: ScannerBase): Unit = {
    iterators.foreach(scanner.addScanIterator)
    columnFamily.foreach(scanner.fetchColumnFamily)
  }
}

object AccumuloQueryPlan extends LazyLogging {

  // scan result => range
  type JoinFunction = Entry[Key, Value] => org.apache.accumulo.core.data.Range

  def explain(plan: AccumuloQueryPlan, explainer: Explainer, prefix: String): Unit = {
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getSimpleName}")
    explainer(s"Tables: ${plan.tables.mkString(", ")}")
    explainer(s"Column Families: ${plan.columnFamily.getOrElse("all")}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(rangeToString).mkString(", ")}")
    explainer(s"Iterators (${plan.iterators.size}):", plan.iterators.map(i => () => i.toString))
    plan.join.foreach { j => explain(j._2, explainer, "Join ") }
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
  case class EmptyPlan(
      filter: FilterStrategy,
      override val reduce: Option[Reducer] = None
    ) extends AccumuloQueryPlan {
    override val tables: Seq[String] = Seq.empty
    override val iterators: Seq[IteratorSetting] = Seq.empty
    override val ranges: Seq[org.apache.accumulo.core.data.Range] = Seq.empty
    override val columnFamily: Option[Text] = None
    override val numThreads: Int = 0
    override def scan(ds: AccumuloDataStore): CloseableIterator[SimpleFeature] = CloseableIterator.empty
  }

  // sequential scan plan
  case class ScanPlan(filter: FilterStrategy,
                      tables: Seq[String],
                      range: org.apache.accumulo.core.data.Range,
                      iterators: Seq[IteratorSetting],
                      columnFamily: Option[Text],
                      entriesToFeatures: Entry[Key, Value] => SimpleFeature,
                      override val reduce: Option[Reducer]) extends AccumuloQueryPlan {

    import scala.collection.JavaConversions._

    override val numThreads = 1
    override val ranges = Seq(range)

    override def scan(ds: AccumuloDataStore): CloseableIterator[SimpleFeature] = {
      // calculate authorizations up front so that multi-threading doesn't mess up auth providers
      val auths = ds.auths
      if (PartitionParallelScan.toBoolean.contains(true)) {
        // kick off all the scans at once
        tables.map(scanner(ds.connector, _, auths)).foldLeft(CloseableIterator.empty[SimpleFeature])(_ ++ _)
      } else {
        // kick off the scans sequentially as they finish
        SelfClosingIterator(tables.iterator).flatMap(scanner(ds.connector, _, auths))
      }
    }

    private def scanner(connector: Connector,
                        table: String,
                        auths: Authorizations): CloseableIterator[SimpleFeature] = {
      val scanner = connector.createScanner(table, auths)
      scanner.setRange(range)
      configure(scanner)
      SelfClosingIterator(scanner.iterator.map(entriesToFeatures), scanner.close())
    }
  }

  // batch scan plan
  case class BatchScanPlan(filter: FilterStrategy,
                           tables: Seq[String],
                           ranges: Seq[org.apache.accumulo.core.data.Range],
                           iterators: Seq[IteratorSetting],
                           columnFamily: Option[Text],
                           entriesToFeatures: Entry[Key, Value] => SimpleFeature,
                           override val reduce: Option[Reducer],
                           numThreads: Int) extends AccumuloQueryPlan {

    import scala.collection.JavaConversions._

    override def scan(ds: AccumuloDataStore): CloseableIterator[SimpleFeature] =
      // calculate authorizations up front so that multi-threading doesn't mess up auth providers
      scanEntries(ds.connector, ds.auths).map(entriesToFeatures)

    def scanEntries(connector: Connector, auths: Authorizations): CloseableIterator[Entry[Key, Value]] = {
      if (PartitionParallelScan.toBoolean.contains(true)) {
        // kick off all the scans at once
        tables.map(scanner(connector, _, auths)).foldLeft(CloseableIterator.empty[Entry[Key, Value]])(_ ++ _)
      } else {
        // kick off the scans sequentially as they finish
        SelfClosingIterator(tables.iterator).flatMap(scanner(connector, _, auths))
      }
    }

    private def scanner(connector: Connector,
                        table: String,
                        auths: Authorizations): CloseableIterator[Entry[Key, Value]] = {
      val scanner = connector.createBatchScanner(table, auths, numThreads)
      scanner.setRanges(ranges)
      configure(scanner)
      SelfClosingIterator(scanner.iterator, scanner.close())
    }
  }

  // join on multiple tables - requires multiple scans
  case class JoinPlan(filter: FilterStrategy,
                      tables: Seq[String],
                      ranges: Seq[org.apache.accumulo.core.data.Range],
                      iterators: Seq[IteratorSetting],
                      columnFamily: Option[Text],
                      numThreads: Int,
                      joinFunction: JoinFunction,
                      joinQuery: BatchScanPlan) extends AccumuloQueryPlan {

    override val join = Some((joinFunction, joinQuery))
    override def reduce: Option[Reducer] = joinQuery.reduce

    override def scan(ds: AccumuloDataStore): CloseableIterator[SimpleFeature] = {
      // calculate authorizations up front so that multi-threading doesn't mess up auth providers
      val auths = ds.auths
      val joinTables = joinQuery.tables.iterator
      if (PartitionParallelScan.toBoolean.contains(true)) {
        // kick off all the scans at once
        tables.map(scanner(ds.connector, _, joinTables.next, auths))
            .foldLeft(CloseableIterator.empty[SimpleFeature])(_ ++ _)
      } else {
        // kick off the scans sequentially as they finish
        SelfClosingIterator(tables.iterator).flatMap(scanner(ds.connector, _, joinTables.next, auths))
      }
    }

    private def scanner(connector: Connector,
                        table: String,
                        joinTable: String,
                        auths: Authorizations): CloseableIterator[SimpleFeature] = {
      import scala.collection.JavaConverters._

      val primary = if (ranges.lengthCompare(1) == 0) {
        val scanner = connector.createScanner(table, auths)
        scanner.setRange(ranges.head)
        scanner
      } else {
        val scanner = connector.createBatchScanner(table, auths, numThreads)
        scanner.setRanges(ranges.asJava)
        scanner
      }
      configure(primary)
      val join = joinQuery.copy(tables = Seq(joinTable))
      val bms = new BatchMultiScanner(connector, primary, join, joinFunction, auths)
      SelfClosingIterator(bms.iterator.map(joinQuery.entriesToFeatures), bms.close())
    }
  }
}
