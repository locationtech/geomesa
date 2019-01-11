/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{IteratorSetting, ScannerBase}
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.index.AccumuloQueryPlan.JoinFunction
import org.locationtech.geomesa.accumulo.util.BatchMultiScanner
import org.locationtech.geomesa.accumulo.{AccumuloFilterStrategyType, AccumuloQueryPlanType}
import org.locationtech.geomesa.index.PartitionParallelScan
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeature

sealed trait AccumuloQueryPlan extends AccumuloQueryPlanType {

  def tables: Seq[String]
  def columnFamilies: Seq[Text]
  def ranges: Seq[org.apache.accumulo.core.data.Range]
  def iterators: Seq[IteratorSetting]
  def numThreads: Int

  def join: Option[(JoinFunction, AccumuloQueryPlan)] = None

  override def explain(explainer: Explainer, prefix: String = ""): Unit =
    AccumuloQueryPlan.explain(this, explainer, prefix)

  protected def configure(scanner: ScannerBase): Unit = {
    iterators.foreach(scanner.addScanIterator)
    columnFamilies.foreach(scanner.fetchColumnFamily)
  }
}

object AccumuloQueryPlan extends LazyLogging {

  // scan result => range
  type JoinFunction = Entry[Key, Value] => org.apache.accumulo.core.data.Range

  def explain(plan: AccumuloQueryPlan, explainer: Explainer, prefix: String): Unit = {
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getName}")
    explainer(s"Tables: ${plan.tables.mkString(", ")}")
    explainer(s"Deduplicate: ${plan.hasDuplicates}")
    explainer(s"Column Families${if (plan.columnFamilies.isEmpty) ": all"
    else s" (${plan.columnFamilies.size}): ${plan.columnFamilies.take(20)}"}")
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
}

// plan that will not actually scan anything
case class EmptyPlan(filter: AccumuloFilterStrategyType) extends AccumuloQueryPlan {
  override val tables: Seq[String] = Seq.empty
  override val iterators: Seq[IteratorSetting] = Seq.empty
  override val ranges: Seq[org.apache.accumulo.core.data.Range] = Seq.empty
  override val columnFamilies: Seq[Text] = Seq.empty
  override val hasDuplicates: Boolean = false
  override val numThreads: Int = 0
  override def scan(ds: AccumuloDataStore): CloseableIterator[SimpleFeature] = CloseableIterator.empty
}

// single scan plan
case class ScanPlan(filter: AccumuloFilterStrategyType,
                    tables: Seq[String],
                    range: org.apache.accumulo.core.data.Range,
                    iterators: Seq[IteratorSetting],
                    columnFamilies: Seq[Text],
                    entriesToFeatures: Entry[Key, Value] => SimpleFeature,
                    override val reduce: Option[CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature]],
                    override val hasDuplicates: Boolean) extends AccumuloQueryPlan {

  import scala.collection.JavaConversions._

  override val numThreads = 1
  override val ranges = Seq(range)

  override def scan(ds: AccumuloDataStore): CloseableIterator[SimpleFeature] = {
    if (PartitionParallelScan.toBoolean.contains(true)) {
      // kick off all the scans at once
      tables.map(scanner(ds, _)).foldLeft(CloseableIterator.empty[SimpleFeature])(_ ++ _)
    } else {
      // kick off the scans sequentially as they finish
      SelfClosingIterator(tables.iterator).flatMap(scanner(ds, _))
    }
  }

  private def scanner(ds: AccumuloDataStore, table: String): CloseableIterator[SimpleFeature] = {
    val scanner = ds.connector.createScanner(table, ds.auths)
    scanner.setRange(range)
    configure(scanner)
    SelfClosingIterator(scanner.iterator.map(entriesToFeatures), scanner.close())
  }
}

// batch scan plan
case class BatchScanPlan(filter: AccumuloFilterStrategyType,
                         tables: Seq[String],
                         ranges: Seq[org.apache.accumulo.core.data.Range],
                         iterators: Seq[IteratorSetting],
                         columnFamilies: Seq[Text],
                         entriesToFeatures: Entry[Key, Value] => SimpleFeature,
                         override val reduce: Option[CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature]],
                         numThreads: Int,
                         override val hasDuplicates: Boolean) extends AccumuloQueryPlan {

  import scala.collection.JavaConversions._

  override def scan(ds: AccumuloDataStore): CloseableIterator[SimpleFeature] =
    scanEntries(ds).map(entriesToFeatures)

  def scanEntries(ds: AccumuloDataStore, auths: Option[Authorizations] = None): CloseableIterator[Entry[Key, Value]] = {
    if (PartitionParallelScan.toBoolean.contains(true)) {
      // kick off all the scans at once
      tables.map(scanner(ds, auths, _)).foldLeft(CloseableIterator.empty[Entry[Key, Value]])(_ ++ _)
    } else {
      // kick off the scans sequentially as they finish
      SelfClosingIterator(tables.iterator).flatMap(scanner(ds, auths, _))
    }
  }

  private def scanner(ds: AccumuloDataStore,
                      auths: Option[Authorizations],
                      table: String): CloseableIterator[Entry[Key, Value]] = {
    val scanner = ds.connector.createBatchScanner(table, auths.getOrElse(ds.auths), numThreads)
    scanner.setRanges(ranges)
    configure(scanner)
    SelfClosingIterator(scanner.iterator, scanner.close())
  }
}

// join on multiple tables - requires multiple scans
case class JoinPlan(filter: AccumuloFilterStrategyType,
                    tables: Seq[String],
                    ranges: Seq[org.apache.accumulo.core.data.Range],
                    iterators: Seq[IteratorSetting],
                    columnFamilies: Seq[Text],
                    numThreads: Int,
                    override val hasDuplicates: Boolean,
                    joinFunction: JoinFunction,
                    joinQuery: BatchScanPlan) extends AccumuloQueryPlan {

  override val join = Some((joinFunction, joinQuery))
  override def reduce: Option[CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature]] = joinQuery.reduce

  override def scan(ds: AccumuloDataStore): CloseableIterator[SimpleFeature] = {
    val joinTables = joinQuery.tables.iterator
    if (PartitionParallelScan.toBoolean.contains(true)) {
      // kick off all the scans at once
      tables.map(scanner(ds, _, joinTables.next)).foldLeft(CloseableIterator.empty[SimpleFeature])(_ ++ _)
    } else {
      // kick off the scans sequentially as they finish
      SelfClosingIterator(tables.iterator).flatMap(scanner(ds, _, joinTables.next))
    }
  }

  private def scanner(ds: AccumuloDataStore, table: String, joinTable: String): CloseableIterator[SimpleFeature] = {
    import scala.collection.JavaConverters._

    val primary = if (ranges.lengthCompare(1) == 0) {
      val scanner = ds.connector.createScanner(table, ds.auths)
      scanner.setRange(ranges.head)
      scanner
    } else {
      val scanner = ds.connector.createBatchScanner(table, ds.auths, numThreads)
      scanner.setRanges(ranges.asJava)
      scanner
    }
    configure(primary)
    val bms = new BatchMultiScanner(ds, primary, joinQuery.copy(tables = Seq(joinTable)), joinFunction)
    SelfClosingIterator(bms.iterator.map(joinQuery.entriesToFeatures), bms.close())
  }
}
