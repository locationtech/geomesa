/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{IteratorSetting, ScannerBase}
import org.apache.accumulo.core.data.{Key, Value, Range => aRange}
import org.apache.accumulo.core.security.Authorizations
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.AccumuloProperties.AccumuloQueryProperties
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.index.AccumuloQueryPlan.JoinFunction
import org.locationtech.geomesa.accumulo.util.BatchMultiScanner
import org.locationtech.geomesa.accumulo.{AccumuloFilterStrategyType, AccumuloQueryPlanType}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeature

sealed trait AccumuloQueryPlan extends AccumuloQueryPlanType {

  def table: String
  def columnFamilies: Seq[Text]
  def ranges: Seq[aRange]
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

  type JoinFunction = (Entry[Key, Value]) => aRange

  def explain(plan: AccumuloQueryPlan, explainer: Explainer, prefix: String): Unit = {
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getName}")
    explainer(s"Table: ${plan.table}")
    explainer(s"Deduplicate: ${plan.hasDuplicates}")
    explainer(s"Column Families${if (plan.columnFamilies.isEmpty) ": all"
    else s" (${plan.columnFamilies.size}): ${plan.columnFamilies.take(20)}"}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(rangeToString).mkString(", ")}")
    explainer(s"Iterators (${plan.iterators.size}):", plan.iterators.map(i => () => i.toString))
    plan.join.foreach { j => explain(j._2, explainer, "Join ") }
    explainer.popLevel()
  }

  // converts a range to a printable string - only includes the row
  private def rangeToString(r: aRange): String = {
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
  override val table: String = ""
  override val iterators: Seq[IteratorSetting] = Seq.empty
  override val ranges: Seq[aRange] = Seq.empty
  override val columnFamilies: Seq[Text] = Seq.empty
  override val hasDuplicates: Boolean = false
  override val numThreads: Int = 0
  override def scan(ds: AccumuloDataStore): CloseableIterator[SimpleFeature] = CloseableIterator.empty
}

// single scan plan
case class ScanPlan(filter: AccumuloFilterStrategyType,
                    table: String,
                    range: aRange,
                    iterators: Seq[IteratorSetting],
                    columnFamilies: Seq[Text],
                    entriesToFeatures: (Entry[Key, Value]) => SimpleFeature,
                    override val reduce: Option[(CloseableIterator[SimpleFeature]) => CloseableIterator[SimpleFeature]],
                    override val hasDuplicates: Boolean) extends AccumuloQueryPlan {

  import scala.collection.JavaConversions._

  override val numThreads = 1
  override val ranges = Seq(range)

  override def scan(ds: AccumuloDataStore): CloseableIterator[SimpleFeature] = {
    val scanner = ds.connector.createScanner(table, ds.auths)
    scanner.setRange(range)
    configure(scanner)
    SelfClosingIterator(scanner.iterator.map(entriesToFeatures), scanner.close())
  }
}

// batch scan plan
case class BatchScanPlan(filter: AccumuloFilterStrategyType,
                         table: String,
                         ranges: Seq[aRange],
                         iterators: Seq[IteratorSetting],
                         columnFamilies: Seq[Text],
                         entriesToFeatures: (Entry[Key, Value]) => SimpleFeature,
                         override val reduce: Option[(CloseableIterator[SimpleFeature]) => CloseableIterator[SimpleFeature]],
                         numThreads: Int,
                         override val hasDuplicates: Boolean) extends AccumuloQueryPlan {

  import scala.collection.JavaConversions._

  override def scan(ds: AccumuloDataStore): CloseableIterator[SimpleFeature] =
    scanEntries(ds).map(entriesToFeatures)

  def scanEntries(ds: AccumuloDataStore, auths: Option[Authorizations] = None): CloseableIterator[Entry[Key, Value]] = {
    if (ranges.isEmpty) { CloseableIterator.empty } else {
      val batchRanges = AccumuloQueryProperties.SCAN_BATCH_RANGES.option.map(_.toInt).getOrElse(Int.MaxValue)
      val batched = ranges.grouped(batchRanges)
      SelfClosingIterator(batched).flatMap { ranges =>
        val scanner = ds.connector.createBatchScanner(table, auths.getOrElse(ds.auths), numThreads)
        scanner.setRanges(ranges)
        configure(scanner)
        SelfClosingIterator(scanner.iterator, scanner.close())
      }
    }
  }
}

// join on multiple tables - requires multiple scans
case class JoinPlan(filter: AccumuloFilterStrategyType,
                    table: String,
                    ranges: Seq[aRange],
                    iterators: Seq[IteratorSetting],
                    columnFamilies: Seq[Text],
                    numThreads: Int,
                    override val hasDuplicates: Boolean,
                    joinFunction: JoinFunction,
                    joinQuery: BatchScanPlan) extends AccumuloQueryPlan {

  override val join = Some((joinFunction, joinQuery))
  override def reduce: Option[(CloseableIterator[SimpleFeature]) => CloseableIterator[SimpleFeature]] = joinQuery.reduce

  override def scan(ds: AccumuloDataStore): CloseableIterator[SimpleFeature] = {
    import scala.collection.JavaConversions._

    val primary = if (ranges.length == 1) {
      val scanner = ds.connector.createScanner(table, ds.auths)
      scanner.setRange(ranges.head)
      scanner
    } else {
      val scanner = ds.connector.createBatchScanner(table, ds.auths, numThreads)
      scanner.setRanges(ranges)
      scanner
    }
    configure(primary)

    val bms = new BatchMultiScanner(ds, primary, joinQuery, joinFunction)
    SelfClosingIterator(bms.iterator.map(joinQuery.entriesToFeatures), bms.close())
  }
}
