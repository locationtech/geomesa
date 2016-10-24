/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.{BatchScanner, IteratorSetting, Scanner}
import org.apache.accumulo.core.data.{Key, Value, Range => aRange}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.AccumuloConnectorCreator
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex.AccumuloFilterStrategy
import org.locationtech.geomesa.accumulo.index.QueryPlan.{FeatureFunction, JoinFunction}
import org.locationtech.geomesa.accumulo.index.QueryPlanner.KVIter
import org.locationtech.geomesa.accumulo.util.{BatchMultiScanner, CloseableIterator, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeature

object QueryPlan {

  type JoinFunction = (Entry[Key, Value]) => aRange
  type FeatureFunction = (Entry[Key, Value]) => SimpleFeature

  def configureBatchScanner(bs: BatchScanner, qp: QueryPlan) {
    import scala.collection.JavaConversions._
    bs.setRanges(qp.ranges)
    qp.iterators.foreach(bs.addScanIterator)
    qp.columnFamilies.foreach(bs.fetchColumnFamily)
  }

  def configureScanner(scanner: Scanner, qp: QueryPlan) {
    qp.ranges.headOption.foreach(scanner.setRange)
    qp.iterators.foreach(scanner.addScanIterator)
    qp.columnFamilies.foreach(scanner.fetchColumnFamily)
  }
}

sealed trait QueryPlan extends LazyLogging {

  def filter: AccumuloFilterStrategy
  def table: String
  def ranges: Seq[aRange]
  def iterators: Seq[IteratorSetting]
  def columnFamilies: Seq[Text]
  def numThreads: Int
  def hasDuplicates: Boolean
  def kvsToFeatures: FeatureFunction

  def join: Option[(JoinFunction, QueryPlan)] = None

  def execute(acc: AccumuloConnectorCreator): KVIter = {
    try {
      if (ranges.isEmpty) {
        CloseableIterator.empty
      } else {
        executeInternal(acc)
      }
    } catch {
      case e: Exception =>
        logger.error(s"Error in creating scanner: $e", e)
        // since GeoTools would eat the error and return no records anyway,
        // there's no harm in returning an empty iterator.
        CloseableIterator.empty
    }
  }

  protected def executeInternal(acc: AccumuloConnectorCreator): KVIter
}

// plan that will not actually scan anything
case class EmptyPlan(filter: AccumuloFilterStrategy) extends QueryPlan {
  override val table: String = ""
  override val iterators: Seq[IteratorSetting] = Seq.empty
  override val kvsToFeatures: FeatureFunction = (_) => null
  override val ranges: Seq[aRange] = Seq.empty
  override val columnFamilies: Seq[Text] = Seq.empty
  override val hasDuplicates: Boolean = false
  override val numThreads: Int = 0

  override protected def executeInternal(acc: AccumuloConnectorCreator): KVIter = CloseableIterator.empty
}

// single scan plan
case class ScanPlan(filter: AccumuloFilterStrategy,
                    table: String,
                    range: aRange,
                    iterators: Seq[IteratorSetting],
                    columnFamilies: Seq[Text],
                    kvsToFeatures: FeatureFunction,
                    hasDuplicates: Boolean) extends QueryPlan {

  override val numThreads = 1
  override val ranges = Seq(range)

  override protected def executeInternal(acc: AccumuloConnectorCreator): KVIter = {
    val scanner = acc.getScanner(table)
    QueryPlan.configureScanner(scanner, this)
    SelfClosingIterator(scanner)
  }
}

// batch scan plan
case class BatchScanPlan(filter: AccumuloFilterStrategy,
                         table: String,
                         ranges: Seq[aRange],
                         iterators: Seq[IteratorSetting],
                         columnFamilies: Seq[Text],
                         kvsToFeatures: FeatureFunction,
                         numThreads: Int,
                         hasDuplicates: Boolean) extends QueryPlan {

  override protected def executeInternal(acc: AccumuloConnectorCreator): KVIter = {
    val batchScanner = acc.getBatchScanner(table, numThreads)
    QueryPlan.configureBatchScanner(batchScanner, this)
    SelfClosingIterator(batchScanner)
  }
}

// join on multiple tables - requires multiple scans
case class JoinPlan(filter: AccumuloFilterStrategy,
                    table: String,
                    ranges: Seq[aRange],
                    iterators: Seq[IteratorSetting],
                    columnFamilies: Seq[Text],
                    numThreads: Int,
                    hasDuplicates: Boolean,
                    joinFunction: JoinFunction,
                    joinQuery: BatchScanPlan) extends QueryPlan {

  override def kvsToFeatures: FeatureFunction = joinQuery.kvsToFeatures
  override val join = Some((joinFunction, joinQuery))

  override protected def executeInternal(acc: AccumuloConnectorCreator): KVIter = {
    val primary = if (ranges.length == 1) {
      val scanner = acc.getScanner(table)
      QueryPlan.configureScanner(scanner, this)
      scanner
    } else {
      val batchScanner = acc.getBatchScanner(table, numThreads)
      QueryPlan.configureBatchScanner(batchScanner, this)
      batchScanner
    }

    val bms = new BatchMultiScanner(acc, primary, joinQuery, joinFunction)
    SelfClosingIterator(bms.iterator, () => bms.close())
  }
}
