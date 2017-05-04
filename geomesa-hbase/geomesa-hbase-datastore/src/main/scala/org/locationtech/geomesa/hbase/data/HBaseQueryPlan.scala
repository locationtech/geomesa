/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import com.google.protobuf.ByteString
import org.apache.commons.lang.NotImplementedException
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter.{FilterList, Filter => HFilter}
import org.geotools.factory.Hints
import org.locationtech.geomesa.hbase.coprocessor.KryoLazyDensityCoprocessor
import org.locationtech.geomesa.hbase.driver.KryoLazyDensityDriver
import org.locationtech.geomesa.hbase.utils.HBaseBatchScan
import org.locationtech.geomesa.hbase.{HBaseFilterStrategyType, HBaseQueryPlanType}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait HBaseQueryPlan extends HBaseQueryPlanType {
  def filter: HBaseFilterStrategyType
  def table: TableName
  def ranges: Seq[Query]
  // note: entriesToFeatures encapsulates ecql and transform
  def resultsToFeatures: Iterator[Result] => Iterator[SimpleFeature]

  override def explain(explainer: Explainer, prefix: String): Unit =
    HBaseQueryPlan.explain(this, explainer, prefix)
}

object HBaseQueryPlan {

  def explain(plan: HBaseQueryPlan, explainer: Explainer, prefix: String): Unit = {
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getName}")
    explainer(s"Table: ${Option(plan.table).orNull}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(rangeToString).mkString(", ")}")
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
  override val resultsToFeatures: Iterator[Result] => Iterator[SimpleFeature] = (i) => Iterator.empty
  override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = CloseableIterator.empty
}

case class ScanPlan(filter: HBaseFilterStrategyType,
                    table: TableName,
                    ranges: Seq[Scan],
                    resultsToFeatures: Iterator[Result] => Iterator[SimpleFeature]) extends HBaseQueryPlan {
  override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = {
    ranges.foreach(ds.applySecurity)
    val results = new HBaseBatchScan(ds.connection, table, ranges, ds.config.queryThreads, 100000)
    SelfClosingIterator(resultsToFeatures(results), results.close)
  }
}

case class CoprocessorPlan(sft: SimpleFeatureType,
                           filter: HBaseFilterStrategyType,
                           hints: Hints,
                           table: TableName,
                           ranges: Seq[Scan],
                           hbaseFilters: Seq[HFilter],
                           resultsToFeatures: Iterator[Result] => Iterator[SimpleFeature]) extends HBaseQueryPlan  {
  /**
    * Runs the query plain against the underlying database, returning the raw entries
    *
    * @param ds data store - provides connection object and metadata
    * @return
    */
  override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    if (hints.isDensityQuery) {
      val is: Map[String, String] = KryoLazyDensityCoprocessor.configure(sft, ranges, hbaseFilters, hints)
      val byteArray: Array[Byte] = KryoLazyDensityCoprocessor.serializeOptions(is)
      val hbaseTable = ds.connection.getTable(table)
      val client = new KryoLazyDensityDriver()
      val result: List[ByteString] = client.kryoLazyDensityFilter(hbaseTable, byteArray)
      result.map(r => KryoLazyDensityCoprocessor.bytesToFeatures(r.toByteArray)).toIterator
    } else {
      throw new NotImplementedException()
    }
  }
}

case class GetPlan(filter: HBaseFilterStrategyType,
                   table: TableName,
                   ranges: Seq[Get],
                   remoteFilters: Seq[HFilter] = Nil,
                   resultsToFeatures: Iterator[Result] => Iterator[SimpleFeature]) extends HBaseQueryPlan {
  override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = {
    import scala.collection.JavaConversions._
    val filterList = new FilterList()
    remoteFilters.foreach { filter => filterList.addFilter(filter) }
    ranges.foreach { range =>
      range.setFilter(filterList)
      ds.applySecurity(range)
    }
    val get = ds.connection.getTable(table)
    SelfClosingIterator(resultsToFeatures(get.get(ranges).iterator), get.close)
  }
}
