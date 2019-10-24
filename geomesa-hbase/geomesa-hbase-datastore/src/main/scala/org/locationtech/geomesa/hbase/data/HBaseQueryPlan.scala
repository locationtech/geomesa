/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.hbase.coprocessor.CoprocessorConfig
import org.locationtech.geomesa.hbase.utils.HBaseBatchScan
import org.locationtech.geomesa.index.PartitionParallelScan
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeature

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
  case class EmptyPlan(
      filter: FilterStrategy,
      resultsToFeatures: CloseableIterator[Result] => CloseableIterator[SimpleFeature]
    ) extends HBaseQueryPlan {
    override val tables: Seq[TableName] = Seq.empty
    override val ranges: Seq[Scan] = Seq.empty
    override val scans: Seq[Scan] = Seq.empty
    override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] =
      resultsToFeatures(CloseableIterator.empty)
  }

  case class ScanPlan(filter: FilterStrategy,
                      tables: Seq[TableName],
                      ranges: Seq[Scan],
                      scans: Seq[Scan],
                      resultsToFeatures: CloseableIterator[Result] => CloseableIterator[SimpleFeature])
      extends HBaseQueryPlan {

    override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = {
      // note: we have to copy the ranges for each scan, except the last one (since it won't conflict at that point)
      val iter = tables.iterator
      val scans = iter.map(singleTableScan(ds, _, copyScans = iter.hasNext))

      if (PartitionParallelScan.toBoolean.contains(true)) {
        // kick off all the scans at once
        scans.foldLeft(CloseableIterator.empty[SimpleFeature])(_ ++ _)
      } else {
        // kick off the scans sequentially as they finish
        SelfClosingIterator(scans).flatMap(s => s)
      }
    }

    private def singleTableScan(ds: HBaseDataStore,
                                table: TableName,
                                copyScans: Boolean): CloseableIterator[SimpleFeature] = {
      val s = if (copyScans) { scans.map(new Scan(_)) } else { scans }
      val results = HBaseBatchScan(ds.connection, table, s, ds.config.queryThreads)
      SelfClosingIterator(resultsToFeatures(results), results.close())
    }
  }


  case class CoprocessorPlan(filter: FilterStrategy,
                             tables: Seq[TableName],
                             ranges: Seq[Scan],
                             scan: Scan,
                             config: CoprocessorConfig) extends HBaseQueryPlan  {

    import org.locationtech.geomesa.hbase.coprocessor._

    override def scans: Seq[Scan] = Seq(scan)

    /**
      * Runs the query plain against the underlying database, returning the raw entries
      *
      * @param ds data store - provides connection object and metadata
      * @return
      */
    override def scan(ds: HBaseDataStore): CloseableIterator[SimpleFeature] = {
      // note: we have to copy the ranges for each scan, except the last one (since it won't conflict at that point)
      val iter = tables.iterator
      val scans = iter.map(singleTableScan(ds, _, copyScans = iter.hasNext))

      if (PartitionParallelScan.toBoolean.contains(true)) {
        // kick off all the scans at once
        config.reduce(scans.foldLeft(CloseableIterator.empty[SimpleFeature])(_ ++ _))
      } else {
        // kick off the scans sequentially as they finish
        config.reduce(SelfClosingIterator(scans).flatMap(s => s))
      }
    }

    override protected def explain(explainer: Explainer): Unit =
      explainer("Coprocessor options: " + config.options.map(m => s"[${m._1}:${m._2}]").mkString(", "))

    private def singleTableScan(ds: HBaseDataStore,
                                table: TableName,
                                copyScans: Boolean): CloseableIterator[SimpleFeature] = {
      val s = if (copyScans) { new Scan(scan) } else { scan }
      GeoMesaCoprocessor.execute(ds.connection.getTable(table), s, config.options).collect {
        case r if r.size() > 0 => config.bytesToFeatures(r.toByteArray)
      }
    }
  }
}
