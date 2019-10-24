/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.data

import org.apache.kudu.client.{KuduPredicate, PartialRow}
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.kudu.result.KuduResultAdapter
import org.locationtech.geomesa.kudu.utils.KuduBatchScan
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

sealed trait KuduQueryPlan extends QueryPlan[KuduDataStore] {
  def tables: Seq[String]
  def ranges: Seq[(Option[PartialRow], Option[PartialRow])]
  def predicates: Seq[KuduPredicate]
  def ecql: Option[Filter]
  def adapter: KuduResultAdapter
  def numThreads: Int

  override def explain(explainer: Explainer, prefix: String): Unit =
    KuduQueryPlan.explain(this, explainer, prefix)
}

object KuduQueryPlan {

  def explain(plan: KuduQueryPlan, explainer: Explainer, prefix: String): Unit = {
    import org.locationtech.geomesa.filter.filterToString
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getName}")
    explainer(s"Table: ${plan.tables.mkString(", ")}")
    explainer(s"Columns: ${plan.adapter.columns.mkString(", ")}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(_.toString).mkString(", ")}")
    explainer(s"Additional predicates: ${if (plan.predicates.isEmpty) { "None" } else { plan.predicates.map(_.toString).mkString(", ") }}")
    explainer(s"Client-side filter: ${plan.ecql.map(filterToString).getOrElse("None")}")
    explainer(s"Rows to features: ${plan.adapter}")
    explainer.popLevel()
  }

  // plan that will not actually scan anything
  case class EmptyPlan(filter: FilterStrategy, adapter: KuduResultAdapter) extends KuduQueryPlan {
    override def tables: Seq[String] = Seq.empty
    override def ranges: Seq[(Option[PartialRow], Option[PartialRow])] = Seq.empty
    override def predicates: Seq[KuduPredicate] = Seq.empty
    override def ecql: Option[Filter] = None
    override def numThreads: Int = 0
    override def scan(ds: KuduDataStore): CloseableIterator[SimpleFeature] = adapter.adapt(CloseableIterator.empty)
  }

  case class ScanPlan(filter: FilterStrategy,
                      tables: Seq[String],
                      ranges: Seq[(Option[PartialRow], Option[PartialRow])],
                      predicates: Seq[KuduPredicate],
                      // note: filter is applied in entriesToFeatures, this is just for explain logging
                      ecql: Option[Filter],
                      adapter: KuduResultAdapter,
                      numThreads: Int) extends KuduQueryPlan {

    import scala.collection.JavaConverters._

    override def scan(ds: KuduDataStore): CloseableIterator[SimpleFeature] = {
      import org.locationtech.geomesa.kudu.utils.RichKuduClient.RichScanner

      if (numThreads > 1 || tables.lengthCompare(1) > 0) {
        CloseableIterator(tables.iterator).flatMap { table =>
          val scan = KuduBatchScan(ds.client, table, adapter.columns, ranges, predicates, numThreads)
          adapter.adapt(scan.flatMap(_.iterator.asScala))
        }
      } else {
        // avoid the overhead of spinning up the threads, etc
        val kuduTable = ds.client.openTable(tables.head)
        val cols = adapter.columns.map(kuduTable.getSchema.getColumnIndex).asJava.asInstanceOf[java.util.List[Integer]]
        val iter = CloseableIterator(ranges.iterator).flatMap { case (lo, hi) =>
          val builder = ds.client.newScannerBuilder(kuduTable).setProjectedColumnIndexes(cols)
          lo.foreach(builder.lowerBound)
          hi.foreach(builder.exclusiveUpperBound)
          predicates.foreach(builder.addPredicate)
          builder.build().iterator
        }
        adapter.adapt(iter)
      }
    }
  }
}
