/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.hbase.data

import org.apache.hadoop.hbase.client._
import org.locationtech.geomesa.hbase.{HBaseFilterStrategyType, HBaseQueryPlanType}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

sealed trait HBaseQueryPlan extends HBaseQueryPlanType {
  def filter: HBaseFilterStrategyType
  def table: Table
  def ranges: Seq[Query]
  def clientSideFilter: Option[Filter] // TODO use scan filters?

  override def reduce: Option[(CloseableIterator[SimpleFeature]) => CloseableIterator[SimpleFeature]] =
    clientSideFilter.map(f => (i) => i.filter(f.evaluate))

  override def explain(explainer: Explainer, prefix: String): Unit =
    HBaseQueryPlan.explain(this, explainer, prefix)
}

object HBaseQueryPlan {

  def explain(plan: HBaseQueryPlan, explainer: Explainer, prefix: String): Unit = {
    import org.locationtech.geomesa.filter.filterToString
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getName}")
    explainer(s"Table: ${Option(plan.table).map(_.getName).orNull}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(rangeToString).mkString(", ")}")
    explainer(s"Client-side filter: ${plan.clientSideFilter.map(filterToString).getOrElse("None")}")
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
  override val table: Table = null
  override val ranges: Seq[Query] = Seq.empty
  override val entriesToFeatures: (Result) => SimpleFeature = (_) => null
  override val clientSideFilter: Option[Filter] = None
  override def scan(ops: HBaseDataStore): CloseableIterator[Result] = CloseableIterator.empty
}

case class ScanPlan(filter: HBaseFilterStrategyType,
                    table: Table,
                    ranges: Seq[Scan],
                    entriesToFeatures: (Result) => SimpleFeature,
                    clientSideFilter: Option[Filter]) extends HBaseQueryPlan {

  override def scan(ops: HBaseDataStore): CloseableIterator[Result] = {
    val scanners = ranges.map(table.getScanner)
    CloseableIterator(scanners.flatMap(result).iterator, scanners.foreach(_.close))
  }

  private def result(scanner: ResultScanner): SelfClosingIterator[Result] = {
    import scala.collection.JavaConversions._
    SelfClosingIterator(scanner.toList.iterator, scanner.close())
  }
}

case class GetPlan(filter: HBaseFilterStrategyType,
                   table: Table,
                   ranges: Seq[Get],
                   entriesToFeatures: (Result) => SimpleFeature,
                   clientSideFilter: Option[Filter]) extends HBaseQueryPlan {

  override def scan(ops: HBaseDataStore): CloseableIterator[Result] = {
    import scala.collection.JavaConversions._
    CloseableIterator(table.get(ranges).iterator)
  }
}
