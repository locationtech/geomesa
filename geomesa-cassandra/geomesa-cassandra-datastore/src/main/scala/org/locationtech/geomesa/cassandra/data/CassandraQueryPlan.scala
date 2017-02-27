/***********************************************************************
* Copyright (c) 2016  IBM
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.data

import scala.collection.JavaConversions.iterableAsScalaIterable

import org.locationtech.geomesa.cassandra.CassandraFilterStrategyType
import org.locationtech.geomesa.cassandra.CassandraQueryPlanType
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import com.datastax.driver.core.Row
import com.datastax.driver.core.Statement

sealed trait CassandraQueryPlan extends CassandraQueryPlanType {
  def filter: CassandraFilterStrategyType
  def ranges: Seq[Statement]
  def table: String
  def clientSideFilter: Option[Filter]

  override def explain(explainer: Explainer, prefix: String): Unit =
    CassandraQueryPlan.explain(this, explainer, prefix)
}

object CassandraQueryPlan {

  def explain(plan: CassandraQueryPlan, explainer: Explainer, prefix: String): Unit = {
    import org.locationtech.geomesa.filter.filterToString
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getName}")
    explainer(s"Table: ${Option(plan.table).orNull}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(_.toString).mkString(", ")}")
    explainer(s"Client-side filter: ${plan.clientSideFilter.map(filterToString).getOrElse("None")}")
    explainer.popLevel()
  }
}

// plan that will not actually scan anything
case class EmptyPlan(filter: CassandraFilterStrategyType) extends CassandraQueryPlan {
  override val table: String = ""
  override val ranges: Seq[Statement] = Seq.empty
  override val clientSideFilter: Option[Filter] = None
  override def scan(ds: CassandraDataStore): CloseableIterator[SimpleFeature] = CloseableIterator.empty
}

case class QueryPlan(filter: CassandraFilterStrategyType,
                    table: String,
                    ranges: Seq[Statement],
                    clientSideFilter: Option[Filter],
                    entriesToFeatures: Iterator[Row] => Iterator[SimpleFeature]) extends CassandraQueryPlan {

  override def hasDuplicates: Boolean = true

  override def scan(ds: CassandraDataStore): CloseableIterator[SimpleFeature] = {
    // TODO multi-thread the range queries, leave single threaded for now for debug
    var results = Array[Row]()

    ranges.foreach(stmt => {
      results = results ++ ds.session.execute(stmt)
    })

    SelfClosingIterator(entriesToFeatures(results.iterator))
  }
}
