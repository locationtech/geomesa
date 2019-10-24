/***********************************************************************
 * Copyright (c) 2017-2019 IBM
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core.{Row, Statement}
import org.locationtech.geomesa.cassandra.utils.CassandraBatchScan
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

sealed trait CassandraQueryPlan extends QueryPlan[CassandraDataStore] {

  def tables: Seq[String]
  def ranges: Seq[Statement]
  def numThreads: Int
  def clientSideFilter: Option[Filter]

  override def explain(explainer: Explainer, prefix: String): Unit =
    CassandraQueryPlan.explain(this, explainer, prefix)
}

object CassandraQueryPlan {
  def explain(plan: CassandraQueryPlan, explainer: Explainer, prefix: String): Unit = {
    import org.locationtech.geomesa.filter.filterToString
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getName}")
    explainer(s"Tables: ${plan.tables.mkString(", ")}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(_.toString).mkString(", ")}")
    explainer(s"Client-side filter: ${plan.clientSideFilter.map(filterToString).getOrElse("None")}")
    explainer.popLevel()
  }
}

// plan that will not actually scan anything
case class EmptyPlan(
    filter: FilterStrategy,
    rowsToFeatures: CloseableIterator[Row] => CloseableIterator[SimpleFeature]
  ) extends CassandraQueryPlan {
  override val tables: Seq[String] = Seq.empty
  override val ranges: Seq[Statement] = Seq.empty
  override val numThreads: Int = 0
  override val clientSideFilter: Option[Filter] = None
  override def scan(ds: CassandraDataStore): CloseableIterator[SimpleFeature] =
    rowsToFeatures(CloseableIterator.empty)
}

case class StatementPlan(filter: FilterStrategy,
                         tables: Seq[String],
                         ranges: Seq[Statement],
                         numThreads: Int,
                          // note: filter is applied in entriesToFeatures, this is just for explain logging
                         clientSideFilter: Option[Filter],
                         rowsToFeatures: CloseableIterator[Row] => CloseableIterator[SimpleFeature])
    extends CassandraQueryPlan {

  override def scan(ds: CassandraDataStore): CloseableIterator[SimpleFeature] =
    rowsToFeatures(CassandraBatchScan(ds.session, ranges, numThreads))
}
