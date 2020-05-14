/***********************************************************************
 * Copyright (c) 2017-2020 IBM
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core.{Row, Statement}
import org.locationtech.geomesa.cassandra.utils.CassandraBatchScan
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.index.utils.ThreadManagement.Timeout
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.opengis.filter.Filter

sealed trait CassandraQueryPlan extends QueryPlan[CassandraDataStore] {

  override type Results = Row

  def tables: Seq[String]
  def ranges: Seq[Statement]
  def numThreads: Int

  /**
    * Note: filter is applied in entriesToFeatures, this is just for explain logging
    * @return
    */
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
case class EmptyPlan(filter: FilterStrategy, reducer: Option[FeatureReducer] = None) extends CassandraQueryPlan {
  override val tables: Seq[String] = Seq.empty
  override val ranges: Seq[Statement] = Seq.empty
  override val numThreads: Int = 0
  override val clientSideFilter: Option[Filter] = None
  override val resultsToFeatures: ResultsToFeatures[Row] = ResultsToFeatures.empty
  override val sort: Option[Seq[(String, Boolean)]] = None
  override val maxFeatures: Option[Int] = None
  override val projection: Option[QueryReferenceSystems] = None
  override def scan(ds: CassandraDataStore): CloseableIterator[Row] = CloseableIterator.empty
}

case class StatementPlan(
    filter: FilterStrategy,
    tables: Seq[String],
    ranges: Seq[Statement],
    numThreads: Int,
    // note: filter is applied in entriesToFeatures, this is just for explain logging
    clientSideFilter: Option[Filter],
    resultsToFeatures: ResultsToFeatures[Row],
    reducer: Option[FeatureReducer],
    sort: Option[Seq[(String, Boolean)]],
    maxFeatures: Option[Int],
    projection: Option[QueryReferenceSystems]
  ) extends CassandraQueryPlan {

  override def scan(ds: CassandraDataStore): CloseableIterator[Row] =
    CassandraBatchScan(this, ds.session, ranges, numThreads, ds.config.queries.timeout.map(Timeout.apply))
}
