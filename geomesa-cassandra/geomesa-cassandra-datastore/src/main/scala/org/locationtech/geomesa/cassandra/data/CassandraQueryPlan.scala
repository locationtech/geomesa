/***********************************************************************
 * Copyright (c) 2017-2025 IBM
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core.{Row, Statement}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.cassandra.utils.CassandraBatchScan
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.api.{QueryPlan, QueryStrategy}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.index.utils.ThreadManagement.Timeout
import org.locationtech.geomesa.utils.collection.CloseableIterator

sealed trait CassandraQueryPlan extends QueryPlan {

  override type Results = Row

  def strategy: QueryStrategy
  def tables: Seq[String]
  def ranges: Seq[Statement]
  def numThreads: Int

  override def explain(explainer: Explainer, prefix: String): Unit =
    CassandraQueryPlan.explain(this, explainer, prefix)
}

object CassandraQueryPlan {
  def explain(plan: CassandraQueryPlan, explainer: Explainer, prefix: String): Unit = {
    explainer.pushLevel(s"${prefix}Plan: ${plan.getClass.getName}")
    explainer(s"Tables: ${plan.tables.mkString(", ")}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(_.toString).mkString(", ")}")
    explainer(s"Client-side filter: ${plan.localFilter.fold("none")(FilterHelper.toString)}")
    explainer(s"Reduce: ${plan.reducer.getOrElse("none")}")
    explainer.popLevel()
  }
}

// plan that will not actually scan anything
case class EmptyPlan(strategy: QueryStrategy, reducer: Option[FeatureReducer] = None) extends CassandraQueryPlan {
  override val tables: Seq[String] = Seq.empty
  override val ranges: Seq[Statement] = Seq.empty
  override val numThreads: Int = 0
  override val localFilter: Option[Filter] = None
  override val localTransform: Option[(String, SimpleFeatureType)] = None
  override val resultsToFeatures: ResultsToFeatures[Row] = ResultsToFeatures.empty
  override val sort: Option[Seq[(String, Boolean)]] = None
  override val maxFeatures: Option[Int] = None
  override val projection: Option[QueryReferenceSystems] = None
  override def scan(): CloseableIterator[Row] = CloseableIterator.empty
}

case class StatementPlan(
    ds: CassandraDataStore,
    strategy: QueryStrategy,
    tables: Seq[String],
    ranges: Seq[Statement],
    numThreads: Int,
    resultsToFeatures: ResultsToFeatures[Row],
    localFilter: Option[Filter],
    localTransform: Option[(String, SimpleFeatureType)],
    reducer: Option[FeatureReducer],
    sort: Option[Seq[(String, Boolean)]],
    maxFeatures: Option[Int],
    projection: Option[QueryReferenceSystems]
  ) extends CassandraQueryPlan {

  override def scan(): CloseableIterator[Row] = {
    strategy.runGuards(ds) // query guard hook - also handles full table scan checks
    CassandraBatchScan(this, ds.session, ranges, numThreads, ds.config.queries.timeout.map(Timeout.apply))
  }
}
