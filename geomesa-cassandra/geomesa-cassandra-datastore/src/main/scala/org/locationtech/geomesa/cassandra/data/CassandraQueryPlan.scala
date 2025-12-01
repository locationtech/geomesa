/***********************************************************************
 * Copyright (c) 2017-2025 IBM
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import com.datastax.driver.core.Statement
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.cassandra.data.CassandraIndexAdapter.CassandraResultsToFeatures
import org.locationtech.geomesa.cassandra.utils.CassandraBatchScan
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, QueryStrategyPlan, ResultsToFeatures}
import org.locationtech.geomesa.index.api.QueryStrategy
import org.locationtech.geomesa.index.planning.LocalQueryRunner.LocalProcessor
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.index.utils.ThreadManagement.Timeout
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

sealed trait CassandraQueryPlan extends QueryStrategyPlan {

  override type Results = SimpleFeature

  def tables: Seq[String]
  def ranges: Seq[Statement]
  def numThreads: Int

  override def explain(explainer: Explainer): Unit = CassandraQueryPlan.explain(this, explainer)

  protected def moreExplaining(explainer: Explainer): Unit = {}
}

object CassandraQueryPlan {
  def explain(plan: CassandraQueryPlan, explainer: Explainer): Unit = {
    explainer.pushLevel(s"Plan: ${plan.getClass.getName}")
    explainer(s"Tables: ${plan.tables.mkString(", ")}")
    explainer(s"Ranges (${plan.ranges.size}): ${plan.ranges.take(5).map(_.toString).mkString(", ")}")
    plan.moreExplaining(explainer)
    explainer(s"Reduce: ${plan.reducer.getOrElse("none")}")
    explainer.popLevel()
  }
}

// plan that will not actually scan anything
case class EmptyPlan(strategy: QueryStrategy, reducer: Option[FeatureReducer] = None) extends CassandraQueryPlan {
  override val tables: Seq[String] = Seq.empty
  override val ranges: Seq[Statement] = Seq.empty
  override val numThreads: Int = 0
  override val resultsToFeatures: ResultsToFeatures[SimpleFeature] = ResultsToFeatures.empty
  override val sort: Option[Seq[(String, Boolean)]] = None
  override val maxFeatures: Option[Int] = None
  override val projection: Option[QueryReferenceSystems] = None
  override def scan(): CloseableIterator[SimpleFeature] = CloseableIterator.empty
}

case class StatementPlan(
    ds: CassandraDataStore,
    strategy: QueryStrategy,
    tables: Seq[String],
    ranges: Seq[Statement],
    numThreads: Int,
    processor: LocalProcessor,
    resultsToFeatures: ResultsToFeatures[SimpleFeature],
    projection: Option[QueryReferenceSystems]
  ) extends CassandraQueryPlan {

  override def reducer: Option[FeatureReducer] = processor.reducer
  // handled in the local processor
  override def sort: Option[Seq[(String, Boolean)]] = None
  override def maxFeatures: Option[Int] = None

  override def scan(): CloseableIterator[SimpleFeature] = {
    strategy.runGuards(ds) // query guard hook - also handles full table scan checks
    val timeout = ds.config.queries.timeout.map(Timeout.apply)
    val toFeatures = new CassandraResultsToFeatures(strategy.index, strategy.index.sft)
    processor(CassandraBatchScan(this, ds.session, ranges, numThreads, timeout).map(toFeatures.apply))
  }

  override protected def moreExplaining(explainer: Explainer): Unit = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    // filter, transforms, sort, max features are all captured in the local processor so pull them out of the hints instead of the plan
    explainer(s"Client-side filter: ${processor.filter.fold("none")(FilterHelper.toString)}")
    explainer(s"Transform: ${strategy.hints.getTransform.fold("none")(t => s"${t._1} ${SimpleFeatureTypes.encodeType(t._2)}")}")
    explainer(s"Sort: ${strategy.hints.getSortFields.fold("none")(_.mkString(", "))}")
    explainer(s"Max Features: ${strategy.hints.getMaxFeatures.getOrElse("none")}")
  }
}
