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
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.cassandra.data.CassandraIndexAdapter.CassandraResultsToFeatures
import org.locationtech.geomesa.cassandra.utils.CassandraBatchScan
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, QueryStrategyPlan, ResultsToFeatures}
import org.locationtech.geomesa.index.api.QueryStrategy
import org.locationtech.geomesa.index.planning.LocalQueryRunner.{LocalProcessor, LocalProcessorPlan}
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.index.utils.ThreadManagement.Timeout
import org.locationtech.geomesa.utils.collection.CloseableIterator

sealed trait CassandraQueryPlan extends QueryStrategyPlan {

  override type Results = SimpleFeature

  def tables: Seq[String]
  def ranges: Seq[Statement]
  def numThreads: Int

  override def explain(explainer: Explainer): Unit = {
    explainer.pushLevel(s"Plan: ${getClass.getName}")
    explainer(s"Tables: ${tables.mkString(", ")}")
    explainer(s"Ranges (${ranges.size}): ${ranges.take(5).map(_.toString).mkString(", ")}")
    moreExplaining(explainer)
    explainer(s"Reduce: ${reducer.getOrElse("none")}")
    explainer.popLevel()
  }

  protected def moreExplaining(explainer: Explainer): Unit = {}
}

object CassandraQueryPlan {

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
      localFilter: Option[Filter],
      processor: LocalProcessor,
      resultsToFeatures: ResultsToFeatures[SimpleFeature],
      projection: Option[QueryReferenceSystems]
    ) extends CassandraQueryPlan with LocalProcessorPlan {

    override def scan(): CloseableIterator[SimpleFeature] = {
      strategy.runGuards(ds) // query guard hook - also handles full table scan checks
      val timeout = ds.config.queries.timeout.map(Timeout.apply)
      val toFeatures = new CassandraResultsToFeatures(strategy.index, strategy.index.sft)
      val scanner = CassandraBatchScan(this, ds.session, ranges, numThreads, timeout).map(toFeatures.apply)
      val features = localFilter.fold(scanner)(f => scanner.filter(f.evaluate))
      processor(features)
    }

    override protected def moreExplaining(explainer: Explainer): Unit = {
      explainer(s"Client-side filter: ${localFilter.fold("none")(FilterHelper.toString)}")
      processor.explain(explainer)
    }
  }
}
