/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument.Tags
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.index.api.QueryPlan
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.index.utils._

/**
 * Plans and executes queries against geomesa
 */
class DataStoreQueryRunner[DS <: GeoMesaDataStore[DS]](ds: DS) extends QueryRunner with LazyLogging {

  override protected val interceptors: QueryInterceptorFactory = ds.interceptors

  override protected def tags(typeName: String): Tags = ds.tags(typeName)

  override protected def getQueryPlans(sft: SimpleFeatureType, query: Query, explain: Explainer): Seq[QueryPlan] = {
    explain.pushLevel("Strategy selection:")
    val strategies = StrategyDecider.getFilterPlan(ds, sft, query.getFilter, query.getHints, explain)
    explain.popLevel()

    var strategyCount = 1
    strategies.map { strategy =>
      explain.pushLevel(s"Strategy $strategyCount of ${strategies.length}: ${strategy.index}")
      strategyCount += 1
      explain(s"Strategy filter: $strategy")
      val start = System.nanoTime()
      val qs = strategy.getQueryStrategy(explain)
      val plan = ds.adapter.createQueryPlan(qs)
      plan.explain(explain)
      explain(s"Plan creation took ${(System.nanoTime() - start) / 1000000L}ms").popLevel()
      plan
    }
  }

}
