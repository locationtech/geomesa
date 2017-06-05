/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.api

import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timing, TimingsImpl}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

class StrategyDecider[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W](manager: GeoMesaIndexManager[DS, F, W])
    extends MethodProfiling {

  type TypedFeatureIndex = GeoMesaFeatureIndex[DS, F, W]
  type TypedFilterStrategy = FilterStrategy[DS, F, W]

  /**
    * Selects a strategy for executing a given query.
    *
    * If a particular strategy has been requested, that strategy will be used (note - this is only
    * partially supported, and should be used with care.)
    *
    * Otherwise, the query will be examined for strategies that could be used to execute it. The cost of
    * executing each available strategy will be calculated, and the least expensive strategy will be used.
    *
    * @param sft simple feature type
    * @param ops handle to operations
    * @param filter filter to execute
    * @param transform return transformation
    * @param requested requested index
    * @param explain for trace logging
    * @return
    */
  def getFilterPlan(sft: SimpleFeatureType,
                    ops: Option[DS],
                    filter: Filter,
                    transform: Option[SimpleFeatureType],
                    requested: Option[TypedFeatureIndex],
                    explain: Explainer = ExplainNull): Seq[TypedFilterStrategy] = {
    implicit val timings = new TimingsImpl()

    val availableIndices = manager.indices(sft, IndexMode.Read)

    // get the various options that we could potentially use
    val options = profile("split")(new FilterSplitter(sft, availableIndices).getQueryOptions(filter, transform))

    explain(s"Query processing took ${timings.time("split")}ms and produced ${options.length} options")

    val selected = profile("cost") {
      if (requested.isDefined) {
        val forced = forceStrategy(options, requested.get, filter)
        explain(s"Filter plan forced to $forced")
        forced
      } else if (options.isEmpty) {
        // corresponds to filter.exclude
        explain("No filter plans found")
        FilterPlan[DS, F, W](Seq.empty)
      } else if (options.length == 1) {
        // only a single option, so don't bother with cost
        explain(s"Filter plan: ${options.head}")
        options.head
      } else {
        // choose the best option based on cost
        val costs = options.map { option =>
          implicit val timing = new Timing()
          val optionCosts = profile(option.strategies.map(f => f.index.getCost(sft, ops, f, transform)))
          (option, optionCosts.sum, timing.time)
        }.sortBy(_._2)
        val (cheapest, cost, time) = costs.head
        explain(s"Filter plan selected: $cheapest (Cost $cost) in ${time}ms")
        explain(s"Filter plans not used (${costs.size - 1}):",
          costs.drop(1).map(c => s"${c._1} (Cost ${c._2} in ${c._3}ms)"))
        cheapest
      }
    }

    explain(s"Strategy selection took ${timings.time("cost")}ms for ${options.length} options")

    selected.strategies
  }

  // see if one of the normal plans matches the requested type - if not, force it
  private def forceStrategy(options: Seq[FilterPlan[DS, F, W]],
                            index: TypedFeatureIndex,
                            allFilter: Filter): FilterPlan[DS, F, W] = {
    def checkStrategy(f: FilterStrategy[DS, F, W]) = f.index == index
    options.find(_.strategies.forall(checkStrategy)).getOrElse {
      val secondary = if (allFilter == Filter.INCLUDE) None else Some(allFilter)
      FilterPlan(Seq(FilterStrategy(index, None, secondary)))
    }
  }
}
