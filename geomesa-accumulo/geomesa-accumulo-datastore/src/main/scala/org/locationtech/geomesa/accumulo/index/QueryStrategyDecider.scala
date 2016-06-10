/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.data.Query
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaStats
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.Strategy.CostEvaluation.CostEvaluation
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType.StrategyType
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timing}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait QueryStrategyDecider {
  def chooseStrategies(sft: SimpleFeatureType,
                       query: Query,
                       stats: GeoMesaStats,
                       requested: Option[StrategyType],
                       output: ExplainerOutputType = ExplainNull): Seq[Strategy]
}

object QueryStrategyDecider extends QueryStrategyDecider with MethodProfiling {

  private val ImplVersionChange = 6 // schema version that we changed decider implementations

  /**
   * Selects a strategy for executing a given query.
   *
   * If a particular strategy has been requested, that strategy will be used (note - this is only
   * partially supported, and should be used with care.)
   *
   * Otherwise, the query will be examined for strategies that could be used to execute it. The cost of
   * executing each available strategy will be calculated, and the least expensive strategy will be used.
   *
   * Currently, the costs are hard-coded to conform to the following priority:
   *
   *  * If an ID predicate is present, then use the record index strategy
   *  * If high cardinality attribute filters are present, then use the attribute index strategy
   *  * If a date filter is present, then use the Z3 index strategy
   *  * If a spatial filter is present, then use the ST index strategy
   *  * If other attribute filters are present, then use the attribute index strategy
   *  * If none of the above, use the record index strategy (likely a full table scan)
   *
   */
  override def chooseStrategies(sft: SimpleFeatureType,
                                query: Query,
                                stats: GeoMesaStats,
                                requested: Option[StrategyType],
                                output: ExplainerOutputType): Seq[Strategy] = {

    implicit val timings = new Timing()

    // get the various options that we could potentially use
    val options = {
      val all = new QueryFilterSplitter(sft).getQueryOptions(query.getFilter, output)
      // don't evaluate z2 index if there is a z3 option, it will always be picked
      // TODO if we eventually take into account date range, this will need to be removed
      if (requested.isEmpty && all.exists(_.filters.forall(_.strategy == StrategyType.Z3)) &&
          all.exists(_.filters.forall(_.strategy == StrategyType.Z2))) {
        all.filterNot(_.filters.forall(_.strategy == StrategyType.Z2))
      } else {
        all
      }
    }

    val selected = profile {
      if (requested.isDefined) {
        val forced = forceStrategy(options, requested.get, query.getFilter)
        output(s"Filter plan forced to $forced")
        forced.filters.map(createStrategy(_, sft))
      } else if (options.isEmpty) {
        output("No filter plans found")
        Seq.empty // corresponds to filter.exclude
      } else {
        val filterPlan = if (options.length == 1) {
          // only a single option, so don't bother with cost
          output(s"Filter plan: ${options.head}")
          options.head
        } else {
          // choose the best option based on cost
          val evaluation = query.getHints.getCostEvaluation
          val transform = query.getHints.getTransformSchema
          val costs = options.map { option =>
            val timing = new Timing
            val optionCosts = profile(option.filters.map(getCost(sft, _, transform, stats, evaluation)))(timing)
            (option, optionCosts.sum, timing.time)
          }.sortBy(_._2)
          val (cheapest, cost, time) = costs.head
          output(s"Filter plan selected: $cheapest (Cost $cost)(Cost evaluation: $evaluation in ${time}ms)")
          output(s"Filter plans not used (${costs.size - 1}):",
            costs.drop(1).map(c => s"${c._1} (Cost ${c._2} in ${c._3}ms)"))
          cheapest
        }
        filterPlan.filters.map(createStrategy(_, sft))
      }
    }
    output(s"Strategy selection took ${timings.time}ms for ${options.length} options")
    selected
  }

  // see if one of the normal plans matches the requested type - if not, force it
  private def forceStrategy(options: Seq[FilterPlan], strategy: StrategyType, allFilter: Filter): FilterPlan = {
    def checkStrategy(f: QueryFilter) = f.strategy == strategy
    options.find(_.filters.forall(checkStrategy)).getOrElse {
      FilterPlan(Seq(QueryFilter(strategy, Seq(Filter.INCLUDE), Some(allFilter))))
    }
  }

  /**
   * Gets the estimated cost of running a particular strategy
   */
  // noinspection ScalaDeprecation
  private def getCost(sft: SimpleFeatureType,
                      filter: QueryFilter,
                      transform: Option[SimpleFeatureType],
                      stats: GeoMesaStats,
                      evaluation: CostEvaluation): Long = {
    val strategy = filter.strategy match {
      case StrategyType.Z2        => Z2IdxStrategy
      case StrategyType.Z3        => Z3IdxStrategy
      case StrategyType.RECORD    => RecordIdxStrategy
      case StrategyType.ATTRIBUTE => AttributeIdxStrategy
      case StrategyType.ST        => STIdxStrategy
      case _ => throw new IllegalStateException(s"Unknown query plan requested: ${filter.strategy}")
    }
    if (sft.getSchemaVersion < ImplVersionChange && strategy == AttributeIdxStrategy) {
      AttributeIdxStrategyV5.getCost(sft, filter, transform, stats, evaluation)
    } else {
      strategy.getCost(sft, filter, transform, stats, evaluation)
    }
  }

  /**
   * Mapping from strategy type enum to concrete implementation class
   */
  // noinspection ScalaDeprecation
  private def createStrategy(filter: QueryFilter, sft: SimpleFeatureType): Strategy = {
    filter.strategy match {
      case StrategyType.Z2        => new Z2IdxStrategy(filter)
      case StrategyType.Z3        => new Z3IdxStrategy(filter)
      case StrategyType.RECORD    => new RecordIdxStrategy(filter)
      case StrategyType.ATTRIBUTE if sft.getSchemaVersion >= ImplVersionChange => new AttributeIdxStrategy(filter)
      case StrategyType.ST        => new STIdxStrategy(filter)
      case StrategyType.ATTRIBUTE => new AttributeIdxStrategyV5(filter)
      case _ => throw new IllegalStateException(s"Unknown query plan requested: ${filter.strategy}")
    }
  }
}
