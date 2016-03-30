/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.data.Query
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType.StrategyType
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.stats.{MethodProfiling, TimingsImpl}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait QueryStrategyDecider {
  def chooseStrategies(sft: SimpleFeatureType,
                       query: Query,
                       hints: StrategyHints,
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
                                hints: StrategyHints,
                                requested: Option[StrategyType],
                                output: ExplainerOutputType): Seq[Strategy] = {

    implicit val timings = new TimingsImpl()

    // get the various options that we could potentially use
    val options = new QueryFilterSplitter(sft).getQueryOptions(query.getFilter)

    val selected = profile({
      if (requested.isDefined) {
        val forced = forceStrategy(options, requested.get, query.getFilter)
        output(s"Filter plan forced to $forced")
        forced.filters.map(createStrategy(_, sft))
      } else if (options.isEmpty) {
        output("No filter plans found")
        Seq.empty // corresponds to filter.exclude
      } else {
        val filterPlan = if (query.getHints.isDensityQuery) {
          // TODO GEOMESA-322 use other strategies with density iterator
          val st = options.find(_.filters.forall(q => q.strategy == StrategyType.Z3 ||
              q.strategy == StrategyType.ST))
          val density = st.getOrElse {
            val fallback = if (query.getFilter == Filter.INCLUDE) None else Some(query.getFilter)
            FilterPlan(Seq(QueryFilter(StrategyType.ST, Seq(Filter.INCLUDE), fallback)))
          }
          output(s"Filter plan for density query: $density")
          density
        } else if (options.length == 1) {
          // only a single option, so don't bother with cost
          output(s"Filter plan: ${options.head}")
          options.head
        } else {
          // choose the best option based on cost
          val costs = options.map(o => (o, o.filters.map(getCost(_, sft, hints)).sum)).sortBy(_._2)
          val cheapest = costs.head
          output(s"Filter plan selected: ${cheapest._1} (Cost ${cheapest._2})")
          output(s"Filter plans not used (${costs.size - 1}):", costs.drop(1).map(c => s"${c._1} (Cost ${c._2})"))
          cheapest._1
        }
        filterPlan.filters.map(createStrategy(_, sft))
      }
    }, "cost")
    output(s"Strategy selection took ${timings.time("cost")}ms for ${options.length} options")
    selected
  }

  // see if one of the normal plans matches the requested type - if not, force it
  private def forceStrategy(options: Seq[FilterPlan], strategy: StrategyType, allFilter: Filter): FilterPlan = {
    def checkStrategy(f: QueryFilter) = f.strategy == strategy ||
        (strategy == StrategyType.ST && f.strategy == StrategyType.Z3)

    options.find(_.filters.forall(checkStrategy)) match {
      case None => FilterPlan(Seq(QueryFilter(strategy, Seq(Filter.INCLUDE), Some(allFilter))))
      case Some(fp) =>
        val filters = fp.filters.map {
          // swap z3 to st if required
          case filter if filter.strategy != strategy => filter.copy(strategy = strategy)
          case filter => filter
        }
        fp.copy(filters = filters)
    }
  }

  /**
   * Gets the estimated cost of running a particular strategy
   */
  // noinspection ScalaDeprecation
  private def getCost(filter: QueryFilter, sft: SimpleFeatureType, hints: StrategyHints): Int = {
    filter.strategy match {
      case StrategyType.Z3        => Z3IdxStrategy.getCost(filter, sft, hints)
      case StrategyType.ST        => STIdxStrategy.getCost(filter, sft, hints)
      case StrategyType.RECORD    => RecordIdxStrategy.getCost(filter, sft, hints)
      case StrategyType.ATTRIBUTE if sft.getSchemaVersion < ImplVersionChange =>
        AttributeIdxStrategyV5.getCost(filter, sft, hints)
      case StrategyType.ATTRIBUTE => AttributeIdxStrategy.getCost(filter, sft, hints)
      case _ => throw new IllegalStateException(s"Unknown query plan requested: ${filter.strategy}")
    }
  }

  /**
   * Mapping from strategy type enum to concrete implementation class
   */
  // noinspection ScalaDeprecation
  private def createStrategy(filter: QueryFilter, sft: SimpleFeatureType): Strategy = {
    filter.strategy match {
      case StrategyType.Z3        => new Z3IdxStrategy(filter)
      case StrategyType.ST        => new STIdxStrategy(filter)
      case StrategyType.RECORD    => new RecordIdxStrategy(filter)
      case StrategyType.ATTRIBUTE if sft.getSchemaVersion < ImplVersionChange =>
        new AttributeIdxStrategyV5(filter)
      case StrategyType.ATTRIBUTE => new AttributeIdxStrategy(filter)
      case _ => throw new IllegalStateException(s"Unknown query plan requested: ${filter.strategy}")
    }
  }
}
