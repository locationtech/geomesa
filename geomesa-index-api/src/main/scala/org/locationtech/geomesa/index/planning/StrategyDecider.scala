/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.index.api.{WrappedFeature, _}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation
import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation.CostEvaluation
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.stats.MethodProfiling
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

/**
  * Selects from available strategies for answering a query
  */
trait StrategyDecider {

  /**
    * Select from available filter plans
    *
    * @param sft simple feature type being queried
    * @param stats handle to stats service, if available
    * @param options filter plans that can satisfy the query
    * @param transform requested query transform, if any
    * @param explain explain logging
    * @return filter plan to execute
    */
  def selectFilterPlan[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W]
      (sft: SimpleFeatureType,
       stats: Option[GeoMesaStats],
       options: Seq[FilterPlan[DS, F, W]],
       transform: Option[SimpleFeatureType],
       explain: Explainer): FilterPlan[DS, F, W]
}

class CostBasedStrategyDecider extends StrategyDecider with MethodProfiling {
  override def selectFilterPlan[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W]
      (sft: SimpleFeatureType,
       stats: Option[GeoMesaStats],
       options: Seq[FilterPlan[DS, F, W]],
       transform: Option[SimpleFeatureType],
       explain: Explainer): FilterPlan[DS, F, W] = {

    def cost(option: FilterPlan[DS, F, W]): FilterPlanCost[DS, F, W] = {
      profile((fpc: FilterPlanCost[DS, F, W], time: Long) => fpc.copy(time = time)) {
        var cost = BigInt(0)
        option.strategies.foreach { strategy =>
          val filter = strategy.primary.getOrElse(Filter.INCLUDE)
          val count = stats.flatMap(_.getCount(sft, filter, exact = false)).getOrElse(100L)
          cost = cost + BigInt((count * strategy.costMultiplier).toLong)
        }
        FilterPlanCost(option, if (cost.isValidLong) { cost.longValue() } else { Long.MaxValue }, 0L)
      }
    }

    val costs = options.map(cost).sorted

//    val temporal = if (!sft.isTemporalPriority) { None } else {
//      costs.find(c => c.plan.strategies.nonEmpty && c.plan.strategies.forall(_.temporal))
//    }
    val selected = costs.head // temporal.getOrElse(costs.head)
    explain(s"Filter plan selected: $selected")
    explain(s"Filter plans not selected: ${costs.filterNot(_.eq(selected)).mkString(", ")}")
    selected.plan
  }

  private case class FilterPlanCost[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W](plan: FilterPlan[DS, F, W], cost: Long, time: Long) extends Comparable[FilterPlanCost[DS, F, W]] {
    override def compareTo(o: FilterPlanCost[DS, F, W]): Int = java.lang.Long.compare(cost, o.cost)
    override def toString: String = s"$plan (Cost $cost in ${time}ms)"
  }
}

object StrategyDecider extends MethodProfiling with LazyLogging {

  private val decider: StrategyDecider = SystemProperty("geomesa.strategy.decider").option match {
    case None       => new CostBasedStrategyDecider()
    case Some(clas) => Class.forName(clas).newInstance().asInstanceOf[StrategyDecider]
  }

  logger.debug(s"Using strategy provider '${decider.getClass.getName}'")

  /**
    * Selects a strategy for executing a given query.
    *
    * If a particular strategy has been requested, that strategy will be used (note - this is only
    * partially supported, and should be used with care.)
    *
    * Otherwise, the query will be examined for strategies that could be used to execute it. The cost of
    * executing each available strategy will be calculated, and the least expensive strategy will be used.
    *
    * @param ds data store
    * @param sft simple feature type
    * @param filter filter to execute
    * @param transform return transformation
    * @param requested requested index
    * @param explain for trace logging
    * @return
    */
  def getFilterPlan[DS <: GeoMesaDataStore[DS, F, W], F <: WrappedFeature, W]
      (ds: DS,
       sft: SimpleFeatureType,
       filter: Filter,
       transform: Option[SimpleFeatureType],
       evaluation: CostEvaluation,
       requested: Option[GeoMesaFeatureIndex[DS, F, W]],
       explain: Explainer = ExplainNull): Seq[FilterStrategy[DS, F, W]] = {

    val availableIndices = ds.manager.indices(sft, mode = IndexMode.Read)

    // get the various options that we could potentially use
    var time = 0L
    val options = profile(t => time = t) {
      new FilterSplitter(sft, availableIndices).getQueryOptions(filter, transform)
    }
    explain(s"Query processing took ${time}ms and produced ${options.length} options")

    val selected = profile(time => explain(s"Strategy selection took ${time}ms for ${options.length} options")) {
      if (requested.isDefined) {
        val forced = {
          val index = requested.get
          def checkStrategy(f: FilterStrategy[DS, F, W]) = f.index == index
          // see if one of the normal plans matches the requested type - if not, force it
          options.find(_.strategies.forall(checkStrategy)).getOrElse {
            val secondary = if (filter == Filter.INCLUDE) { None } else { Some(filter) }
            FilterPlan(Seq(FilterStrategy(index, None, secondary, temporal = false, Float.PositiveInfinity)))
          }
        }
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
        val stats = evaluation match {
          case CostEvaluation.Stats => Some(ds.stats)
          case CostEvaluation.Index => None
        }
        val plan = decider.selectFilterPlan(sft, stats, options, transform, explain)
        explain(s"Filter plan selected: $plan")
        explain(s"Filter plans not selected: ${options.filterNot(_.eq(plan)).mkString(", ")}")
        plan
      }
    }

    selected.strategies
  }
}