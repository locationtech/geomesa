/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation
import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation.CostEvaluation
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
    * @param options filter plans that can satisfy the query
    * @param explain explain logging
    * @return filter plan to execute
    */
  def selectFilterPlan(sft: SimpleFeatureType, options: Seq[FilterPlan], explain: Explainer): FilterPlan
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
  def getFilterPlan[DS <: GeoMesaDataStore[DS]](ds: DS,
                                                sft: SimpleFeatureType,
                                                filter: Filter,
                                                transform: Option[SimpleFeatureType],
                                                evaluation: CostEvaluation,
                                                requested: Option[String],
                                                explain: Explainer = ExplainNull): Seq[FilterStrategy] = {

    def complete(op: String, time: Long, count: Int): Unit = explain(s"$op took ${time}ms for $count options")

    // choose the best option based on cost
    val stats = evaluation match {
      case CostEvaluation.Stats => Some(ds.stats)
      case CostEvaluation.Index => None
    }

    val indices = ds.manager.indices(sft, mode = IndexMode.Read)

    // get the various options that we could potentially use
    val options = profile((o: Seq[FilterPlan], t: Long) => complete("Query processing", t, o.length)) {
      new FilterSplitter(sft, indices, stats).getQueryOptions(filter, transform)
    }

    val selected = profile(t => complete("Strategy selection", t, options.length)) {
      if (requested.isDefined) {
        val forced = matchRequested(requested.get, indices, options, filter)
        explain(s"Filter plan forced to $forced")
        forced
      } else if (options.isEmpty) {
        // corresponds to filter.exclude
        explain("No filter plans found")
        FilterPlan(Seq.empty)
      } else if (options.lengthCompare(1) == 0) {
        // only a single option, so don't bother with cost
        explain(s"Filter plan: ${options.head}")
        options.head
      } else {
        val plan = decider.selectFilterPlan(sft, options, explain)
        explain(s"Filter plan selected: $plan")
        explain(s"Filter plans not selected: ${options.filterNot(_.eq(plan)).mkString(", ")}")
        plan
      }
    }

    selected.strategies
  }

  private def matchRequested(id: String,
                             indices: Seq[GeoMesaFeatureIndex[_, _]],
                             options: Seq[FilterPlan],
                             filter: Filter): FilterPlan = {
    // see if one of the normal plans matches the requested type - if not, force it
    def byId: Option[FilterPlan] = options.find(_.strategies.forall(_.index.identifier.equalsIgnoreCase(id)))
    def byName: Option[FilterPlan] = options.find(_.strategies.forall(_.index.name.equalsIgnoreCase(id)))
    // back-compatibility for attr vs join index name
    def byJoin: Option[FilterPlan] = if (!AttributeIndex.name.equalsIgnoreCase(id)) { None } else {
      options.find(_.strategies.forall(_.index.name == AttributeIndex.JoinIndexName))
    }

    def fallback: FilterPlan = {
      val index = indices.find(_.identifier.equalsIgnoreCase(id))
          .orElse(indices.find(_.name.equalsIgnoreCase(id)))
          .getOrElse {
            throw new IllegalArgumentException(s"Invalid index strategy: $id. Valid values are " +
                indices.map(i => s"${i.name}, ${i.identifier}").mkString(", "))
          }
      val secondary = if (filter == Filter.INCLUDE) { None } else { Some(filter) }
      FilterPlan(Seq(FilterStrategy(index, None, secondary, temporal = false, 0L)))
    }

    byId.orElse(byName).orElse(byJoin).getOrElse(fallback)
  }

  class CostBasedStrategyDecider extends StrategyDecider with MethodProfiling {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    override def selectFilterPlan(
        sft: SimpleFeatureType,
        options: Seq[FilterPlan],
        explain: Explainer): FilterPlan = {

      val costs = options.map(cost).sorted
      explain(s"Costs: ${costs.mkString("; ")}")

      val temporal = if (!sft.isTemporalPriority) { None } else {
        costs.find(c => c.plan.strategies.nonEmpty && c.plan.strategies.forall(_.temporal))
      }
      temporal.getOrElse(costs.head).plan
    }

    private def cost(option: FilterPlan): FilterPlanCost = {
      profile((fpc: FilterPlanCost, time: Long) => fpc.copy(time = time)) {
        FilterPlanCost(option, option.strategies.map(_.cost).sum, 0L)
      }
    }

    private case class FilterPlanCost(plan: FilterPlan, cost: Long, time: Long) extends Comparable[FilterPlanCost] {
      override def compareTo(o: FilterPlanCost): Int = java.lang.Long.compare(cost, o.cost)
      override def toString: String = s"$plan (Cost $cost in ${time}ms)"
    }
  }
}
