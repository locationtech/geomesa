/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.index.EmptyIndex
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation
import org.locationtech.geomesa.index.planning.QueryPlanner.CostEvaluation.CostEvaluation
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.utils.{ExplainNull, Explainer}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.index.IndexMode
import org.locationtech.geomesa.utils.metrics.MethodProfiling

/**
  * Selects from available strategies for answering a query
  */
trait StrategyDecider {

  /**
   * Select from available filter plans
   *
   * @param sft simple feature type being queried
   * @param options filter plans that can satisfy the query
   * @param stats optional stats that can be used for selecting a plan
   * @param explain explain logging
   * @return filter plan to execute
   */
  @deprecated("Replaced with alternate signature")
  def selectFilterPlan(
    sft: SimpleFeatureType,
    options: Seq[FilterPlan],
    stats: Option[GeoMesaStats],
    explain: Explainer): FilterPlan = throw new NotImplementedError()

  /**
   * Select from available filter plans.
   *
   * This method has a default implementation that delegates to the old method for
   * back-compatibility, but should be overridden going forward
   *
   * @param ds data store
   * @param sft simple feature type being queried
   * @param options filter plans that can satisfy the query
   * @param evaluation cost evaluation type
   * @param explain explain logging
   * @return filter plan to execute
   */
  def selectFilterPlan[DS <: GeoMesaDataStore[DS]](
      ds: DS,
      sft: SimpleFeatureType,
      options: Seq[FilterPlan],
      evaluation: CostEvaluation,
      explain: Explainer): FilterPlan = {
    val stats = evaluation match {
      case CostEvaluation.Stats => Option(ds.stats)
      case CostEvaluation.Index => None
    }
    // noinspection ScalaDeprecation
    selectFilterPlan(sft, options, stats, explain)
  }
}

object StrategyDecider extends MethodProfiling with LazyLogging {

  private val decider: StrategyDecider = SystemProperty("geomesa.strategy.decider").option match {
    case None       => new CostBasedStrategyDecider()
    case Some(clas) => Class.forName(clas).getConstructor().newInstance().asInstanceOf[StrategyDecider]
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
    * @param hints query hints (transform, etc)
    * @param explain for trace logging
    * @return
    */
  def getFilterPlan[DS <: GeoMesaDataStore[DS]](
      ds: DS,
      sft: SimpleFeatureType,
      filter: Filter,
      hints: Hints,
      explain: Explainer = ExplainNull): Seq[FilterStrategy] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

    def complete(op: String, time: Long, count: Int): Unit = explain(s"$op took ${time}ms for $count options")

    val evaluation = hints.getCostEvaluation
    val indices = ds.manager.indices(sft, mode = IndexMode.Read)

    // get the various options that we could potentially use
    val options = profile((o: Seq[FilterPlan], t: Long) => complete("Query processing", t, o.length)) {
      new FilterSplitter(sft, indices).getQueryOptions(filter, hints)
    }

    val selected = profile(t => complete("Strategy selection", t, options.length)) {
      val requested = hints.getRequestedIndex.orNull
      if (requested != null) {
        // see if one of the normal plans matches the requested type - if not, force it
        val forced = matchRequested(requested, options).getOrElse {
          val index =
            indices.find(_.identifier.equalsIgnoreCase(requested))
              .orElse(indices.find(_.name.equalsIgnoreCase(requested)))
              .getOrElse {
                throw new IllegalArgumentException(s"Invalid index strategy: $requested. Valid values are " +
                  indices.map(i => s"${i.name}, ${i.identifier}").mkString(", "))
              }
          val secondary = if (filter == Filter.INCLUDE) { None } else { Some(filter) }
          FilterPlan(Seq(FilterStrategy(index, None, secondary, temporal = false, Float.PositiveInfinity, hints)))
        }
        explain(s"Filter plan forced to $forced")
        forced
      } else if (options.isEmpty) {
        // corresponds to filter.exclude
        // we still need to return something so that we can handle reduce steps, if needed
        explain("No filter plans found - creating empty plan")
        FilterPlan(Seq(FilterStrategy(new EmptyIndex(ds, sft), Some(Filter.EXCLUDE), None, temporal = false, 1f, hints)))
      } else if (options.lengthCompare(1) == 0) {
        // only a single option, so don't bother with cost
        explain(s"Filter plan: ${options.head}")
        options.head
      } else {
        // let the decider implementation choose the best option
        decider.selectFilterPlan(ds, sft, options, evaluation, explain)
      }
    }

    selected.strategies
  }

  private def matchRequested(id: String, options: Seq[FilterPlan]): Option[FilterPlan] = {
    def byId: Option[FilterPlan] = options.find(_.strategies.forall(_.index.identifier.equalsIgnoreCase(id)))
    def byName: Option[FilterPlan] = options.find(_.strategies.forall(_.index.name.equalsIgnoreCase(id)))
    // back-compatibility for attr vs join index name
    def byJoin: Option[FilterPlan] = if (!AttributeIndex.name.equalsIgnoreCase(id)) { None } else {
      options.find(_.strategies.forall(_.index.name == AttributeIndex.JoinIndexName))
    }
    byId.orElse(byName).orElse(byJoin)
  }

  private class CostBasedStrategyDecider extends StrategyDecider {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    override def selectFilterPlan[DS <: GeoMesaDataStore[DS]](
        ds: DS,
        sft: SimpleFeatureType,
        options: Seq[FilterPlan],
        evaluation: CostEvaluation,
        explain: Explainer): FilterPlan = {

      def cost(option: FilterPlan): FilterPlanCost = {
        profile((fpc: FilterPlanCost, time: Long) => fpc.copy(time = time)) {
          var cost = BigInt(0)
          option.strategies.foreach { strategy =>
            val count = evaluation match {
              case CostEvaluation.Stats => ds.adapter.getStrategyCost(strategy, explain)
              case CostEvaluation.Index => None
            }
            cost = cost + BigInt((count.getOrElse(100L) * strategy.costMultiplier).toLong)
          }
          FilterPlanCost(option, if (cost.isValidLong) { cost.longValue } else { Long.MaxValue }, 0L)
        }
      }

      val costs = options.map(cost).sorted

      val temporal = if (!sft.isTemporalPriority) { None } else {
        costs.find(c => c.plan.strategies.nonEmpty && c.plan.strategies.forall(_.temporal))
      }
      val selected = temporal.getOrElse(costs.head)
      explain(s"Filter plan selected: $selected")
      explain(s"Filter plans not selected: ${costs.filterNot(_.eq(selected)).mkString(", ")}")
      selected.plan
    }

    private case class FilterPlanCost(plan: FilterPlan, cost: Long, time: Long) extends Comparable[FilterPlanCost] {
      override def compareTo(o: FilterPlanCost): Int = java.lang.Long.compare(cost, o.cost)
      override def toString: String = s"$plan (Cost $cost in ${time}ms)"
    }
  }
}
