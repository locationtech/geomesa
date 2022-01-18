/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
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

  @deprecated("replaced with selectFilterPlan(SimpleFeatureType,Seq[FilterPlan],Option[GeoMesaStats],Explainer)")
  def selectFilterPlan(sft: SimpleFeatureType, options: Seq[FilterPlan], explain: Explainer): FilterPlan

  /**
   * Select from available filter plans
   *
   * @param sft simple feature type being queried
   * @param options filter plans that can satisfy the query
   * @param stats stats (if available)
   * @param explain explain logging
   * @return filter plan to execute
   */
  def selectFilterPlan(
      sft: SimpleFeatureType,
      options: Seq[FilterPlan],
      stats: Option[GeoMesaStats],
      explain: Explainer): FilterPlan = {
    // TODO remove default impl in next major release
    // noinspection ScalaDeprecation
    selectFilterPlan(sft, options, explain)
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
  def getFilterPlan[DS <: GeoMesaDataStore[DS]](ds: DS,
                                                sft: SimpleFeatureType,
                                                filter: Filter,
                                                transform: Option[SimpleFeatureType],
                                                evaluation: CostEvaluation,
                                                requested: Option[String],
                                                explain: Explainer = ExplainNull): Seq[FilterStrategy] = {

    def complete(op: String, time: Long, count: Int): Unit = explain(s"$op took ${time}ms for $count options")

    val indices = ds.manager.indices(sft, mode = IndexMode.Read)

    // get the various options that we could potentially use
    val options = profile((o: Seq[FilterPlan], t: Long) => complete("Query processing", t, o.length)) {
      new FilterSplitter(sft, indices).getQueryOptions(filter, transform)
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
        // choose the best option based on cost
        val stats = evaluation match {
          case CostEvaluation.Stats => Some(ds.stats)
          case CostEvaluation.Index => None
        }
        decider.selectFilterPlan(sft, options, stats, explain)
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
      FilterPlan(Seq(FilterStrategy(index, None, secondary, temporal = false, Float.PositiveInfinity)))
    }

    byId.orElse(byName).orElse(byJoin).getOrElse(fallback)
  }

  class CostBasedStrategyDecider extends StrategyDecider with MethodProfiling {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    // noinspection ScalaDeprecation
    override def selectFilterPlan(
        sft: SimpleFeatureType,
        options: Seq[FilterPlan],
        explain: Explainer): FilterPlan = selectFilterPlan(sft, options, None, explain)

    override def selectFilterPlan(
        sft: SimpleFeatureType,
        options: Seq[FilterPlan],
        stats: Option[GeoMesaStats],
        explain: Explainer): FilterPlan = {

      def cost(option: FilterPlan): FilterPlanCost = {
        profile((fpc: FilterPlanCost, time: Long) => fpc.copy(time = time)) {
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
