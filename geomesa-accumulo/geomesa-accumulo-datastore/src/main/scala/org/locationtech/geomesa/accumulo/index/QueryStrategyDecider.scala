/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import org.geotools.data.Query
import org.locationtech.geomesa.accumulo.data.INTERNAL_GEOMESA_VERSION
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.accumulo.index.QueryHints._
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType.StrategyType
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

trait QueryStrategyDecider {
  def chooseStrategies(sft: SimpleFeatureType,
                       query: Query,
                       hints: StrategyHints,
                       requested: Option[StrategyType]): Seq[Strategy]
}

object QueryStrategyDecider {

  // first element is null so that the array index aligns with the version
  private val strategies: Array[QueryStrategyDecider] =
    Array[QueryStrategyDecider](null) ++ (1 to INTERNAL_GEOMESA_VERSION).map(QueryStrategyDecider.apply)

  def apply(version: Int): QueryStrategyDecider = {
    if (version <= 4) new QueryStrategyDeciderV4 else new QueryStrategyDeciderV5
  }

  def chooseStrategies(sft: SimpleFeatureType,
                       query: Query,
                       hints: StrategyHints,
                       requested: Option[StrategyType],
                       version: Int): Seq[Strategy] = {
    strategies(version).chooseStrategies(sft, query, hints, requested)
  }
}

class QueryStrategyDeciderV5 extends QueryStrategyDecider {

  /**
   * Scans the filter and identify the type of predicates present, then picks a strategy based on cost.
   * Currently, the costs are hard-coded to conform to the following priority:
   *
   *   * If an ID predicate is present, it is assumed that only a small number of IDs are requested
   *            --> The Record Index is scanned, and the other ECQL filters, if any, are then applied
   *
   *   * If high cardinality attribute filters are present, then use the attribute strategy
   *            --> The Attribute Indices are scanned, and the other ECQL filters, if any, are then applied
   *
   *   * If ST filters are present, use the STIdxStrategy
   *            --> The ST Index is scanned, and the other ECQL filters, if any are then applied
   *
   *   * If other attribute filters are present, then use the Attribute strategy
   *            --> The Attribute Indices are scanned, and the other ECQL filters, if any, are then applied
   *
   *   * If filters are not identified, use the STIdxStrategy
   *            --> The ST Index is scanned (likely a full table scan) and the ECQL filters are applied
   */
  override def chooseStrategies(sft: SimpleFeatureType,
                                query: Query,
                                hints: StrategyHints,
                                requested: Option[StrategyType]): Seq[Strategy] = {

    // get the various options that we could potentially use
    val options = new QueryFilterSplitter(sft, supportsZ3(sft)).getQueryOptions(query.getFilter)

    if (requested.isDefined) {
      // see if one of the normal plans matches the requested type - if not, force it
      val strategy = requested.get
      options.find(_.filters.forall(_.strategy == strategy)) match {
        case Some(plan) => plan.filters.map(createStrategy)
        case None => Seq(createStrategy(QueryFilter(strategy, Seq(Filter.INCLUDE), Some(query.getFilter))))
      }
    } else if (options.isEmpty) {
      Seq.empty // corresponds to filter.exclude
    } else {
      val filterPlan = if (query.getHints.isDensityQuery) {
        // TODO GEOMESA-322 use other strategies with density iterator
        val st = options.find(_.filters.forall(q => q.strategy == StrategyType.Z3 ||
            q.strategy == StrategyType.ST))
        st.getOrElse {
          val fallback = if (query.getFilter == Filter.INCLUDE) None else Some(query.getFilter)
          FilterPlan(Seq(QueryFilter(StrategyType.ST, Seq(Filter.INCLUDE), fallback)))
        }

      } else if (options.length == 1) {
        // only a single option, so don't bother with cost
        options.head
      } else {
        // choose the best option based on cost
        options.sortBy { filterPlan =>
          filterPlan.filters.map { filter =>
            filter.strategy match {
              case StrategyType.Z3 => Z3IdxStrategy.getCost(filter, sft, hints)
              case StrategyType.ST => STIdxStrategy.getCost(filter, sft, hints)
              case StrategyType.RECORD => RecordIdxStrategy.getCost(filter, sft, hints)
              case StrategyType.ATTRIBUTE => AttributeIdxStrategy.getCost(filter, sft, hints)
              case _ => throw new IllegalStateException(s"Unknown query plan requested: ${filter.strategy}")
            }
          }.sum
        }.head
      }
      filterPlan.filters.map(createStrategy)
    }
  }

  /**
   * Mapping from strategy type enum to concrete implementation class
   */
  private def createStrategy(filter: QueryFilter): Strategy = {
    filter.strategy match {
      case StrategyType.Z3        => new Z3IdxStrategy(filter)
      case StrategyType.ST        => new STIdxStrategy(filter)
      case StrategyType.RECORD    => new RecordIdxStrategy(filter)
      case StrategyType.ATTRIBUTE => new AttributeIdxStrategy(filter)
      case _ => throw new IllegalStateException(s"Unknown query plan requested: ${filter.strategy}")
    }
  }

  def supportsZ3(sft: SimpleFeatureType): Boolean = Z3Table.supports(sft)
}

class QueryStrategyDeciderV4 extends QueryStrategyDeciderV5 {
  // version 4 does not ever support z3
  override def supportsZ3(sft: SimpleFeatureType): Boolean = false
}
