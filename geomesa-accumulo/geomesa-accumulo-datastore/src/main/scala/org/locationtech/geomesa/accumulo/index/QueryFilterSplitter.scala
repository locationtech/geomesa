/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.accumulo.data.tables._
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType.StrategyType
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.{FilterExtractingVisitor, IdDetectingFilterVisitor, IdExtractingVisitor}
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Class for splitting queries up based on Boolean clauses and the available query strategies.
 */
class QueryFilterSplitter(sft: SimpleFeatureType) extends LazyLogging {

  import QueryFilterSplitter._

  val supported = GeoMesaTable.getTables(sft).toSet

  /**
    * Splits the query up into different filter plans to be evaluated. Each filter plan will consist of one or
    * more query plans. Each query plan will have a primary part (that would be used for query planning)
    * and an optional secondary part (that would be applied as a secondary filter).
    *
    * Examples:
    *
    * bbox(geom) AND attr1 = ? =>
    *
    * Seq(FilterPlan(Seq(QueryFilter(ST,Some([ geom bbox ]),Some([ attr1 = ? ])))))
    *
    * bbox(geom) OR attr1 = ? =>
    *
    * Seq(FilterPlan(Seq(QueryFilter(ST,Some([ geom bbox ]),None), QueryFilter(ATTRIBUTE,Some([ attr1 = ? ]),None))))
    *
    * bbox(geom) AND dtg DURING ? AND attr1 = ? =>
    *
    * Seq(FilterPlan(Seq(QueryFilter(Z3,Some([ geom bbox AND dtg during ? ]),Some([ attr1 = ? ])))),
    *     FilterPlan(Seq(QueryFilter(ATTRIBUTE,Some([ attr1 = ? ]),Some([ geom bbox AND dtg during ? ])))))
    *
    * note: spatial and temporal filters are combined.
    *
    * (bbox(geom) OR geom INTERSECT) AND attr1 = ? =>
    *
    * Seq(FilterPlan(Seq(QueryFilter(ST,Some([ geom bbox OR geom intersect ]),Some([ attr1 = ? ]))))
    *     FilterPlan(Seq(QueryFilter(ATTRIBUTE,Seq([ attr1 = ? ]),Some([ geom bbox OR geom intersect ])))))
    *
    * note: ORs will not be split if they operate on a single attribute
    *
    */
  def getQueryOptions(filter: Filter): Seq[FilterPlan] = {
    // cnf gives us a top level AND with ORs as first children
    rewriteFilterInCNF(filter) match {
      case a: And =>
        // look for ORs across attributes, e.g. bbox OR dtg
        val (complex, simple) = a.getChildren.partition(f => f.isInstanceOf[Or] && attributeAndIdCount(f) > 1)
        if (complex.isEmpty) {
          // no cross-attribute ORs
          getSimpleQueryOptions(a).map(FilterPlan.apply)
        } else if (simple.nonEmpty) {
          logger.warn("Not considering complex OR predicates in query planning: " +
              s"${complex.map(filterToString).mkString("(", ") AND (", ")")}")
          def addComplexPredicates(qf: QueryFilter) = qf.copy(secondary = andOption(qf.secondary.toSeq ++ complex))
          getSimpleQueryOptions(andFilters(simple)).map(addComplexPredicates).map(FilterPlan.apply)
        } else {
          logger.warn(s"Falling back to expand/reduce query splitting for filter ${filterToString(filter)}")
          val dnf = rewriteFilterInDNF(filter).asInstanceOf[Or]
          expandReduceOrOptions(dnf).map(makeDisjoint)
        }

      case o: Or =>
        // there are no ands - just ors between fields
        // this implies that each child has only a single property or ID
        def getGroup(f: Filter) = (FilterHelper.propertyNames(f, sft), FilterHelper.hasIdFilter(f))

        // group and then recombine the OR'd filters by the attribute they operate on
        val groups = o.getChildren.groupBy(getGroup).values.map(ff.or(_)).toSeq
        val perAttributeOptions = groups.flatMap { g =>
          val options = getSimpleQueryOptions(g)
          require(options.length < 2, s"Expected only a single option for ${filterToString(g)} but got $options")
          options.headOption
        }
        if (perAttributeOptions.exists(_.primary.isEmpty)) {
          // we have to do a full table scan for part of the query, just append everything to that
          Seq(FilterPlan(fullTableScanOption(o)))
        } else {
          Seq(makeDisjoint(FilterPlan(perAttributeOptions)))
        }

      case f =>
        getSimpleQueryOptions(f).map(qf => FilterPlan(Seq(qf)))
    }
  }

  /**
    * Gets options for a 'simple' filter, where each OR is on a single attribute, e.g.
    *   (bbox1 OR bbox2) AND dtg
    *   bbox AND dtg AND (attr1 = foo OR attr = bar)
    * not:
    *   bbox OR dtg
    *
    * Because the inputs are simple, each one can be satisfied with a single query filter.
    * The returned values will each satisfy the query, using a different strategy.
    *
    * @param filter input filter
    * @return sequence of options, any of which can satisfy the query
    */
  private def getSimpleQueryOptions(filter: Filter): Seq[QueryFilter] = {

    if (filter == Filter.INCLUDE) {
      return Seq(fullTableScanOption(filter))
    } else if (filter == Filter.EXCLUDE) {
      return Seq.empty
    }

    // buffer to store our results
    val options = ArrayBuffer.empty[QueryFilter]

    val attributes = FilterHelper.propertyNames(filter, sft)
    lazy val temporalIndexed = sft.getDtgField.map(sft.getDescriptor).exists(_.isIndexed)

    // record index check - ID filters
    if (supported.contains(RecordTable)) {
      val (ids, notIds) = IdExtractingVisitor(filter)
      if (ids.isDefined) {
        options.append(QueryFilter(StrategyType.RECORD, ids, notIds))
      }
    }

    // z2 (or ST) and z3 index check - spatial and spatio-temporal filters
    if (attributes.contains(sft.getGeomField)) {
      // check for spatial index - either z2 or ST (for old schemas)
      val (spatial, nonSpatial) = FilterExtractingVisitor(filter, sft.getGeomField, sft, Z2IdxStrategy.spatialCheck)
      val (temporal, others) = (sft.getDtgField, nonSpatial) match {
        case (Some(dtg), Some(ns)) => FilterExtractingVisitor(ns, dtg, sft)
        case _ => (None, nonSpatial)
      }
      lazy val spatioTemporal = andOption((spatial ++ temporal).toSeq)

      if (spatial.isDefined) {
        if (supported.contains(Z2Table)) {
          options.append(QueryFilter(StrategyType.Z2, spatial, nonSpatial))
        } else if(supported.contains(XZ2Table)) {
          options.append(QueryFilter(StrategyType.XZ2, spatial, nonSpatial))
        } else if (supported.contains(SpatioTemporalTable)) {
          // noinspection ScalaDeprecation
          options.append(QueryFilter(StrategyType.ST, spatioTemporal, others))
        }
      }
      // use z3 if we have both spatial and temporal predicates,
      // or if we just have a temporal predicate and the date is not attribute indexed
      if (supported.contains(Z3Table) && temporal.exists(isBounded) &&
          (spatial.isDefined || !supported.contains(AttributeTable) || !temporalIndexed)) {
        options.append(QueryFilter(StrategyType.Z3, spatioTemporal, others))
      }
    } else if (sft.getDtgField.exists(attributes.contains)) {
      // check for z3 without a geometry
      lazy val (temporal, others) = sft.getDtgField match {
        case None => (None, Some(filter))
        case Some(dtg) => FilterExtractingVisitor(filter, dtg, sft)
      }
      // use z3 if we just have a temporal predicate and the date is not attribute indexed
      if (supported.contains(Z3Table) && temporal.exists(isBounded) &&
          !(supported.contains(AttributeTable) && temporalIndexed)) {
        options.append(QueryFilter(StrategyType.Z3, temporal, others))
      }
    }

    // attribute index check
    if (supported.contains(AttributeTable)) {
      val indexedAttributes = attributes.filter(a => Option(sft.getDescriptor(a)).exists(_.isIndexed))
      indexedAttributes.foreach { attribute =>
        val (primary, secondary) = FilterExtractingVisitor(filter, attribute, sft, AttributeIdxStrategy.attributeCheck)
        if (primary.isDefined) {
          options.append(QueryFilter(StrategyType.ATTRIBUTE, primary, secondary))
        }
      }
    }

    // fall back - full table scan
    if (options.isEmpty) {
      options.append(fullTableScanOption(filter))
    }

    options
  }

  /**
    * Calculates all possible options for each part of the filter, then determines all permutations of
    * the options. This can end up being expensive (O(2&#94;n)), so is only used as a fall-back.
    */
  private def expandReduceOrOptions(filter: Or): Seq[FilterPlan] = {

    // for each child of the or, get the query options
    // each filter plan should only have a single query filter
    def getChildOptions: Seq[Seq[FilterPlan]] =
      filter.getChildren.map(getSimpleQueryOptions(_).map(qf => FilterPlan(Seq(qf))))

    // combine the filter plans so that each plan has multiple query filters
    // use the permutations of the different options for each child
    // TODO GEOMESA-941 Fix algorithmically dangerous (2^N exponential runtime)
    def reduceChildOptions(childOptions: Seq[Seq[FilterPlan]]): Seq[FilterPlan] =
      childOptions.reduce { (left, right) =>
        left.flatMap(l => right.map(r => FilterPlan(l.filters ++ r.filters)))
      }

    // try to combine query filters in each filter plan if they have the same primary filter
    // this avoids scanning the same ranges twice with different secondary predicates
    def combineSecondaryFilters(options: Seq[FilterPlan]): Seq[FilterPlan] = options.map { r =>
      // build up the result array instead of using a group by to preserve filter order
      val groups = ArrayBuffer.empty[QueryFilter]
      r.filters.distinct.foreach { f =>
        val i = groups.indexWhere(g => g.strategy == f.strategy && g.primary == f.primary)
        if (i == -1) {
          groups.append(f)
        } else {
          val current = groups(i).secondary match {
            case Some(o) if o.isInstanceOf[Or] => o.asInstanceOf[Or].getChildren.toSeq
            case Some(n) => Seq(n)
            case None => Seq.empty
          }
          groups.update(i, f.copy(secondary = orOption(current ++ f.secondary)))
        }
      }
      FilterPlan(groups)
    }

    // if a filter plan has any query filters that scan a subset of the range of a different query filter,
    // then we can combine them, as we have to scan the larger range anyway
    def mergeOverlappedFilters(options: Seq[FilterPlan]): Seq[FilterPlan] = options.map { filterPlan =>
      val filters = ArrayBuffer(filterPlan.filters: _*)
      var merged: QueryFilter = null
      var i = 0
      while (i < filters.length) {
        val toMerge = filters(i)
        if (toMerge != null) {
          var j = 0
          while (j < filters.length && merged == null) {
            if (i != j) {
              val mergeTo = filters(j)
              if (mergeTo != null) {
                merged = tryMerge(toMerge, mergeTo)
              }
            }
            j += 1
          }
          if (merged != null) {
            // remove the merged query filter and replace the one merged into
            filters.update(i, null)
            filters.update(j - 1, merged)
            merged = null
          }
        }
        i += 1
      }

      // if we have replaced anything, recreate the filter plan
      val overlapped = filters.filter(_ != null)
      if (overlapped.length < filterPlan.filters.length) {
        FilterPlan(overlapped)
      } else {
        filterPlan
      }
    }

    val childOpts   = getChildOptions
    val reducedOpts = reduceChildOptions(childOpts)
    val combinedSec = combineSecondaryFilters(reducedOpts)
    val merged      = mergeOverlappedFilters(combinedSec)
    if (merged.nonEmpty) {
      merged
    } else {
      Seq(FilterPlan(Seq(fullTableScanOption(filter))))
    }
  }

  /**
    * Will perform a full table scan - used when we don't have anything better. Currently z3, z2 and record
    * tables support full table scans.
    */
  private def fullTableScanOption(filter: Filter): QueryFilter = {
    val strategy = fullTableScanOptions.find(o => supported.contains(o._1)).map(_._2).getOrElse {
      throw new UnsupportedOperationException(s"Configured indices do not support the query ${filterToString(filter)}")
    }
    val secondary = if (filter == Filter.INCLUDE) None else Some(filter)
    QueryFilter(strategy, None, secondary)
  }


  /**
    * Gets the count of distinct attributes being queried - ID is treated as an attribute
    */
  private def attributeAndIdCount(filter: Filter): Int = {
    val attributeCount = FilterHelper.propertyNames(filter, sft).size
    val idCount = if (filter.accept(new IdDetectingFilterVisitor, false).asInstanceOf[Boolean]) 1 else 0
    attributeCount + idCount
  }

  /**
    * Returns true if the temporal filters create a range with an upper and lower bound
    */
  private def isBounded(temporalFilter: Filter): Boolean = {
    import FilterHelper.{MaxDateTime, MinDateTime}
    val intervals = sft.getDtgField.map(FilterHelper.extractIntervals(temporalFilter, _)).getOrElse(Seq.empty)
    intervals.nonEmpty && intervals.forall { case (start, end) => start != MinDateTime && end != MaxDateTime }
  }
}

object QueryFilterSplitter {

  // strategies that support full table scans, in priority order of which to use
  private val fullTableScanOptions = Seq((Z3Table, StrategyType.Z3), (Z2Table, StrategyType.Z2),
    (XZ2Table, StrategyType.XZ2), (RecordTable, StrategyType.RECORD))

  /**
   * Try to merge the two query filters. Return the merged query filter if successful, else null.
   */
  private def tryMerge(toMerge: QueryFilter, mergeTo: QueryFilter): QueryFilter = {
    if (mergeTo.primary.forall(_ == Filter.INCLUDE)) {
      // this is a full table scan, we can just append the OR to the secondary filter
      val secondary = orOption(mergeTo.secondary.toSeq ++ toMerge.filter)
      mergeTo.copy(secondary = secondary)
    } else if (toMerge.strategy == StrategyType.ATTRIBUTE && mergeTo.strategy == StrategyType.ATTRIBUTE) {
      AttributeIdxStrategy.tryMergeAttrStrategy(toMerge, mergeTo)
    } else {
      // overlapping geoms, date ranges, attribute ranges, etc will be handled when extracting bounds
      null
    }
  }

  /**
    * Make a filter plan disjoint ORs - this way we don't have to deduplicate results
    *
    * @param option filter plan
    * @return same filter plan with disjoint ORs
    */
  private def makeDisjoint(option: FilterPlan): FilterPlan = {
    if (option.filters.length < 2) {
      option
    } else {
      // A OR B OR C becomes... A OR (B NOT A) OR (C NOT A and NOT B)
      def extractNot(qp: QueryFilter) = qp.filter.map(ff.not)
      // keep track of our current disjoint clause
      val nots = ArrayBuffer[Filter]()
      extractNot(option.filters.head).foreach(nots.append(_))
      val filters = Seq(option.filters.head) ++ option.filters.tail.map { filter =>
        val sec = Some(andFilters(nots ++ filter.secondary))
        extractNot(filter).foreach(nots.append(_)) // note - side effect
        filter.copy(secondary = sec)
      }
      FilterPlan(filters)
    }
  }
}

/**
 * Filters split into a 'primary' that will be used for range planning,
 * and a 'secondary' that will be applied as a final step.
 */
case class QueryFilter(strategy: StrategyType, primary: Option[Filter], secondary: Option[Filter] = None) {

  lazy val filter: Option[Filter] = andOption(primary.toSeq ++ secondary)

  override lazy val toString: String =
    s"$strategy[${primary.map(filterToString).getOrElse("INCLUDE")}]" +
        s"[${secondary.map(filterToString).getOrElse("None")}]"
}

/**
 * A series of queries required to satisfy a filter - basically split on ORs
 */
case class FilterPlan(filters: Seq[QueryFilter]) {
  override lazy val toString: String = s"FilterPlan[${filters.mkString(",")}]"
}

object FilterPlan {
  def apply(filter: QueryFilter): FilterPlan = FilterPlan(Seq(filter))
}
