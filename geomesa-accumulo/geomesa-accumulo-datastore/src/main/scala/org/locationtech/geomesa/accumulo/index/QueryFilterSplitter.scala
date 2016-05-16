/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.typesafe.scalalogging.LazyLogging
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.accumulo.data.tables._
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType
import org.locationtech.geomesa.accumulo.index.Strategy.StrategyType.StrategyType
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timing}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/**
 * Class for splitting queries up based on Boolean clauses and the available query strategies.
 */
class QueryFilterSplitter(sft: SimpleFeatureType) extends MethodProfiling with LazyLogging {

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
   * Seq(FilterPlan(Seq(QueryFilter(ST,Seq([ geom bbox ]),Some([ attr1 = ? ])))))
   *
   * bbox(geom) OR attr1 = ? =>
   *
   * Seq(FilterPlan(Seq(QueryFilter(ST,Seq([ geom bbox ]),None), QueryFilter(ATTRIBUTE,Seq([ attr1 = ? ]),None))))
   *
   * bbox(geom) AND dtg DURING ? AND attr1 = ? =>
   *
   * Seq(FilterPlan(Seq(QueryFilter(Z3,Seq([ geom bbox ], [ dtg during ? ]),Some([ attr1 = ? ])))),
   *     FilterPlan(Seq(QueryFilter(ATTRIBUTE,Seq([ attr1 = ? ]),Some([ geom bbox AND dtg during ? ])))))
   * note: spatial and temporal filters are combined.
   *
   * (bbox(geom) OR geom INTERSECT) AND attr1 = ? =>
   *
   * Seq(FilterPlan(Seq(QueryFilter(ST,Seq([ geom bbox ]),Some([ attr1 = ? ])), QueryFilter(ST,Seq([ geom intersect ]),Some([ attr1 = ? ]))))
   *     FilterPlan(Seq(QueryFilter(ATTRIBUTE,Seq([ attr1 = ? ]),Some([ geom bbox OR geom intersect ])))))
   *
   * note: ors will not be split if they are part of the secondary filter
   *
   */
  def getQueryOptions(filter: Filter, output: ExplainerOutputType): Seq[FilterPlan] = {
    implicit val timing = new Timing

    val options = profile {
      rewriteFilterInDNF(filter) match {
        case o: Or  => getOrQueryOptions(filter, o, rewriteFilterInCNF(filter))
        case f      => getAndQueryOptions(f)
      }
    }
    output(s"Query processing took ${timing.time}ms and produced ${options.length} options")
    options
  }

  private def getAndQueryOptions(f: Filter): Seq[FilterPlan] = {
    f match {
      case a: And => getAndQueryOptions(a.getChildren.sortBy(ECQL.toCQL))
      case _      => getAndQueryOptions(Seq(f))
    }
  }

  /**
   * Gets options for filters which are ANDed together, and do not contain any ORs
   */
  private def getAndQueryOptions(filters: Seq[Filter]): Seq[FilterPlan] = {

    // check for filter.exclude - nothing will be returned
    if (filters.contains(Filter.EXCLUDE)) {
      return Seq.empty
    }

    val options = ArrayBuffer.empty[FilterPlan]

    // filter out any whole world geoms, since they are essentially meaningless
    val (invalid, validFilters) = filters.filter(_ != Filter.INCLUDE).partition(FilterHelper.isFilterWholeWorld)
    if (invalid.nonEmpty) {
      logger.debug(s"Dropping whole world filters: ${invalid.map(filterToString).mkString(", ")}")
    }
    val (spatial, temporal, attribute, dateAttribute, others) = partitionFilters(validFilters)

    // z2 and ST - spatial only queries
    if (spatial.nonEmpty) {
      if (supported.contains(Z2Table)) {
        val primary = spatial
        val secondary = andOption(temporal ++ attribute ++ others)
        options.append(FilterPlan(Seq(QueryFilter(StrategyType.Z2, primary, secondary))))
      } else if (supported.contains(SpatioTemporalTable)) {
        val primary = spatial ++ temporal
        val secondary = andOption(attribute ++ others)
        // noinspection ScalaDeprecation
        options.append(FilterPlan(Seq(QueryFilter(StrategyType.ST, primary, secondary))))
      }
    }

    // z3 - spatial and temporal - also use for temporal only, if it's not indexed separately
    if (supported.contains(Z3Table) && isBounded(temporal) &&
        (spatial.nonEmpty || !supported.contains(AttributeTable) || dateAttribute.isEmpty)) {
      val primary = spatial ++ temporal
      val secondary = andOption(attribute ++ others)
      options.append(FilterPlan(Seq(QueryFilter(StrategyType.Z3, primary, secondary))))
    }

    // ids
    if (others.nonEmpty) {
      val ids = others.collect { case id: Id => id }
      if (supported.contains(RecordTable) && ids.nonEmpty) {
        val primary = ids
        val nonIds = others.filterNot(ids.contains) ++ spatial ++ temporal ++ attribute
        val secondary = andOption(nonIds)
        options.append(FilterPlan(Seq(QueryFilter(StrategyType.RECORD, primary, secondary))))
      }
    }

    // attributes
    if (supported.contains(AttributeTable) && (attribute.nonEmpty || dateAttribute.nonEmpty)) {
      val allAttributes = attribute ++ dateAttribute
      allAttributes.foreach { attr =>
        val primary = Seq(attr)
        val nonPrimary = allAttributes.filterNot(primary.contains) ++
          temporal.filterNot(primary.contains) ++ spatial ++ others
        val secondary = andOption(nonPrimary)
        options.append(FilterPlan(Seq(QueryFilter(StrategyType.ATTRIBUTE, primary, secondary))))
      }
    }

    // fall back - full table scan
    if (options.isEmpty) {
      options.append(fullTableScanOption(andOption(validFilters)))
    }

    options
  }

  /**
    * Get options for a filter which contain an 'OR'
    *
    * @param filter original filter as passed in
    * @param dnf same filter in disjunctive normal form - OR will be only at the top of the filter tree
    * @param cnf same filter in conjunctive normal form - AND will be only at the top of the filter tree
    * @return
    */
  private def getOrQueryOptions(filter: Filter, dnf: Or, cnf: Filter): Seq[FilterPlan] = {

    import QueryFilterSplitter.isFullTableScan

    // check for basic ORs with no nested ANDs/Ors
    // e.g a  = '1' OR bbox OR during
    def basicOrs: Option[Seq[FilterPlan]] = Some(cnf).collect { case o: Or => o.getChildren }.map { ors =>
      // detect single attribute queries like "name in ('1', '2')"
      val attributes = ors.flatMap(getAttributeProperty).map(_.name)
      if (attributes.length == ors.length && attributes.forall(_ == attributes.head)) {
        Seq(FilterPlan(Seq(getSameAttributeOption(ors))))
      } else {
        // get the option for each piece of the OR and combine them
        val orOptions = ors.map(f => getAndQueryOptions(Seq(f)))
        if (orOptions.exists(opt => opt.length == 1 && opt.head.filters.exists(isFullTableScan))) {
          // if we have to do a full table scan, we can just compress everything into that
          Seq(fullTableScanOption(Some(filter)))
        } else {
          // multiple attributes being queried, come up with the different permutations of options
          // note: this is O(2^n) but at most will usually only have 1-2 options
          orOptions.reduce { (left, right) => left.flatMap(l => right.map(r => FilterPlan(l.filters ++ r.filters))) }
        }
      }
    }

    // check for attribute ORs that are ANDed with non-attribute filters
    // e.g. a in ('1', '2') AND bbox AND during
    def attributeOrs: Option[Seq[FilterPlan]] = Some(cnf).collect { case a: And => a }.flatMap { a =>
      val (orFilters, nonOrs) = a.getChildren.partition(_.isInstanceOf[Or])
      // we only deal with a single OR here
      val singleOr = Some(orFilters).collect {case Seq(o: Or) => o.getChildren }
      // make sure that the ORs are on a single attribute, and not a spatio-temporal one
      val singleAttributeOr = singleOr.filter { ors =>
        // TODO https://geomesa.atlassian.net/browse/GEOMESA-1168
        // TODO this won't take into account indexed dates... but our getSameAttributeOption doesn't either
        val attributes = ors.flatMap(getAttributeProperty).map(_.name)
        attributes.length == ors.length && attributes.forall(_ == attributes.head) &&
            attributes.head != sft.getGeomField && !sft.getDtgField.contains(attributes.head)
      }
      singleAttributeOr.map { ors =>
        // we can create one plan for the single attribute filter, then AND the other filters to that,
        // and also AND the attribute ORs to the other options
        val attributePlan = Some(getSameAttributeOption(ors)).collect {
          // only consider the attribute option if it results in a real plan (e.g. attribute is indexed)
          // secondary part of attribute option should be empty so we can overwrite it
          case qf if !isFullTableScan(qf) => FilterPlan(Seq(qf.copy(secondary = andOption(nonOrs))))
        }
        val nonAttributeOptions = getAndQueryOptions(nonOrs).filterNot(_.filters.forall(isFullTableScan))
        val nonAttributePlan = nonAttributeOptions.map { fp =>
          FilterPlan(fp.filters.map(qf => qf.copy(secondary = andOption(orOption(ors).toSeq ++ qf.secondary))))
        }
        nonAttributePlan ++ attributePlan
      }
    }

    // handles mixes of spatio-temporal attributes ORed together
    // e.g. (bbox OR bbox) AND during, (bbox AND during) OR (bbox AND during)
    def spatioTemporalOrs: Option[Seq[FilterPlan]] = Some(dnf).collect { case o: Or => o.getChildren }.flatMap { ors =>
      val orOptions: Seq[Seq[FilterPlan]] = ors.map(getAndQueryOptions)
      val z2Option = orOptions.flatMap(o => o.find(_.filters.exists(_.strategy == StrategyType.Z2)))
      val z3Option = orOptions.flatMap(o => o.find(_.filters.exists(_.strategy == StrategyType.Z3)))
      val z2Plan = if (z2Option.length != ors.length) { None } else {
        Some(z2Option.head.copy(filters = z2Option.flatMap(_.filters)))
      }
      val z3Plan = if (z3Option.length != ors.length) { None } else {
        Some(z3Option.head.copy(filters = z3Option.flatMap(_.filters)))
      }
      Some(z2Plan.toSeq ++ z3Plan).filter(_.nonEmpty)
    }

    val options = basicOrs.orElse(attributeOrs).orElse(spatioTemporalOrs).getOrElse {
      logger.warn(s"Falling back to expand/reduce query splitting for filter ${filterToString(filter)}")
      expandReduceOrOptions(dnf)
    }

    if (options.nonEmpty) {
      options.map(makeDisjoint)
    } else {
      Seq(fullTableScanOption(Some(filter)))
    }
  }

  /**
    * Calculates all possible options for each part of the filter, then determines all permutations of
    * the options. This can end up being expensive (O(2&#94;n)), so is only used as a fall-back.
    */
  private def expandReduceOrOptions(filter: Or): Seq[FilterPlan] = {

    // for each child of the or, get the query options
    // each filter plan should only have a single query filter
    def getChildOptions: Seq[Seq[FilterPlan]] = filter.getChildren.map(getAndQueryOptions)

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
    merged
  }

  /**
   * Splits filters up according to the filter type. Note that the 'dateAttribute' will be a duplicate
   * of a filter in the 'temporal' filter list.
   */
  private def partitionFilters(filters: Seq[Filter]) = {
    val (spatial, nonSpatial)         = partitionPrimarySpatials(filters, sft)
    val (temporal, nonSpatioTemporal) = partitionPrimaryTemporals(nonSpatial, sft)
    val (attribute, others)           = partitionIndexedAttributes(nonSpatioTemporal, sft)
    val dateAttribute                 = partitionIndexedAttributes(temporal, sft)._1

    (spatial, temporal, attribute, dateAttribute, others)
  }

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
      // TODO we could technically check for overlapping geoms, date ranges, attribute ranges, etc
      // not sure it's worth it though
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

  private def getSameAttributeOption(filters: Seq[Filter]): QueryFilter = {
    // just get the options for the head filter, the others should be the same
    val headAttributeOptions = getAndQueryOptions(filters.head)
    require(headAttributeOptions.length == 1,
      s"Should have only gotten a single option for non-OR query ${filterToString(filters.head)}")
    require(headAttributeOptions.head.filters.length == 1,
      s"Should have only gotten a single filter for non-OR query ${filterToString(filters.head)}")

    val qf = headAttributeOptions.head.filters.head
    qf.secondary match {
      case None    => qf.copy(primary = filters.tail ++ qf.primary, or = true)
      case Some(s) => qf.copy(secondary = orOption(filters.tail ++ qf.secondary))
    }
  }

  /**
   * Returns true if the temporal filters create a range with an upper and lower bound
   */
  private def isBounded(temporalFilters: Seq[Filter]): Boolean = {
    import FilterHelper._
    val interval = extractInterval(temporalFilters, sft.getDtgField)
    interval != null && interval.getStartMillis != minDateTime && interval.getEndMillis != maxDateTime
  }

  /**
   * Will perform a full table scan - used when we don't have anything better. Uses z2 table
   */
  private def fullTableScanOption(filter: Option[Filter]): FilterPlan = {
    if (supported.contains(Z2Table)) {
      FilterPlan(Seq(QueryFilter(StrategyType.Z2, Seq(Filter.INCLUDE), filter)))
    } else if (supported.contains(RecordTable)) {
      FilterPlan(Seq(QueryFilter(StrategyType.RECORD, Seq(Filter.INCLUDE), filter)))
    } else {
      throw new UnsupportedOperationException("Configured indices do not support the query " +
          s"${filterToString(filter.getOrElse(Filter.INCLUDE))}")
    }
  }
}

object QueryFilterSplitter {

  /**
    * Checks if the query filter is a full table scan
    */
  def isFullTableScan(qf: QueryFilter) = qf.primary == Seq(Filter.INCLUDE)
}

/**
 * Filters split into a 'primary' that will be used for range planning,
 * and a 'secondary' that will be applied as a final step.
 */
case class QueryFilter(strategy: StrategyType,
                       primary: Seq[Filter],
                       secondary: Option[Filter] = None,
                       or: Boolean = false) {
  lazy val filter: Option[Filter] = {
    val incl = if (or) andOption(orOption(primary).toSeq ++ secondary) else andOption(primary ++ secondary)
    incl.filter(_ != Filter.INCLUDE)
  }
  override lazy val toString: String =
    s"$strategy[${primary.map(filterToString).mkString(if (or) " OR " else " AND ")}]" +
      s"[${secondary.map(filterToString).getOrElse("None")}]"
}

/**
 * A series of queries required to satisfy a filter - basically split on ORs
 */
case class FilterPlan(filters: Seq[QueryFilter]) {
  override lazy val toString: String = s"FilterPlan[${filters.mkString(",")}]"
}

