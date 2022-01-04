/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.planning

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.IdDetectingFilterVisitor
import org.locationtech.geomesa.index.api.{FilterPlan, FilterStrategy, GeoMesaFeatureIndex}
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._

import scala.collection.mutable.ArrayBuffer

/**
 * Class for splitting queries up based on Boolean clauses and the available query strategies.
 */
class FilterSplitter(sft: SimpleFeatureType, indices: Seq[GeoMesaFeatureIndex[_, _]]) extends LazyLogging {

  import FilterSplitter._

  import scala.collection.JavaConverters._

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
  def getQueryOptions(filter: Filter, transform: Option[SimpleFeatureType] = None): Seq[FilterPlan] = {
    // cnf gives us a top level AND with ORs as first children
    rewriteFilterInCNF(filter) match {
      case a: And =>
        // look for ORs across attributes, e.g. bbox OR dtg
        val complex = ArrayBuffer.empty[Or]
        val simple = ArrayBuffer.empty[Filter]
        var permutations = 1

        a.getChildren.asScala.foreach {
          case or: Or =>
            val attributes = attributeAndIdCount(or, sft)
            if (attributes > 1) {
              complex += or
              permutations *= attributes
            } else {
              simple += or
            }

          case f =>
            simple += f
        }

        if (complex.isEmpty) {
          // no cross-attribute ORs
          getSimpleQueryOptions(a, transform).map(fs => FilterPlan(Seq(fs)))
        } else {
          // we don't generally consider all the permutations available, as that takes exponential time
          // instead, we consider the cross-attribute-ors and the rest of the query separately

          // the cross-attribute or plans
          // each option should just result in 1 filter plan since there are no ANDs
          val complexOptions = complex.map(getQueryOptions(_, transform))

          // the non-cross-attribute ors, with the ors tacked on as secondary filters at the end
          lazy val simpleOptions = if (simple.isEmpty) { Seq.empty } else {
            getSimpleQueryOptions(andFilters(simple), transform)
                .map(s => FilterPlan(Seq(addSecondaryPredicates(s, complex))))
          }
          lazy val expandReduceOptions =
            expandReduceOrOptions(rewriteFilterInDNF(filter).asInstanceOf[Or], transform)
                .map(fp => FilterPlan(makeDisjoint(fp.strategies)))

          if (complexOptions.forall(o => o.lengthCompare(1) == 0 && o.head.strategies.forall(_.isPreferredScan))) {
            // if each of the complex options has a good scan available, return those plus the simple options
            complexOptions.map(o => FilterPlan(o.head.strategies.map(addSecondaryPredicates(_, simple)))) ++
                simpleOptions
          } else if (ExpandReduceThreshold.toInt.exists(_ > permutations)) {
            logger.debug(s"Using ${getExpandReduceLog(filter, permutations)}")
            expandReduceOptions
          } else if (simpleOptions.nonEmpty) {
            // if there's not a clear option for the complex scans, ignore them and just return the simple ones
            logger.warn("Not considering complex OR predicates in query planning: " +
                s"${complex.map(filterToString).mkString("(", ") AND (", ")")}")
            simpleOptions
          } else {
            // if there aren't any simple plans or clear options for the complex plans,
            // fall back to the expand-reduce logic
            logger.warn(s"Falling back to ${getExpandReduceLog(filter, permutations)}")
            expandReduceOptions
          }
        }

      case o: Or =>
        // there are no ands - just ors between fields
        // this implies that each child has only a single property or ID
        def getGroup(f: Filter): (Seq[String], Boolean) =
          (FilterHelper.propertyNames(f, sft), FilterHelper.hasIdFilter(f))

        // group and then recombine the OR'd filters by the attribute they operate on
        val groups = o.getChildren.asScala.groupBy(getGroup).values.map(g => ff.or(g.asJava)).toSeq
        val perAttributeOptions = groups.flatMap { g =>
          val options = getSimpleQueryOptions(g, transform)
          require(options.length < 2, s"Expected only a single option for ${filterToString(g)} but got $options")
          options.headOption
        }
        if (perAttributeOptions.exists(_.isFullTableScan)) {
          // we have to do a full table scan for part of the query, just append everything to that
          Seq(FilterPlan(Seq(fullTableScanOption(o, transform))))
        } else {
          Seq(FilterPlan(makeDisjoint(perAttributeOptions)))
        }

      case f =>
        getSimpleQueryOptions(f, transform).map(fs => FilterPlan(Seq(fs)))
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
  private def getSimpleQueryOptions(filter: Filter, transform: Option[SimpleFeatureType]): Seq[FilterStrategy] = {
    if (filter == Filter.EXCLUDE) { Seq.empty } else {
      val fullTableScansBuilder  = Seq.newBuilder[FilterStrategy]
      val preferredScansBuilder  = Seq.newBuilder[FilterStrategy]
      val acceptableScansBuilder = Seq.newBuilder[FilterStrategy]

      indices.foreach { i =>
        i.getFilterStrategy(filter, transform).foreach {
          case f if f.isFullTableScan => fullTableScansBuilder += f
          case f if f.isPreferredScan => preferredScansBuilder += f
          case f                      => acceptableScansBuilder += f
        }
      }

      val preferredScans = preferredScansBuilder.result
      if (preferredScans.nonEmpty) { preferredScans } else {
        val acceptableScans = acceptableScansBuilder.result
        if (acceptableScans.nonEmpty) { acceptableScans } else {
          fullTableScansBuilder.result.take(1)
        }
      }
    }
  }

  /**
    * Calculates all possible options for each part of the filter, then determines all permutations of
    * the options. This can end up being expensive (O(2&#94;n)), so is only used as a fall-back.
    */
  private def expandReduceOrOptions(filter: Or, transform: Option[SimpleFeatureType]): Seq[FilterPlan] = {

    // for each child of the or, get the query options
    // each filter plan should only have a single query filter
    def getChildOptions: Seq[Seq[FilterPlan]] =
      filter.getChildren.asScala.map(getSimpleQueryOptions(_, transform).map(fs => FilterPlan(Seq(fs))))

    // combine the filter plans so that each plan has multiple query filters
    // use the permutations of the different options for each child
    // TODO GEOMESA-941 Fix algorithmically dangerous (2^N exponential runtime)
    def reduceChildOptions(childOptions: Seq[Seq[FilterPlan]]): Seq[FilterPlan] =
      childOptions.reduce { (left, right) =>
        left.flatMap(l => right.map(r => FilterPlan(l.strategies ++ r.strategies)))
      }

    // try to combine query filters in each filter plan if they have the same primary filter
    // this avoids scanning the same ranges twice with different secondary predicates
    def combineSecondaryFilters(options: Seq[FilterPlan]): Seq[FilterPlan] = options.map { r =>
      // build up the result array instead of using a group by to preserve filter order
      val groups = ArrayBuffer.empty[FilterStrategy]
      r.strategies.distinct.foreach { f =>
        val i = groups.indexWhere(g => g.index == f.index && g.primary == f.primary)
        if (i == -1) {
          groups.append(f)
        } else {
          val current = groups(i).secondary match {
            case Some(o: Or) => o.getChildren.asScala
            case Some(n) => Seq(n)
            case None => Seq.empty
          }
          val secondary = orOption(current ++ f.secondary)
          val temporal = f.temporal && groups(i).temporal
          groups.update(i, FilterStrategy(f.index, f.primary, secondary, temporal, f.costMultiplier))
        }
      }
      FilterPlan(groups)
    }

    // if a filter plan has any query filters that scan a subset of the range of a different query filter,
    // then we can combine them, as we have to scan the larger range anyway
    def mergeOverlappedFilters(options: Seq[FilterPlan]): Seq[FilterPlan] = options.map { filterPlan =>
      val filters = ArrayBuffer[FilterStrategy](filterPlan.strategies: _*)
      var merged: FilterStrategy = null
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
      if (overlapped.length < filterPlan.strategies.length) {
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
      Seq(FilterPlan(Seq(fullTableScanOption(filter, transform))))
    }
  }

  /**
    * Will perform a full table scan - used when we don't have anything better. Currently z3, z2 and record
    * tables support full table scans.
    */
  private def fullTableScanOption(filter: Filter, transform: Option[SimpleFeatureType]): FilterStrategy = {
    val secondary = if (filter == Filter.INCLUDE) { None } else { Some(filter) }
    // note: early return after we find a matching strategy
    val iter = indices.iterator
    while (iter.hasNext) {
      iter.next().getFilterStrategy(Filter.INCLUDE, transform) match {
        case None => // no-op
        case Some(i) => return FilterStrategy(i.index, i.primary, secondary, i.temporal, i.costMultiplier)
      }
    }
    throw new UnsupportedOperationException(s"Configured indices do not support the query ${filterToString(filter)}")
  }

  private def addSecondaryPredicates(filter: FilterStrategy, predicates: Seq[Filter]): FilterStrategy = {
    val secondary = andOption(filter.secondary.toSeq ++ predicates)
    FilterStrategy(filter.index, filter.primary, secondary, filter.temporal, filter.costMultiplier)
  }

  private def getExpandReduceLog(filter: Filter, permutations: Int): String =
    s"expand/reduce query splitting with $permutations permutations for filter ${filterToString(filter)}"

}

object FilterSplitter {

  val ExpandReduceThreshold: SystemProperty = SystemProperty("geomesa.query.processing.or.threshold")

  /**
    * Gets the count of distinct attributes being queried - ID is treated as an attribute
    */
  def attributeAndIdCount(filter: Filter, sft: SimpleFeatureType): Int = {
    val idCount = if (filter.accept(new IdDetectingFilterVisitor, false).asInstanceOf[Boolean]) { 1 } else { 0 }
    FilterHelper.propertyNames(filter, sft).length + idCount
  }

  /**
   * Try to merge the two query filters. Return the merged query filter if successful, else null.
   */
  def tryMerge(toMerge: FilterStrategy, mergeTo: FilterStrategy): FilterStrategy = {
    if (mergeTo.primary.forall(_ == Filter.INCLUDE)) {
      // this is a full table scan, we can just append the OR to the secondary filter
      val secondary = orOption(mergeTo.secondary.toSeq ++ toMerge.filter)
      FilterStrategy(mergeTo.index, mergeTo.primary, secondary, mergeTo.temporal, mergeTo.costMultiplier)
    } else if (toMerge.index.name == AttributeIndex.name && mergeTo.index.name == AttributeIndex.name) {
      // TODO extract this out into the API?
      tryMergeAttrStrategy(toMerge, mergeTo)
    } else {
      // overlapping geoms, date ranges, attribute ranges, etc will be handled when extracting bounds
      null
    }
  }

  /**
    * Tries to merge the two filters that are OR'd together into a single filter that can be queried in one pass.
    * Will return the merged filter, or null if they can't be merged.
    *
    * We can merge filters if they have the same secondary filter AND:
    *   1. One of them does not have a primary filter
    *   2. They both have a primary filter on the same attribute
    *
    * @param toMerge first filter
    * @param mergeTo second filter
    * @return merged filter that satisfies both inputs, or null if that isn't possible
    */
  def tryMergeAttrStrategy(toMerge: FilterStrategy, mergeTo: FilterStrategy): FilterStrategy = {
    // TODO this will be incorrect for multi-valued properties where we have an AND in the primary filter
    val leftAttributes = toMerge.primary.map(FilterHelper.propertyNames(_, null))
    val rightAttributes = mergeTo.primary.map(FilterHelper.propertyNames(_, null))

    val canMergePrimary = (leftAttributes, rightAttributes) match {
      case (Some(left), Some(right)) => left.length == 1 && right.length == 1 && left.head == right.head
      case _ => true
    }

    if (canMergePrimary && toMerge.secondary == mergeTo.secondary) {
      val primary = orOption(toMerge.primary.toSeq ++ mergeTo.primary)
      FilterStrategy(mergeTo.index, primary, mergeTo.secondary, mergeTo.temporal, mergeTo.costMultiplier)
    } else {
      null
    }
  }

  /**
    * Make a filter plan disjoint ORs - this way we don't have to deduplicate results
    *
    * @param strategies strategies
    * @return same strategies with disjoint ORs
    */
  private def makeDisjoint(strategies: Seq[FilterStrategy]): Seq[FilterStrategy] = {
    if (strategies.lengthCompare(2) < 0) { strategies } else {
      // A OR B OR C becomes... A OR (B NOT A) OR (C NOT A and NOT B)
      // keep track of our current disjoint clause
      val nots = ArrayBuffer.empty[Filter]
      strategies.map { filter =>
        val res = if (nots.isEmpty) { filter } else {
          val secondary = Some(andFilters(nots ++ filter.secondary))
          FilterStrategy(filter.index, filter.primary, secondary, filter.temporal, filter.costMultiplier)
        }
        nots ++= filter.filter.map(ff.not) // note - side effect
        res
      }
    }
  }
}
