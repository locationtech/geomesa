/*
 * Copyright 2014-2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.index

import java.util

import org.geotools.data.Query
import org.locationtech.geomesa.core.index.AttributeIndexStrategy.getAttributeIndexStrategy
import org.locationtech.geomesa.core.index.FilterHelper._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.core.index.RecordIdxStrategy.getRecordIdxStrategy
import org.locationtech.geomesa.core.index.STIdxStrategy.getSTIdxStrategy
import org.locationtech.geomesa.utils.stats.Cardinality
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.{And, Filter, Id, PropertyIsLike}

import scala.collection.JavaConversions._
import scala.collection.mutable

object QueryStrategyDecider {

  def chooseStrategy(sft: SimpleFeatureType, query: Query, hints: StrategyHints, version: Int): Strategy = {
    if (version < 1) {
      // if datastore doesn't support attr index use spatiotemporal only
      return new STIdxStrategy
    }

    val isDensity = query.getHints.containsKey(BBOX_KEY) || query.getHints.contains(TIME_BUCKETS_KEY)
    if (isDensity) {
      // TODO GEOMESA-322 use other strategies with density iterator
      return new STIdxStrategy
    }

    val filter = query.getFilter
    // check if we can use the attribute index first
    val attributeStrategy = getAttributeIndexStrategy(filter, sft)
    attributeStrategy.getOrElse {
      filter match {
        case idFilter: Id => new RecordIdxStrategy
        case and: And     => processAnd(and, sft, hints)
        case cql          => new STIdxStrategy
      }
    }
  }

  case class StrategyAndFilter(strategy: Strategy, filter: Filter)

  /**
   * Choose the query strategy to be employed here. This is the priority
   *   * If an ID predicate is present, it is assumed that only a small number of IDs are requested
   *            --> The Record Index is scanned, and the other ECQL filters, if any, are then applied
   *
   *   * If attribute filters and ST filters are present, use the cardinality + ordering to choose
   *     the correct strategy
   *
   *   * If attribute filters are present, then select the correct type of AttributeIdx Strategy
   *            --> The Attribute Indices are scanned, and the other ECQL filters, if any, are then applied
   *
   *   * If ST filters are present, use the STIdxStrategy
   *            --> The ST Index is scanned, and the other ECQL filters, if any are then applied
   *
   *   * If filters are not identified, use the STIdxStrategy
   *            --> The ST Index is scanned (likely a full table scan) and the ECQL filters are applied
   */
  private def processAnd(and: And, sft: SimpleFeatureType, hints: StrategyHints): Strategy = {

    val filters: util.List[Filter] = decomposeAnd(and)

    // scan the query and identify the type of predicates present

    // record strategy takes priority
    val recordStrategies =
      filters.toStream.flatMap(f => getRecordIdxStrategy(f, sft).map(StrategyAndFilter(_, f)))
    if (!recordStrategies.isEmpty) {
      return recordStrategies(0).strategy
    }

    val attributeStrategies =
      filters.flatMap(f => getAttributeIndexStrategy(f, sft).map(StrategyAndFilter(_, f)))
    // if no attribute or record strategies, use ST
    if (attributeStrategies.isEmpty) {
      return new STIdxStrategy
    }

    // next look for high-cardinality attribute filters
    val highCardinalityStrategy = attributeStrategies.find { case StrategyAndFilter(strategy, filter) =>
      val (prop, _) = AttributeIndexStrategy.getPropertyAndRange(filter, sft)
      hints.cardinality(sft.getDescriptor(prop)) == Cardinality.HIGH
    }
    if (highCardinalityStrategy.isDefined) {
      return highCardinalityStrategy.get.strategy
    }

    // finally, compare spatial and attribute filters based on order
    val stStrategy = filters.flatMap(f => getSTIdxStrategy(f, sft).map(StrategyAndFilter(_, f))).headOption
    val attrStrategy = attributeStrategies.find { case StrategyAndFilter(strategy, filter) =>
      val (prop, _) = AttributeIndexStrategy.getPropertyAndRange(filter, sft)
      hints.cardinality(sft.getDescriptor(prop)) != Cardinality.LOW
    }

    (stStrategy, attrStrategy) match {
      case (None, None)           => new STIdxStrategy
      case (Some(st), None)       => st.strategy
      case (None, Some(attr))     => attr.strategy
      case (Some(st), Some(attr)) =>
        if (filters.indexOf(st.filter) < filters.indexOf(attr.filter)) {
          st.strategy
        } else {
          attr.strategy
        }
    }
  }

  // TODO try to use wildcard values from the Filter itself (https://geomesa.atlassian.net/browse/GEOMESA-309)
  // Currently pulling the wildcard values from the filter
  // leads to inconsistent results...so use % as wildcard
  val MULTICHAR_WILDCARD = "%"
  val SINGLE_CHAR_WILDCARD = "_"
  val NULLBYTE = Array[Byte](0.toByte)

  /* Like queries that can be handled by current reverse index */
  def likeEligible(filter: PropertyIsLike) = containsNoSingles(filter) && trailingOnlyWildcard(filter)

  /* contains no single character wildcards */
  def containsNoSingles(filter: PropertyIsLike) =
    !filter.getLiteral.replace("\\\\", "").replace(s"\\$SINGLE_CHAR_WILDCARD", "").contains(SINGLE_CHAR_WILDCARD)

  def trailingOnlyWildcard(filter: PropertyIsLike) =
    (filter.getLiteral.endsWith(MULTICHAR_WILDCARD) &&
      filter.getLiteral.indexOf(MULTICHAR_WILDCARD) == filter.getLiteral.length - MULTICHAR_WILDCARD.length) ||
      filter.getLiteral.indexOf(MULTICHAR_WILDCARD) == -1

}
