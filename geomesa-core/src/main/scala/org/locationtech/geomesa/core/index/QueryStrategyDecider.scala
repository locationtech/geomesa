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
import org.locationtech.geomesa.core.index.FilterHelper._
import org.locationtech.geomesa.core.index.QueryHints._
import org.locationtech.geomesa.utils.stats.Cardinality
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.{And, Filter, Id, PropertyIsLike}

import scala.collection.JavaConversions._

object QueryStrategyDecider {

  val REASONABLE_COST = 10000
  val OPTIMAL_COST = 10

  def chooseStrategy(sft: SimpleFeatureType, query: Query, hints: StrategyHints, version: Int): Strategy = {
    // check for density queries
    if (query.getHints.containsKey(BBOX_KEY) || query.getHints.contains(TIME_BUCKETS_KEY)) {
      // TODO GEOMESA-322 use other strategies with density iterator
      return new STIdxStrategy
    }

    query.getFilter match {
      case id: Id   => new RecordIdxStrategy
      case and: And => processAnd(and, sft, hints)
      case cql =>
        // a single clause - check for indexed attributes or fall back to spatio-temporal
        AttributeIndexStrategy.getStrategy(cql, sft, hints).map(_.strategy).getOrElse(new STIdxStrategy)
    }
  }

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

    import org.locationtech.geomesa.utils.geotools.RichIterator.RichIterator

    val filters = decomposeAnd(and)

    // scan the query and identify the type of predicates present

    // record strategy takes priority
    val recordStrategy = filters.iterator.flatMap(f => RecordIdxStrategy.getStrategy(f, sft, hints)).headOption
    if (recordStrategy.isDefined) {
      return recordStrategy.get.strategy
    }

    // look for reasonable cost attribute strategies
    val attributeStrategies =
      filters.flatMap(f => AttributeIndexStrategy.getStrategy(f, sft, hints)).filter(_.cost < REASONABLE_COST)
    // if no attribute or record strategies, use ST
    if (attributeStrategies.isEmpty) {
      return new STIdxStrategy
    }

    // next look for low cost (high-cardinality) attribute filters - cost is set in the attribute strategy
    val highCardinalityStrategy = attributeStrategies.find(_.cost < OPTIMAL_COST)
    if (highCardinalityStrategy.isDefined) {
      return highCardinalityStrategy.get.strategy
    }

    // finally, prefer spatial filters if available
    val stStrategy = filters.iterator.flatMap(f => STIdxStrategy.getStrategy(f, sft, hints)).headOption
    stStrategy.orElse(attributeStrategies.headOption).map(_.strategy).getOrElse(new STIdxStrategy)
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
