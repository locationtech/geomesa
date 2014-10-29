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
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.{And, Filter, Id, PropertyIsLike}

import scala.collection.JavaConversions._

object QueryStrategyDecider {

  def chooseStrategy(isCatalogTableFormat: Boolean,
                     sft: SimpleFeatureType,
                     query: Query): Strategy =
    // if datastore doesn't support attr index use spatiotemporal only
    if (isCatalogTableFormat) chooseNewStrategy(sft, query) else new STIdxStrategy

  def chooseNewStrategy(sft: SimpleFeatureType, query: Query): Strategy = {
    val filter = query.getFilter
    val isDensity = query.getHints.containsKey(BBOX_KEY)

    if (isDensity) {
      // TODO GEOMESA-322 use other strategies with density iterator
      new STIdxStrategy
    } else {
      // check if we can use the attribute index first
      val attributeStrategy = getAttributeIndexStrategy(filter, sft)
      attributeStrategy.getOrElse {
        filter match {
          case idFilter: Id => new RecordIdxStrategy
          case and: And => processAnd(isDensity, sft, and)
          case cql          => new STIdxStrategy
        }
      }
    }
  }

  private def processAnd(isDensity: Boolean, sft: SimpleFeatureType, and: And): Strategy = {
    val children: util.List[Filter] = decomposeAnd(and)

    def determineStrategy(attr: Filter, st: Filter): Strategy = {
      if (children.indexOf(attr) < children.indexOf(st)) { getAttributeIndexStrategy(attr, sft).get }
      else { new STIdxStrategy }
    }

    // first scan the query and identify the type of predicates present
    val strats = (children.find(c => getAttributeIndexStrategy(c, sft).isDefined),
                  children.find(c => getSTIdxStrategy(c, sft).isDefined),
                  children.find(c => getRecordIdxStrategy(c, sft).isDefined))

    /**
     * Choose the query strategy to be employed here. This is the priority
     *   * If an ID predicate is present, it is assumed that only a small number of IDs are requested
     *            --> The Record Index is scanned, and the other ECQL filters, if any, are then applied
     *
     *   * If attribute filters and ST filters are present, use the ordering to choose the correct strategy
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
    strats match {
      case (               _,              _, Some(idFilter))  => new RecordIdxStrategy
      case (Some(attrFilter), Some(stFilter),           None)  => determineStrategy(attrFilter, stFilter)
      case (Some(attrFilter),           None,           None)  => getAttributeIndexStrategy(attrFilter, sft).get
      case (            None, Some(stFilter),           None)  => new STIdxStrategy
      case (            None,           None,           None)  => new STIdxStrategy
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
