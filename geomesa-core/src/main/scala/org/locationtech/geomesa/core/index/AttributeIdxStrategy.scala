/*
 * Copyright 2013-2014 Commonwealth Computer Research, Inc.
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


import java.util.Map.Entry

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.{IteratorSetting, Scanner}
import org.apache.accumulo.core.data.{Key, Value, Range => AccRange}
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.geotools.temporal.`object`.DefaultPeriod
import org.locationtech.geomesa.core.DEFAULT_FILTER_PROPERTY_NAME
import org.locationtech.geomesa.core.data.AccumuloConnectorCreator
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.FilterHelper._
import org.locationtech.geomesa.core.index.QueryPlanner._
import org.locationtech.geomesa.core.iterators.AttributeIndexFilteringIterator
import org.locationtech.geomesa.core.util.{BatchMultiScanner, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.{Expression, Literal, PropertyName}
import org.opengis.filter.temporal.{After, Before, During, TEquals}

import scala.collection.JavaConversions._

trait AttributeIdxStrategy extends Strategy with Logging {

  /**
   * Perform scan against the Attribute Index Table and get an iterator returning records from the Record table
   */
  def attrIdxQuery(acc: AccumuloConnectorCreator,
                   query: Query,
                   iqp: QueryPlanner,
                   featureType: SimpleFeatureType,
                   range: AccRange,
                   output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    output(s"Searching the attribute table with filter ${query.getFilter}")
    val schema         = iqp.schema
    val featureEncoder = iqp.featureEncoder

    output(s"Scanning attribute table for feature type ${featureType.getTypeName}")
    val attrScanner = acc.createAttrIdxScanner(featureType)

    val (geomFilters, otherFilters) = partitionGeom(query.getFilter)
    val (temporalFilters, nonSTFilters) = partitionTemporal(otherFilters, getDtgFieldName(featureType))

    // NB: Added check to see if the nonSTFilters is empty.
    //  If it is, we needn't configure the SFFI

    output(s"The geom filters are $geomFilters.\nThe temporal filters are $temporalFilters.")
    val ofilter: Option[Filter] = filterListAsAnd(geomFilters ++ temporalFilters)

    configureAttributeIndexIterator(attrScanner, featureType, ofilter, range)

    val recordScanner = acc.createRecordScanner(featureType)

    if (nonSTFilters.nonEmpty) {
      val iterSetting =
        configureSimpleFeatureFilteringIterator(featureType,
                                                Some(filterListAsAnd(nonSTFilters).get.toString),
                                                schema,
                                                featureEncoder,
                                                query)
      recordScanner.addScanIterator(iterSetting)
    }

    // function to join the attribute index scan results to the record table
    // since the row id of the record table is in the CF just grab that
    val joinFunction = (kv: java.util.Map.Entry[Key, Value]) => new AccRange(kv.getKey.getColumnFamily)
    val bms = new BatchMultiScanner(attrScanner, recordScanner, joinFunction)

    SelfClosingIterator(bms.iterator, () => bms.close())
  }

  def configureAttributeIndexIterator(scanner: Scanner,
                                      featureType: SimpleFeatureType,
                                      ofilter: Option[Filter],
                                      range: AccRange) {
    val opts = ofilter.map { f => DEFAULT_FILTER_PROPERTY_NAME -> ECQL.toCQL(f)}.toMap

    if(opts.nonEmpty) {
      val cfg = new IteratorSetting(iteratorPriority_AttributeIndexFilteringIterator,
        "attrIndexFilter",
        classOf[AttributeIndexFilteringIterator].getCanonicalName,
        opts)

      configureFeatureType(cfg, featureType)
      scanner.addScanIterator(cfg)
    }

    logger.trace(s"Attribute Scan Range: ${range.toString}")
    scanner.setRange(range)
  }

  /**
   * Gets a row key that can used as a range for an attribute query.
   * The attribute index encodes the type of the attribute as part of the row. This checks for
   * query literals that don't match the expected type and tries to convert them.
   *
   * @param sft
   * @param prop
   * @param value
   * @return
   */
  def getEncodedAttrIdxRow(sft: SimpleFeatureType, prop: String, value: Any): String = {
    // the class type as defined in the SFT
    val expectedBinding = sft.getDescriptor(prop).getType.getBinding
    // the class type of the literal pulled from the query
    val actualBinding = value.getClass
    val typedValue =
      if (expectedBinding.equals(actualBinding)) {
        value
      } else {
        // type mismatch, encoding won't work b/c class is stored as part of the row
        // try to convert to the appropriate class
        AttributeIndexEntry.convertType(value, actualBinding, expectedBinding)
      }
    AttributeIndexEntry.getAttributeIndexRow(prop, Some(typedValue))
  }

  /**
   * Checks the order of properties and literals in the expression
   *
   * @param one
   * @param two
   * @return (prop, literal, whether the order was flipped)
   */
  def checkOrder(one: Expression, two: Expression): (String, AnyRef, Boolean) =
    (one, two) match {
      case (p: PropertyName, l: Literal) => (p.getPropertyName, l.getValue, false)
      case (l: Literal, p: PropertyName) => (p.getPropertyName, l.getValue, true)
      case _ =>
        val msg = "Unhandled properties in attribute index strategy: " +
                    s"${one.getClass.getName}, ${two.getClass.getName}"
        throw new RuntimeException(msg)
    }

  def partitionFilter(filter: Filter, sft: SimpleFeatureType): (Query, Filter) = {

    val (indexFilter, cqlFilter) = filter match {
      case and: And =>
        findFirst(AttributeIndexStrategy.getAttributeIndexStrategy(_, sft).isDefined)(and.getChildren)
      case f: Filter =>
        (Some(f), Seq())
    }

    (new Query(sft.getTypeName, filterListAsAnd(cqlFilter).getOrElse(Filter.INCLUDE)), indexFilter.get)
  }
}

class AttributeIdxEqualsStrategy extends AttributeIdxStrategy {

  override def execute(acc: AccumuloConnectorCreator,
                       iqp: QueryPlanner,
                       featureType: SimpleFeatureType,
                       query: Query,
                       output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val (strippedQuery, filter) = partitionFilter(query.getFilter, featureType)
    val range =
      filter match {
        case f: PropertyIsEqualTo =>
          val (prop, lit, _) = checkOrder(f.getExpression1, f.getExpression2)
          AccRange.exact(getEncodedAttrIdxRow(featureType, prop, lit))

        case f: TEquals =>
          val (prop, lit, _) = checkOrder(f.getExpression1, f.getExpression2)
          AccRange.exact(getEncodedAttrIdxRow(featureType, prop, lit))

        case f: PropertyIsNil =>
          val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
          AccRange.exact(AttributeIndexEntry.getAttributeIndexRow(prop, None))

        case f: PropertyIsNull =>
          val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
          AccRange.exact(AttributeIndexEntry.getAttributeIndexRow(prop, None))

        case _ =>
          val msg = s"Unhandled filter type in equals strategy: ${filter.getClass.getName}"
          throw new RuntimeException(msg)
      }

    attrIdxQuery(acc, strippedQuery, iqp, featureType, range, output)
  }
}

class AttributeIdxRangeStrategy extends AttributeIdxStrategy {

  override def execute(acc: AccumuloConnectorCreator,
                       iqp: QueryPlanner,
                       featureType: SimpleFeatureType,
                       query: Query,
                       output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val (strippedQuery, filter) = partitionFilter(query.getFilter, featureType)
    val range =
      filter match {
        case f: PropertyIsBetween =>
          val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
          val lower = f.getLowerBoundary.asInstanceOf[Literal].getValue
          val upper = f.getUpperBoundary.asInstanceOf[Literal].getValue
          val lowerBound = getEncodedAttrIdxRow(featureType, prop, lower)
          val upperBound = getEncodedAttrIdxRow(featureType, prop, upper)
          new AccRange(lowerBound, true, upperBound, true)

        case f: PropertyIsGreaterThan =>
          val (prop, lit, flipped) = checkOrder(f.getExpression1, f.getExpression2)
          if (flipped) {
            lessThanRange(featureType, prop, lit)
          } else {
            greaterThanRange(featureType, prop, lit)
          }
        case f: PropertyIsGreaterThanOrEqualTo =>
          val (prop, lit, flipped) = checkOrder(f.getExpression1, f.getExpression2)
          if (flipped) {
            lessThanOrEqualRange(featureType, prop, lit)
          } else {
            greaterThanOrEqualRange(featureType, prop, lit)
          }
        case f: PropertyIsLessThan =>
          val (prop, lit, flipped) = checkOrder(f.getExpression1, f.getExpression2)
          if (flipped) {
            greaterThanRange(featureType, prop, lit)
          } else {
            lessThanRange(featureType, prop, lit)
          }
        case f: PropertyIsLessThanOrEqualTo =>
          val (prop, lit, flipped) = checkOrder(f.getExpression1, f.getExpression2)
          if (flipped) {
            greaterThanOrEqualRange(featureType, prop, lit)
          } else {
            lessThanOrEqualRange(featureType, prop, lit)
          }
        case f: Before =>
          val (prop, lit, flipped) = checkOrder(f.getExpression1, f.getExpression2)
          if (flipped) {
            greaterThanRange(featureType, prop, lit)
          } else {
            lessThanRange(featureType, prop, lit)
          }
        case f: After =>
          val (prop, lit, flipped) = checkOrder(f.getExpression1, f.getExpression2)
          if (flipped) {
            lessThanRange(featureType, prop, lit)
          } else {
            greaterThanRange(featureType, prop, lit)
          }
        case f: During =>
          val (prop, lit, _) = checkOrder(f.getExpression1, f.getExpression2)
          val during = lit.asInstanceOf[DefaultPeriod]
          val lower = during.getBeginning.getPosition.getDate
          val upper = during.getEnding.getPosition.getDate
          val lowerBound = getEncodedAttrIdxRow(featureType, prop, lower)
          val upperBound = getEncodedAttrIdxRow(featureType, prop, upper)
          new AccRange(lowerBound, true, upperBound, true)

        case _ =>
          val msg = s"Unhandled filter type in range strategy: ${filter.getClass.getName}"
          throw new RuntimeException(msg)
      }

    attrIdxQuery(acc, strippedQuery, iqp, featureType, range, output)
  }

  private def greaterThanRange(featureType: SimpleFeatureType, prop: String, lit: AnyRef): AccRange = {
    val start = new Text(getEncodedAttrIdxRow(featureType, prop, lit))
    val end = AccRange.followingPrefix(new Text(AttributeIndexEntry.getAttributeIndexRowPrefix(prop)))
    new AccRange(start, false, end, false)
  }

  private def greaterThanOrEqualRange(featureType: SimpleFeatureType, prop: String, lit: AnyRef): AccRange = {
    val start = new Text(getEncodedAttrIdxRow(featureType, prop, lit))
    val end = AccRange.followingPrefix(new Text(AttributeIndexEntry.getAttributeIndexRowPrefix(prop)))
    new AccRange(start, true, end, false)
  }

  private def lessThanRange(featureType: SimpleFeatureType, prop: String, lit: AnyRef): AccRange = {
    val start = AttributeIndexEntry.getAttributeIndexRowPrefix(prop)
    val end = getEncodedAttrIdxRow(featureType, prop, lit)
    new AccRange(start, false, end, false)
  }

  private def lessThanOrEqualRange(featureType: SimpleFeatureType, prop: String, lit: AnyRef): AccRange = {
    val start = AttributeIndexEntry.getAttributeIndexRowPrefix(prop)
    val end = getEncodedAttrIdxRow(featureType, prop, lit)
    new AccRange(start, false, end, true)
  }
}

class AttributeIdxLikeStrategy extends AttributeIdxStrategy {

  override def execute(acc: AccumuloConnectorCreator,
                       iqp: QueryPlanner,
                       featureType: SimpleFeatureType,
                       query: Query,
                       output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val (strippedQuery, extractedFilter) = partitionFilter(query.getFilter, featureType)
    val filter = extractedFilter.asInstanceOf[PropertyIsLike]
    val expr = filter.getExpression
    val prop = expr match {
      case p: PropertyName => p.getPropertyName
    }

    // Remove the trailing wildcard and create a range prefix
    val literal = filter.getLiteral
    val value =
      if(literal.endsWith(QueryStrategyDecider.MULTICHAR_WILDCARD))
        literal.substring(0, literal.length - QueryStrategyDecider.MULTICHAR_WILDCARD.length)
      else
        literal

    val range = AccRange.prefix(getEncodedAttrIdxRow(featureType, prop, value))

    attrIdxQuery(acc, strippedQuery, iqp, featureType, range, output)
  }
}

object AttributeIndexStrategy {

  import org.locationtech.geomesa.utils.geotools.Conversions._

  def getAttributeIndexStrategy(filter: Filter, sft: SimpleFeatureType): Option[Strategy] =
    filter match {
      // equals strategy checks
      case f: PropertyIsEqualTo =>
        val canQuery = isValidAttributeFilter(sft, f.getExpression1, f.getExpression2)
        if (canQuery) Some(new AttributeIdxEqualsStrategy) else None

      case f: TEquals =>
        val canQuery = isValidAttributeFilter(sft, f.getExpression1, f.getExpression2)
        if (canQuery) Some(new AttributeIdxEqualsStrategy) else None

      case f: PropertyIsNil =>
        val canQuery = isValidAttributeFilter(sft, f.getExpression)
        if (canQuery) Some(new AttributeIdxEqualsStrategy) else None

      case f: PropertyIsNull =>
        val canQuery = isValidAttributeFilter(sft, f.getExpression)
        if (canQuery) Some(new AttributeIdxEqualsStrategy) else None

      // like strategy checks
      case f: PropertyIsLike =>
        val canQuery = isValidAttributeFilter(sft, f.getExpression)
        if (canQuery) Some(new AttributeIdxLikeStrategy) else None

      // range strategy checks
      case f: PropertyIsBetween =>
        val canQuery = isValidAttributeFilter(sft, f.getExpression)
        if (canQuery) Some(new AttributeIdxRangeStrategy) else None

      case f: PropertyIsGreaterThan =>
        val canQuery = isValidAttributeFilter(sft, f.getExpression1, f.getExpression2)
        if (canQuery) Some(new AttributeIdxRangeStrategy) else None

      case f: PropertyIsGreaterThanOrEqualTo =>
        val canQuery = isValidAttributeFilter(sft, f.getExpression1, f.getExpression2)
        if (canQuery) Some(new AttributeIdxRangeStrategy) else None

      case f: PropertyIsLessThan =>
        val canQuery = isValidAttributeFilter(sft, f.getExpression1, f.getExpression2)
        if (canQuery) Some(new AttributeIdxRangeStrategy) else None

      case f: PropertyIsLessThanOrEqualTo =>
        val canQuery = isValidAttributeFilter(sft, f.getExpression1, f.getExpression2)
        if (canQuery) Some(new AttributeIdxRangeStrategy) else None

      case f: Before =>
        val canQuery = isValidAttributeFilter(sft, f.getExpression1, f.getExpression2)
        if (canQuery) Some(new AttributeIdxRangeStrategy) else None

      case f: After =>
        val canQuery = isValidAttributeFilter(sft, f.getExpression1, f.getExpression2)
        if (canQuery) Some(new AttributeIdxRangeStrategy) else None

      case f: During =>
        val canQuery = isValidAttributeFilter(sft, f.getExpression1, f.getExpression2)
        if (canQuery) Some(new AttributeIdxRangeStrategy) else None

      // doesn't match any property strategy
      case _ => None
    }

  /**
   * Ensures the following conditions:
   *   - there is exactly one 'property name' expression
   *   - the property is indexed by GeoMesa
   *   - all other expressions are literals
   *
   * @param sft
   * @param exp
   * @return
   */
  private def isValidAttributeFilter(sft: SimpleFeatureType, exp: Expression*): Boolean = {
    val (props, lits) = exp.partition(_.isInstanceOf[PropertyName])

    props.length == 1 &&
      props.map(_.asInstanceOf[PropertyName].getPropertyName).forall(sft.getDescriptor(_).isIndexed) &&
      lits.forall(_.isInstanceOf[Literal])
  }

}
