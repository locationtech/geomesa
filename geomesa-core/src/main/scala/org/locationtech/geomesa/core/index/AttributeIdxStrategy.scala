/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

import java.nio.charset.StandardCharsets
import java.util.Map.Entry

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.{IteratorSetting, Scanner}
import org.apache.accumulo.core.data.{Key, Value, Range => AccRange}
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.core.data.{FilterToAccumulo, AccumuloConnectorCreator}
import org.locationtech.geomesa.core.DEFAULT_FILTER_PROPERTY_NAME
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.QueryPlanner._
import org.locationtech.geomesa.core.iterators.AttributeIndexFilteringIterator
import org.locationtech.geomesa.core.util.{BatchMultiScanner, SelfClosingIterator}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.{PropertyIsLike, PropertyIsEqualTo, Filter}

import scala.collection.JavaConversions._

trait AttributeIdxStrategy extends Strategy with Logging {

  /**
   * Perform scan against the Attribute Index Table and get an iterator returning records from the Record table
   */
  def attrIdxQuery(acc: AccumuloConnectorCreator,
                   query: Query,
                   iqp: QueryPlanner,
                   featureType: SimpleFeatureType,
                   filterVisitor: FilterToAccumulo,
                   range: AccRange,
                   output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    output(s"Searching the attribute table with filter ${query.getFilter}")
    val schema         = iqp.schema
    val featureEncoder = iqp.featureEncoder

    output(s"Scanning attribute table for feature type ${featureType.getTypeName}")
    val attrScanner = acc.createAttrIdxScanner(featureType)

    val (geomFilters, otherFilters) = partitionGeom(query.getFilter)
    val (temporalFilters, nonSTFilters) = partitionTemporal(otherFilters)

    // NB: Added check to see if the nonSTFilters is empty.
    //  If it is, we needn't configure the SFFI

    output(s"The geom filters are $geomFilters.\nThe temporal filters are $temporalFilters.")
    val ofilter: Option[Filter] = filterListAsAnd(geomFilters ++ temporalFilters)

    configureAttributeIndexIterator(attrScanner, featureType, ofilter, range)

    val recordScanner = acc.createRecordScanner(featureType)
    val iterSetting = configureSimpleFeatureFilteringIterator(featureType, None, schema, featureEncoder, query)
    recordScanner.addScanIterator(iterSetting)

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

  def formatAttrIdxRow(prop: String, lit: String) =
    new Text(prop.getBytes(StandardCharsets.UTF_8) ++ QueryStrategyDecider.NULLBYTE ++ lit.getBytes(StandardCharsets.UTF_8))
}

class AttributeEqualsIdxStrategy extends AttributeIdxStrategy {

  override def execute(acc: AccumuloConnectorCreator,
                       iqp: QueryPlanner,
                       featureType: SimpleFeatureType,
                       query: Query,
                       filterVisitor: FilterToAccumulo,
                       output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val filter = query.getFilter.asInstanceOf[PropertyIsEqualTo]
    val one = filter.getExpression1
    val two = filter.getExpression2
    val (prop, lit) = (one, two) match {
      case (p: PropertyName, l: Literal) => (p.getPropertyName, l.getValue.toString)
      case (l: Literal, p: PropertyName) => (p.getPropertyName, l.getValue.toString)
      case _ =>
        val msg =
          s"""Unhandled equalTo Query (expr1 type: ${one.getClass.getName}, expr2 type: ${two.getClass.getName}
            |Supported types are literal = propertyName and propertyName = literal
          """.stripMargin
        throw new RuntimeException(msg)
    }

    val range = new AccRange(formatAttrIdxRow(prop, lit))

    attrIdxQuery(acc, query, iqp, featureType, filterVisitor, range, output)
  }
}

class AttributeLikeIdxStrategy extends AttributeIdxStrategy {

  override def execute(acc: AccumuloConnectorCreator,
                       iqp: QueryPlanner,
                       featureType: SimpleFeatureType,
                       query: Query,
                       filterVisitor: FilterToAccumulo,
                       output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val filter = query.getFilter.asInstanceOf[PropertyIsLike]
    val expr = filter.getExpression
    val prop = expr match {
      case p: PropertyName => p.getPropertyName
    }

    // Remove the trailing wilcard and create a range prefix
    val literal = filter.getLiteral
    val value =
      if(literal.endsWith(QueryStrategyDecider.MULTICHAR_WILDCARD))
        literal.substring(0, literal.length - QueryStrategyDecider.MULTICHAR_WILDCARD.length)
      else
        literal

    val range = AccRange.prefix(formatAttrIdxRow(prop, value))

    attrIdxQuery(acc, query, iqp, featureType, filterVisitor, range, output)
  }
}