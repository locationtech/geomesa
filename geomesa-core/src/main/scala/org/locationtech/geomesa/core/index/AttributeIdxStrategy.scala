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

import java.util.Date
import java.util.Map.Entry

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Range => AccRange, Value}
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.geotools.temporal.`object`.DefaultPeriod
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.core.data.tables.{AttributeTable, RecordTable}
import org.locationtech.geomesa.core.filter._
import org.locationtech.geomesa.core.index.FilterHelper._
import org.locationtech.geomesa.core.index.QueryPlanner._
import org.locationtech.geomesa.core.iterators._
import org.locationtech.geomesa.core.util.{BatchMultiScanner, SelfClosingIterator}
import org.locationtech.geomesa.feature.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.feature.SimpleFeatureDecoder
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.stats.IndexCoverage.IndexCoverage
import org.locationtech.geomesa.utils.stats.{Cardinality, IndexCoverage}
import org.opengis.feature.`type`.AttributeDescriptor
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.temporal.{After, Before, During, TEquals}
import org.opengis.filter.{Filter, PropertyIsEqualTo, PropertyIsLike, _}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

trait AttributeIdxStrategy extends Strategy with Logging {

  import org.locationtech.geomesa.core.index.AttributeIndexStrategy._

  /**
   * Perform scan against the Attribute Index Table and get an iterator returning records from the Record table
   */
  def attrIdxQuery(
      acc: AccumuloConnectorCreator,
      query: Query,
      iqp: QueryPlanner,
      featureType: SimpleFeatureType,
      attributeName: String,
      range: AccRange,
      output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {

    output(s"Scanning attribute table for feature type ${featureType.getTypeName}")
    output(s"Range: ${ExplainerOutputType.toString(range)}")
    output(s"Filter: ${query.getFilter}")

    val attrScanner = acc.createAttrIdxScanner(featureType)
    attrScanner.setRange(range)

    val (geomFilters, otherFilters) = partitionGeom(query.getFilter, featureType)
    val (temporalFilters, nonSTFilters) = partitionTemporal(otherFilters, getDtgFieldName(featureType))

    output(s"Geometry filters: $geomFilters")
    output(s"Temporal filters: $temporalFilters")
    output(s"Other filters: $nonSTFilters")

    val stFilter: Option[Filter] = filterListAsAnd(geomFilters ++ temporalFilters)
    val ecqlFilter: Option[Filter] = filterListAsAnd(nonSTFilters)

    val encoding = iqp.featureEncoding
    val version = acc.getGeomesaVersion(featureType)

    // choose which iterator we want to use - joining iterator or attribute only iterator
    val iteratorChoice: IteratorConfig =
      IteratorTrigger.chooseAttributeIterator(ecqlFilter, query, featureType, attributeName)

    val iter = iteratorChoice.iterator match {
      case IndexOnlyIterator =>
        // the attribute index iterator also handles transforms and date/geom filters
        val cfg = configureAttributeIndexIterator(featureType, encoding, query, stFilter, ecqlFilter,
          iteratorChoice.transformCoversFilter, attributeName, version)
        attrScanner.addScanIterator(cfg)
        output(s"AttributeIndexIterator: ${cfg.toString }")

        // if this is a request for unique attribute values, add the skipping iterator to speed up response
        if (query.getHints.containsKey(GEOMESA_UNIQUE)) {
          val uCfg = configureUniqueAttributeIterator()
          attrScanner.addScanIterator(uCfg)
          output(s"UniqueAttributeIterator: ${uCfg.toString }")
        }

        // there won't be any non-date/time-filters if the index only iterator has been selected
        SelfClosingIterator(attrScanner)

      case RecordJoinIterator =>
        output("Using record join iterator")
        stFilter.foreach { filter =>
          // apply a filter for the indexed date and geometry
          val cfg = configureSpatioTemporalFilter(featureType, encoding, stFilter, version)
          attrScanner.addScanIterator(cfg)
          output(s"SpatioTemporalFilter: ${cfg.toString }")
        }

        val recordScanner = acc.createRecordScanner(featureType)

        if (iteratorChoice.hasTransformOrFilter) {
          // apply an iterator for any remaining transforms/filters
          val cfg = configureRecordTableIterator(featureType, encoding, ecqlFilter, query)
          recordScanner.addScanIterator(cfg)
          output(s"RecordTableIterator: ${cfg.toString }")
        }

        // function to join the attribute index scan results to the record table
        // since the row id of the record table is in the CF just grab that
        val prefix = getTableSharingPrefix(featureType)
        val joinFunction = (kv: java.util.Map.Entry[Key, Value]) =>
          new AccRange(RecordTable.getRowKey(prefix, kv.getKey.getColumnQualifier.toString))
        val bms = new BatchMultiScanner(attrScanner, recordScanner, joinFunction)

        SelfClosingIterator(bms.iterator, () => bms.close())
    }

    // wrap with a de-duplicator if the attribute could have multiple values, and it won't be
    // de-duped by the query planner
    if (!IndexSchema.mayContainDuplicates(featureType) &&
        featureType.getDescriptor(attributeName).isMultiValued) {
      val returnSft = Option(query.getHints.get(TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType])
          .getOrElse(featureType)
      val decoder = SimpleFeatureDecoder(returnSft, iqp.featureEncoding)
      val deduper = new DeDuplicatingIterator(iter, (_: Key, value: Value) => decoder.extractFeatureId(value.get))
      SelfClosingIterator(deduper)
    } else {
      iter
    }
  }

  private def configureAttributeIndexIterator(
      featureType: SimpleFeatureType,
      encoding: FeatureEncoding,
      query: Query,
      stFilter: Option[Filter],
      ecqlFilter: Option[Filter],
      needsTransform: Boolean,
      attributeName: String,
      version: Int) = {

    // the attribute index iterator also checks any ST filters
    val cfg = new IteratorSetting(
      iteratorPriority_AttributeIndexIterator,
      classOf[AttributeIndexIterator].getSimpleName,
      classOf[AttributeIndexIterator]
    )

    val coverage = featureType.getDescriptor(attributeName).getIndexCoverage()

    configureFeatureTypeName(cfg, featureType.getTypeName)
    configureFeatureEncoding(cfg, encoding)
    configureIndexValues(cfg, featureType)
    configureAttributeName(cfg, attributeName)
    configureIndexCoverage(cfg, coverage)
    configureVersion(cfg, version)
    if (coverage == IndexCoverage.FULL) {
      // combine filters into one check
      configureEcqlFilter(cfg, filterListAsAnd(Seq(stFilter ++ ecqlFilter).flatten).map(ECQL.toCQL))
    } else {
      configureStFilter(cfg, stFilter)
      configureEcqlFilter(cfg, ecqlFilter.map(ECQL.toCQL))
    }
    if (needsTransform) {
      // we have to evaluate the filter against full feature then apply the transform
      configureFeatureType(cfg, featureType)
      configureTransforms(cfg, query)
    } else {
      // we can evaluate the filter against the transformed schema, so skip the original feature decoding
      val transformedType = query.getHints.get(TRANSFORM_SCHEMA).asInstanceOf[SimpleFeatureType]
      configureFeatureType(cfg, transformedType)
    }

    cfg
  }

  private def configureSpatioTemporalFilter(
      featureType: SimpleFeatureType,
      encoding: FeatureEncoding,
      stFilter: Option[Filter],
      version: Int) = {

    // a filter applied to the attribute table to check ST filters
    val cfg = new IteratorSetting(
      iteratorPriority_AttributeIndexFilteringIterator,
      classOf[IndexedSpatioTemporalFilter].getSimpleName,
      classOf[IndexedSpatioTemporalFilter]
    )

    configureFeatureType(cfg, featureType)
    configureFeatureTypeName(cfg, featureType.getTypeName)
    configureIndexValues(cfg, featureType)
    configureFeatureEncoding(cfg, encoding)
    configureStFilter(cfg, stFilter)
    configureVersion(cfg, version)

    cfg
  }

  private def configureUniqueAttributeIterator() =
    // needs to be applied *after* the AttributeIndexIterator
    new IteratorSetting(
      iteratorPriority_AttributeUniqueIterator,
      classOf[UniqueAttributeIterator].getSimpleName,
      classOf[UniqueAttributeIterator]
    )
}

class AttributeIdxEqualsStrategy extends AttributeIdxStrategy {

  import org.locationtech.geomesa.core.index.AttributeIndexStrategy._

  override def execute(acc: AccumuloConnectorCreator,
                       iqp: QueryPlanner,
                       sft: SimpleFeatureType,
                       query: Query,
                       output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val (strippedQuery, filter) = partitionFilter(query, sft)
    val (prop, range) = getPropertyAndRange(filter, sft)
    attrIdxQuery(acc, strippedQuery, iqp, sft, prop, range, output)
  }
}

class AttributeIdxRangeStrategy extends AttributeIdxStrategy {

  import org.locationtech.geomesa.core.index.AttributeIndexStrategy._

  override def execute(acc: AccumuloConnectorCreator,
                       iqp: QueryPlanner,
                       featureType: SimpleFeatureType,
                       query: Query,
                       output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val (strippedQuery, filter) = partitionFilter(query, featureType)
    val (prop, range) = getPropertyAndRange(filter, featureType)
    attrIdxQuery(acc, strippedQuery, iqp, featureType, prop, range, output)
  }
}

class AttributeIdxLikeStrategy extends AttributeIdxStrategy {

  import org.locationtech.geomesa.core.index.AttributeIndexStrategy._

  override def execute(acc: AccumuloConnectorCreator,
                       iqp: QueryPlanner,
                       featureType: SimpleFeatureType,
                       query: Query,
                       output: ExplainerOutputType): SelfClosingIterator[Entry[Key, Value]] = {
    val (strippedQuery, extractedFilter) = partitionFilter(query, featureType)
    val (prop, range) = getPropertyAndRange(extractedFilter, featureType)
    attrIdxQuery(acc, strippedQuery, iqp, featureType, prop, range, output)
  }
}

object AttributeIndexStrategy extends StrategyProvider {

  def cost(ad: AttributeDescriptor) = ad.getCardinality() match {
    case Cardinality.HIGH => 1
    case Cardinality.UNKNOWN => 999
    case Cardinality.LOW => Int.MaxValue
  }

  override def getStrategy(filter: Filter, sft: SimpleFeatureType, hints: StrategyHints) = {
    val indexed: (PropertyLiteral) => Boolean = (p: PropertyLiteral) => sft.getDescriptor(p.name).isIndexed
    val costp: (PropertyLiteral) => Int = (p: PropertyLiteral) => cost(sft.getDescriptor(p.name))
    filter match {
      // equals strategy checks
      case f: PropertyIsEqualTo =>
        checkOrder(f.getExpression1, f.getExpression2).filter(indexed)
            .map(p => StrategyDecision(new AttributeIdxEqualsStrategy, costp(p)))
      case f: TEquals =>
        checkOrder(f.getExpression1, f.getExpression2).filter(indexed)
            .map(p => StrategyDecision(new AttributeIdxEqualsStrategy, costp(p)))

      // like strategy checks
      case f: PropertyIsLike =>
        val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
        val descriptor = sft.getDescriptor(prop)
        if (descriptor.isIndexed && QueryStrategyDecider.likeEligible(f)) {
          Some(StrategyDecision(new AttributeIdxLikeStrategy, cost(descriptor)))
        } else {
          None
        }

      // range strategy checks
      case f: PropertyIsBetween =>
        val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
        val descriptor = sft.getDescriptor(prop)
        if (descriptor.isIndexed) {
          Some(StrategyDecision(new AttributeIdxRangeStrategy, cost(descriptor)))
        } else {
          None
        }
      case f: PropertyIsGreaterThan =>
        checkOrder(f.getExpression1, f.getExpression2).filter(indexed)
            .map(p => StrategyDecision(new AttributeIdxRangeStrategy, costp(p)))
      case f: PropertyIsGreaterThanOrEqualTo =>
        checkOrder(f.getExpression1, f.getExpression2).filter(indexed)
            .map(p => StrategyDecision(new AttributeIdxRangeStrategy, costp(p)))
      case f: PropertyIsLessThan =>
        checkOrder(f.getExpression1, f.getExpression2).filter(indexed)
            .map(p => StrategyDecision(new AttributeIdxRangeStrategy, costp(p)))
      case f: PropertyIsLessThanOrEqualTo =>
        checkOrder(f.getExpression1, f.getExpression2).filter(indexed)
            .map(p => StrategyDecision(new AttributeIdxRangeStrategy, costp(p)))
      case f: Before =>
        checkOrder(f.getExpression1, f.getExpression2).filter(indexed)
            .map(p => StrategyDecision(new AttributeIdxRangeStrategy, costp(p)))
      case f: After =>
        checkOrder(f.getExpression1, f.getExpression2).filter(indexed)
            .map(p => StrategyDecision(new AttributeIdxRangeStrategy, costp(p)))
      case f: During =>
        checkOrder(f.getExpression1, f.getExpression2).filter(indexed)
            .map(p => StrategyDecision(new AttributeIdxRangeStrategy, costp(p)))

      // doesn't match any attribute strategy
      case _ => None
    }
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
    val descriptor = sft.getDescriptor(prop)
    // the class type as defined in the SFT
    val expectedBinding = descriptor.getType.getBinding
    // the class type of the literal pulled from the query
    val actualBinding = value.getClass
    val typedValue =
      if (expectedBinding == actualBinding) {
        value
      } else if (descriptor.isCollection) {
        // we need to encode with the collection type
        descriptor.getCollectionType() match {
          case Some(collectionType) if collectionType == actualBinding => Seq(value).asJava
          case Some(collectionType) if collectionType != actualBinding =>
            Seq(AttributeTable.convertType(value, actualBinding, collectionType)).asJava
        }
      } else if (descriptor.isMap) {
        // TODO GEOMESA-454 - support querying against map attributes
        Map.empty.asJava
      } else {
        // type mismatch, encoding won't work b/c value is wrong class
        // try to convert to the appropriate class
        AttributeTable.convertType(value, actualBinding, expectedBinding)
      }

    val rowIdPrefix = org.locationtech.geomesa.core.index.getTableSharingPrefix(sft)
    // grab the first encoded row - right now there will only ever be a single item in the seq
    // eventually we may support searching a whole collection at once
    val rowWithValue = AttributeTable.getAttributeIndexRows(rowIdPrefix, descriptor, typedValue).headOption
    // if value is null there won't be any rows returned, instead just use the row prefix
    rowWithValue.getOrElse(AttributeTable.getAttributeIndexRowPrefix(rowIdPrefix, descriptor))
  }

  /**
   * Gets the property name from the filter and a range that covers the filter in the attribute table.
   * Note that if the filter is not a valid attribute filter this method will throw an exception.
   *
   * @param filter
   * @param sft
   * @return
   */
  def getPropertyAndRange(filter: Filter, sft: SimpleFeatureType): (String, AccRange) =
    filter match {
      case f: PropertyIsBetween =>
        val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
        val lower = f.getLowerBoundary.asInstanceOf[Literal].getValue
        val upper = f.getUpperBoundary.asInstanceOf[Literal].getValue
        val lowerBound = getEncodedAttrIdxRow(sft, prop, lower)
        val upperBound = getEncodedAttrIdxRow(sft, prop, upper)
        (prop, new AccRange(lowerBound, true, upperBound, true))

      case f: PropertyIsGreaterThan =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        if (prop.flipped) {
          (prop.name, lessThanRange(sft, prop.name, prop.literal.getValue))
        } else {
          (prop.name, greaterThanRange(sft, prop.name, prop.literal.getValue))
        }
      case f: PropertyIsGreaterThanOrEqualTo =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        if (prop.flipped) {
          (prop.name, lessThanOrEqualRange(sft, prop.name, prop.literal.getValue))
        } else {
          (prop.name, greaterThanOrEqualRange(sft, prop.name, prop.literal.getValue))
        }
      case f: PropertyIsLessThan =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        if (prop.flipped) {
          (prop.name, greaterThanRange(sft, prop.name, prop.literal.getValue))
        } else {
          (prop.name, lessThanRange(sft, prop.name, prop.literal.getValue))
        }
      case f: PropertyIsLessThanOrEqualTo =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        if (prop.flipped) {
          (prop.name, greaterThanOrEqualRange(sft, prop.name, prop.literal.getValue))
        } else {
          (prop.name, lessThanOrEqualRange(sft, prop.name, prop.literal.getValue))
        }
      case f: Before =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val lit = prop.literal.evaluate(null, classOf[Date])
        if (prop.flipped) {
          (prop.name, greaterThanRange(sft, prop.name, lit))
        } else {
          (prop.name, lessThanRange(sft, prop.name, lit))
        }
      case f: After =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val lit = prop.literal.evaluate(null, classOf[Date])
        if (prop.flipped) {
          (prop.name, lessThanRange(sft, prop.name, lit))
        } else {
          (prop.name, greaterThanRange(sft, prop.name, lit))
        }
      case f: During =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val during = prop.literal.getValue.asInstanceOf[DefaultPeriod]
        val lower = during.getBeginning.getPosition.getDate
        val upper = during.getEnding.getPosition.getDate
        val lowerBound = getEncodedAttrIdxRow(sft, prop.name, lower)
        val upperBound = getEncodedAttrIdxRow(sft, prop.name, upper)
        (prop.name, new AccRange(lowerBound, true, upperBound, true))

      case f: PropertyIsEqualTo =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        (prop.name, AccRange.exact(getEncodedAttrIdxRow(sft, prop.name, prop.literal.getValue)))

      case f: TEquals =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        (prop.name, AccRange.exact(getEncodedAttrIdxRow(sft, prop.name, prop.literal.getValue)))

      case f: PropertyIsLike =>
        val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
        // Remove the trailing wildcard and create a range prefix
        val literal = f.getLiteral
        val value = if (literal.endsWith(QueryStrategyDecider.MULTICHAR_WILDCARD)) {
          literal.substring(0, literal.length - QueryStrategyDecider.MULTICHAR_WILDCARD.length)
        } else {
          literal
        }
        (prop, AccRange.prefix(getEncodedAttrIdxRow(sft, prop, value)))

      case _ =>
        val msg = s"Unhandled filter type in attribute strategy: ${filter.getClass.getName}"
        throw new RuntimeException(msg)
    }

  private def greaterThanRange(sft: SimpleFeatureType, prop: String, lit: AnyRef): AccRange = {
    val rowIdPrefix = getTableSharingPrefix(sft)
    val start = new Text(getEncodedAttrIdxRow(sft, prop, lit))
    val endPrefix = AttributeTable.getAttributeIndexRowPrefix(rowIdPrefix, sft.getDescriptor(prop))
    val end = AccRange.followingPrefix(new Text(endPrefix))
    new AccRange(start, false, end, false)
  }

  private def greaterThanOrEqualRange(sft: SimpleFeatureType, prop: String, lit: AnyRef): AccRange = {
    val rowIdPrefix = getTableSharingPrefix(sft)
    val start = new Text(getEncodedAttrIdxRow(sft, prop, lit))
    val endPrefix = AttributeTable.getAttributeIndexRowPrefix(rowIdPrefix, sft.getDescriptor(prop))
    val end = AccRange.followingPrefix(new Text(endPrefix))
    new AccRange(start, true, end, false)
  }

  private def lessThanRange(sft: SimpleFeatureType, prop: String, lit: AnyRef): AccRange = {
    val rowIdPrefix = getTableSharingPrefix(sft)
    val start = AttributeTable.getAttributeIndexRowPrefix(rowIdPrefix, sft.getDescriptor(prop))
    val end = getEncodedAttrIdxRow(sft, prop, lit)
    new AccRange(start, false, end, false)
  }

  private def lessThanOrEqualRange(sft: SimpleFeatureType, prop: String, lit: AnyRef): AccRange = {
    val rowIdPrefix = getTableSharingPrefix(sft)
    val start = AttributeTable.getAttributeIndexRowPrefix(rowIdPrefix, sft.getDescriptor(prop))
    val end = getEncodedAttrIdxRow(sft, prop, lit)
    new AccRange(start, false, end, true)
  }

  // This function assumes that the query's filter object is or has an attribute-idx-satisfiable
  //  filter.  If not, you will get a None.get exception.
  def partitionFilter(query: Query, sft: SimpleFeatureType): (Query, Filter) = {

    val filter = query.getFilter

    val (indexFilter: Option[Filter], cqlFilter) = filter match {
      case and: And =>
        findFirst(AttributeIndexStrategy.getStrategy(_, sft, NoOpHints).isDefined)(and.getChildren)
      case f: Filter =>
        (Some(f), Seq())
    }

    val nonIndexFilters = filterListAsAnd(cqlFilter).getOrElse(Filter.INCLUDE)

    val newQuery = new Query(query)
    newQuery.setFilter(nonIndexFilters)

    if (indexFilter.isEmpty) {
      throw new Exception(s"Partition Filter was called on $query for filter $filter. " +
          "The AttributeIdxStrategy did not find a compatible sub-filter.")
    }

    (newQuery, indexFilter.get)
  }

  def configureAttributeName(cfg: IteratorSetting, attributeName: String) =
    cfg.addOption(GEOMESA_ITERATORS_ATTRIBUTE_NAME, attributeName)

  def configureIndexCoverage(cfg: IteratorSetting, coverage: IndexCoverage) =
    cfg.addOption(GEOMESA_ITERATORS_ATTRIBUTE_COVERED, coverage.toString)
}
