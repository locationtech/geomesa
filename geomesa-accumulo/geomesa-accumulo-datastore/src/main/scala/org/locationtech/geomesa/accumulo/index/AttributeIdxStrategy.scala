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

package org.locationtech.geomesa.accumulo.index

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.hadoop.io.Text
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.geotools.temporal.`object`.DefaultPeriod
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.tables.{AttributeTable, RecordTable}
import org.locationtech.geomesa.accumulo.filter._
import org.locationtech.geomesa.accumulo.index.FilterHelper._
import org.locationtech.geomesa.accumulo.index.QueryPlanner._
import org.locationtech.geomesa.accumulo.index.QueryPlanners.JoinFunction
import org.locationtech.geomesa.accumulo.index.Strategy._
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.features.SerializationType.SerializationType
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

  import org.locationtech.geomesa.accumulo.index.AttributeIndexStrategy._

  /**
   * Perform scan against the Attribute Index Table and get an iterator returning records from the Record table
   */
  def getAttributeIdxQueryPlan(query: Query,
                               queryPlanner: QueryPlanner,
                               attributeName: String,
                               range: AccRange,
                               output: ExplainerOutputType): QueryPlan = {

    val sft = queryPlanner.sft
    val acc = queryPlanner.acc

    output(s"Scanning attribute table for feature type ${sft.getTypeName}")
    output(s"Range: ${ExplainerOutputType.toString(range)}")
    output(s"Filter: ${query.getFilter}")

    val attributeIterators = scala.collection.mutable.ArrayBuffer.empty[IteratorSetting]

    val (geomFilters, otherFilters) = partitionGeom(query.getFilter, sft)
    val (temporalFilters, nonSTFilters) = partitionTemporal(otherFilters, getDtgFieldName(sft))

    output(s"Geometry filters: $geomFilters")
    output(s"Temporal filters: $temporalFilters")
    output(s"Other filters: $nonSTFilters")

    val stFilter: Option[Filter] = filterListAsAnd(geomFilters ++ temporalFilters)
    val ecqlFilter: Option[Filter] = filterListAsAnd(nonSTFilters)

    val encoding = queryPlanner.featureEncoding
    val version = acc.getGeomesaVersion(sft)
    val hasDupes = sft.getDescriptor(attributeName).isMultiValued
    val kvsToFeatures = queryPlanner.defaultKVsToFeatures(query)

    // choose which iterator we want to use - joining iterator or attribute only iterator
    val iteratorChoice: IteratorConfig =
      IteratorTrigger.chooseAttributeIterator(ecqlFilter, query, sft, attributeName)

    iteratorChoice.iterator match {
      case IndexOnlyIterator =>
        // the attribute index iterator also handles transforms and date/geom filters
        val cfg = configureAttributeIndexIterator(sft, encoding, query, stFilter, ecqlFilter,
          iteratorChoice.transformCoversFilter, attributeName, version)
        attributeIterators.append(cfg)

        // if this is a request for unique attribute values, add the skipping iterator to speed up response
        if (query.getHints.containsKey(GEOMESA_UNIQUE)) {
          attributeIterators.append(configureUniqueAttributeIterator())
        }

        // there won't be any non-date/time-filters if the index only iterator has been selected
        val table = acc.getAttributeTable(sft)
        ScanPlan(table, range, attributeIterators.toSeq, Seq.empty, kvsToFeatures, hasDupes)

      case RecordJoinIterator =>
        output("Using record join iterator")
        val recordIterators = scala.collection.mutable.ArrayBuffer.empty[IteratorSetting]

        stFilter.foreach { filter =>
          // apply a filter for the indexed date and geometry
          attributeIterators.append(configureSpatioTemporalFilter(sft, encoding, stFilter, version))
        }

        if (iteratorChoice.hasTransformOrFilter) {
          // apply an iterator for any remaining transforms/filters
          recordIterators.append(configureRecordTableIterator(sft, encoding, ecqlFilter, query))
        }

        // function to join the attribute index scan results to the record table
        // since the row id of the record table is in the CF just grab that
        val prefix = getTableSharingPrefix(sft)
        val joinFunction: JoinFunction =
          (kv) => new AccRange(RecordTable.getRowKey(prefix, kv.getKey.getColumnQualifier.toString))

        val recordTable = acc.getRecordTable(sft)
        val recordRanges = Seq(new AccRange()) // this will get overwritten in the join method
        val recordThreads = acc.getSuggestedRecordThreads(sft)
        val joinQuery =
          BatchScanPlan(recordTable, recordRanges, recordIterators.toSeq, Seq.empty, kvsToFeatures, recordThreads, hasDupes)

        val attrTable = acc.getAttributeTable(sft)
        val attrThreads = acc.getSuggestedAttributeThreads(sft)
        val attrIters = attributeIterators.toSeq
        JoinPlan(attrTable, Seq(range), attrIters, Seq.empty, attrThreads, hasDupes, joinFunction, joinQuery)
    }
  }

  private def configureAttributeIndexIterator(
      featureType: SimpleFeatureType,
      encoding: SerializationType,
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
      getTransformSchema(query).foreach(transformedType => configureFeatureType(cfg, transformedType))
    }

    cfg
  }

  private def configureSpatioTemporalFilter(
      featureType: SimpleFeatureType,
      encoding: SerializationType,
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

  import org.locationtech.geomesa.accumulo.index.AttributeIndexStrategy._

  override def getQueryPlans(query: Query, queryPlanner: QueryPlanner, output: ExplainerOutputType) = {
    val (strippedQuery, filter) = partitionFilter(query, queryPlanner.sft)
    val (prop, range) = getPropertyAndRange(filter, queryPlanner.sft)
    val ret = getAttributeIdxQueryPlan(strippedQuery, queryPlanner, prop, range, output)
    Seq(ret)
  }
}

class AttributeIdxRangeStrategy extends AttributeIdxStrategy {

  import org.locationtech.geomesa.accumulo.index.AttributeIndexStrategy._

  override def getQueryPlans(query: Query, queryPlanner: QueryPlanner, output: ExplainerOutputType) = {
    val (strippedQuery, filter) = partitionFilter(query, queryPlanner.sft)
    val (prop, range) = getPropertyAndRange(filter, queryPlanner.sft)
    val ret = getAttributeIdxQueryPlan(strippedQuery, queryPlanner, prop, range, output)
    Seq(ret)
  }
}

class AttributeIdxLikeStrategy extends AttributeIdxStrategy {

  import org.locationtech.geomesa.accumulo.index.AttributeIndexStrategy._

  override def getQueryPlans(query: Query, queryPlanner: QueryPlanner, output: ExplainerOutputType) = {
    val (strippedQuery, extractedFilter) = partitionFilter(query, queryPlanner.sft)
    val (prop, range) = getPropertyAndRange(extractedFilter, queryPlanner.sft)
    val ret = getAttributeIdxQueryPlan(strippedQuery, queryPlanner, prop, range, output)
    Seq(ret)
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

      // not check - we only support 'not null' for an indexed attribute
      case n: Not =>
        Option(n.getFilter).collect { case f: PropertyIsNull => f }.flatMap { f =>
          val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
          val descriptor = sft.getDescriptor(prop)
          if (descriptor.isIndexed) {
            Some(StrategyDecision(new AttributeIdxRangeStrategy, cost(descriptor)))
          } else {
            None
          }
        }

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

    val rowIdPrefix = org.locationtech.geomesa.accumulo.index.getTableSharingPrefix(sft)
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
        (prop, inclusiveRange(sft, prop, lower, upper))

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
        (prop.name, inclusiveRange(sft, prop.name, lower, upper))

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

      case n: Not =>
        val f = n.getFilter.asInstanceOf[PropertyIsNull] // this should have been verified in getStrategy
        val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
        (prop, allRange(sft, prop))

      case _ =>
        val msg = s"Unhandled filter type in attribute strategy: ${filter.getClass.getName}"
        throw new RuntimeException(msg)
    }

  private def greaterThanRange(sft: SimpleFeatureType, prop: String, lit: AnyRef): AccRange = {
    val start = new Text(getEncodedAttrIdxRow(sft, prop, lit))
    val end = upperBound(sft, prop)
    new AccRange(start, false, end, false)
  }

  private def greaterThanOrEqualRange(sft: SimpleFeatureType, prop: String, lit: AnyRef): AccRange = {
    val start = new Text(getEncodedAttrIdxRow(sft, prop, lit))
    val end = upperBound(sft, prop)
    new AccRange(start, true, end, false)
  }

  private def lessThanRange(sft: SimpleFeatureType, prop: String, lit: AnyRef): AccRange = {
    val start = lowerBound(sft, prop)
    val end = new Text(getEncodedAttrIdxRow(sft, prop, lit))
    new AccRange(start, false, end, false)
  }

  private def lessThanOrEqualRange(sft: SimpleFeatureType, prop: String, lit: AnyRef): AccRange = {
    val start = lowerBound(sft, prop)
    val end = new Text(getEncodedAttrIdxRow(sft, prop, lit))
    new AccRange(start, false, end, true)
  }

  private def inclusiveRange(sft: SimpleFeatureType, prop: String, lower: AnyRef, upper: AnyRef): AccRange = {
    val start = getEncodedAttrIdxRow(sft, prop, lower)
    val end = getEncodedAttrIdxRow(sft, prop, upper)
    new AccRange(start, true, end, true)
  }

  private def allRange(sft: SimpleFeatureType, prop: String): AccRange =
    new AccRange(lowerBound(sft, prop), false, upperBound(sft, prop), false)

  private def lowerBound(sft: SimpleFeatureType, prop: String): Text = {
    val rowIdPrefix = getTableSharingPrefix(sft)
    new Text(AttributeTable.getAttributeIndexRowPrefix(rowIdPrefix, sft.getDescriptor(prop)))
  }

  private def upperBound(sft: SimpleFeatureType, prop: String): Text = {
    val rowIdPrefix = getTableSharingPrefix(sft)
    val end = new Text(AttributeTable.getAttributeIndexRowPrefix(rowIdPrefix, sft.getDescriptor(prop)))
    AccRange.followingPrefix(end)
  }

  // This function assumes that the query's filter object is or has an attribute-idx-satisfiable
  //  filter.  If not, you will get a None.get exception.
  def partitionFilter(query: Query, sft: SimpleFeatureType): (Query, Filter) = {

    val filter = query.getFilter

    val (indexFilter: Option[Filter], cqlFilter) = filter match {
      case and: And =>
        val costFn = AttributeIndexStrategy.getStrategy(_ : Filter, sft, NoOpHints).map(_.cost)
        findBest(costFn)(and.getChildren).extract
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
