/*
 * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.index

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.temporal.`object`.DefaultPeriod
import org.locationtech.geomesa.accumulo._
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.data.tables.{AttributeTable, AttributeTableV5, RecordTable}
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index.QueryPlanner._
import org.locationtech.geomesa.accumulo.index.QueryPlanners.JoinFunction
import org.locationtech.geomesa.accumulo.index.Strategy._
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.features.SerializationType.SerializationType
import org.locationtech.geomesa.filter.FilterHelper._
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.stats.IndexCoverage._
import org.locationtech.geomesa.utils.stats.{Cardinality, IndexCoverage}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.temporal.{After, Before, During, TEquals}

import scala.collection.JavaConverters._

@deprecated
class AttributeIdxStrategyV5(val filter: QueryFilter) extends Strategy with Logging {

  import org.locationtech.geomesa.accumulo.index.AttributeIdxStrategyV5._

  /**
   * Perform scan against the Attribute Index Table and get an iterator returning records from the Record table
   */
  override def getQueryPlan(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType) = {
    val propsAndRanges = filter.primary.map(getPropertyAndRange(_, queryPlanner.sft))
    val attributeName = propsAndRanges.head._1
    val ranges = propsAndRanges.map(_._2)
    // ensure we only have 1 prop we're working on
    assert(propsAndRanges.forall(_._1 == attributeName))

    val sft = queryPlanner.sft
    val acc = queryPlanner.acc
    val encoding = queryPlanner.featureEncoding
    val version = sft.getSchemaVersion
    val hasDupes = sft.getDescriptor(attributeName).isMultiValued

    val attributeIterators = scala.collection.mutable.ArrayBuffer.empty[IteratorSetting]

    val (stFilter, ecqlFilter) = filter.secondary.map { f =>
      val (geomFilters, otherFilters) = partitionPrimarySpatials(f, sft)
      val (temporalFilters, nonSTFilters) = partitionPrimaryTemporals(otherFilters, sft)
      val st = andOption(geomFilters ++ temporalFilters)
      val ecql = andOption(nonSTFilters)
      (st, ecql)
    }.getOrElse((None, None))

    val kvsToFeatures = if (hints.isBinQuery) {
      // TODO GEOMESA-822 we can use the aggregating iterator if the features are kryo encoded
      BinAggregatingIterator.nonAggregatedKvsToFeatures(sft, hints, encoding)
    } else {
      queryPlanner.defaultKVsToFeatures(hints)
    }

    // choose which iterator we want to use - joining iterator or attribute only iterator
    val iteratorChoice: IteratorConfig =
      IteratorTrigger.chooseAttributeIterator(ecqlFilter, hints, sft, attributeName)

    iteratorChoice.iterator match {
      case IndexOnlyIterator =>
        // the attribute index iterator also handles transforms and date/geom filters
        val cfg = configureAttributeIndexIterator(sft, encoding, hints, stFilter, ecqlFilter,
          iteratorChoice.transformCoversFilter, attributeName, version)
        attributeIterators.append(cfg)

        // if this is a request for unique attribute values, add the skipping iterator to speed up response
        if (hints.containsKey(GEOMESA_UNIQUE)) {
          attributeIterators.append(configureUniqueAttributeIterator())
        }

        // there won't be any non-date/time-filters if the index only iterator has been selected
        val table = acc.getTableName(sft.getTypeName, AttributeTableV5)
        BatchScanPlan(table, ranges, attributeIterators.toSeq, Seq.empty, kvsToFeatures, 1, hasDupes)

      case RecordJoinIterator =>
        val recordIterators = scala.collection.mutable.ArrayBuffer.empty[IteratorSetting]

        stFilter.foreach { filter =>
          // apply a filter for the indexed date and geometry
          attributeIterators.append(configureSpatioTemporalFilter(sft, encoding, stFilter, version))
        }

        if (iteratorChoice.hasTransformOrFilter) {
          // apply an iterator for any remaining transforms/filters
          recordIterators.append(configureRecordTableIterator(sft, encoding, ecqlFilter, hints))
        }

        // function to join the attribute index scan results to the record table
        // since the row id of the record table is in the CF just grab that
        val prefix = sft.getTableSharingPrefix
        val joinFunction: JoinFunction =
          (kv) => new AccRange(RecordTable.getRowKey(prefix, kv.getKey.getColumnQualifier.toString))

        val recordTable = acc.getTableName(sft.getTypeName, RecordTable)
        val recordThreads = acc.getSuggestedThreads(sft.getTypeName, RecordTable)
        val recordRanges = Seq(new AccRange()) // this will get overwritten in the join method
        val joinQuery = BatchScanPlan(recordTable, recordRanges, recordIterators.toSeq, Seq.empty,
          kvsToFeatures, recordThreads, hasDupes)

        val attrTable = acc.getTableName(sft.getTypeName, AttributeTableV5)
        val attrThreads = acc.getSuggestedThreads(sft.getTypeName, AttributeTableV5)
        val attrIters = attributeIterators.toSeq
        JoinPlan(attrTable, ranges, attrIters, Seq.empty, attrThreads, hasDupes, joinFunction, joinQuery)
    }
  }

  private def configureAttributeIndexIterator(
      featureType: SimpleFeatureType,
      encoding: SerializationType,
      hints: Hints,
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
      configureTransforms(cfg, hints)
    } else {
      // we can evaluate the filter against the transformed schema, so skip the original feature decoding
      hints.getTransformSchema.foreach(transformedType => configureFeatureType(cfg, transformedType))
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

@deprecated
object AttributeIdxStrategyV5 extends StrategyProvider {

  override def getCost(filter: QueryFilter, sft: SimpleFeatureType, hints: StrategyHints) = {
    val cost = filter.primary.flatMap(getAttributeProperty).map { p =>
      val descriptor = sft.getDescriptor(p.name)
      val multiplier = if (descriptor.getIndexCoverage() == IndexCoverage.JOIN) 2 else 1
      hints.cardinality(descriptor) match {
        case Cardinality.HIGH    => 1 * multiplier
        case Cardinality.UNKNOWN => 999 * multiplier
        case Cardinality.LOW     => Int.MaxValue
      }
    }.sum
    if (cost == 0) Int.MaxValue else cost // cost == 0 if somehow the filters don't match anything
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

    val rowIdPrefix = sft.getTableSharingPrefix
    // grab the first encoded row - right now there will only ever be a single item in the seq
    // eventually we may support searching a whole collection at once
    val rowWithValue = AttributeTableV5.getAttributeIndexRows(rowIdPrefix, descriptor, typedValue).headOption
    // if value is null there won't be any rows returned, instead just use the row prefix
    rowWithValue.getOrElse(AttributeTableV5.getAttributeIndexRowPrefix(rowIdPrefix, descriptor))
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
        val value = if (literal.endsWith(MULTICHAR_WILDCARD)) {
          literal.substring(0, literal.length - MULTICHAR_WILDCARD.length)
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
    val rowIdPrefix = sft.getTableSharingPrefix
    new Text(AttributeTableV5.getAttributeIndexRowPrefix(rowIdPrefix, sft.getDescriptor(prop)))
  }

  private def upperBound(sft: SimpleFeatureType, prop: String): Text = {
    val rowIdPrefix = sft.getTableSharingPrefix
    val end = new Text(AttributeTableV5.getAttributeIndexRowPrefix(rowIdPrefix, sft.getDescriptor(prop)))
    AccRange.followingPrefix(end)
  }

  def configureAttributeName(cfg: IteratorSetting, attributeName: String) =
    cfg.addOption(GEOMESA_ITERATORS_ATTRIBUTE_NAME, attributeName)

  def configureIndexCoverage(cfg: IteratorSetting, coverage: IndexCoverage) =
    cfg.addOption(GEOMESA_ITERATORS_ATTRIBUTE_COVERED, coverage.toString)
}
