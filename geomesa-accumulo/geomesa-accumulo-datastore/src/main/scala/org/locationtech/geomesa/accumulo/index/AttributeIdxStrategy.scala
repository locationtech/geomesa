/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data.{Range => AccRange}
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.geotools.temporal.`object`.DefaultPeriod
import org.locationtech.geomesa.accumulo.data.tables.{AttributeTable, RecordTable}
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index.QueryPlanners.JoinFunction
import org.locationtech.geomesa.accumulo.index.Strategy._
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.filter.FilterHelper._
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.stats.{Cardinality, IndexCoverage}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.expression.{Literal, PropertyName}
import org.opengis.filter.temporal.{After, Before, During, TEquals}
import org.opengis.filter.{Filter, PropertyIsEqualTo, PropertyIsLike, _}

import scala.collection.JavaConversions._

class AttributeIdxStrategy(val filter: QueryFilter) extends Strategy with Logging {

  import org.locationtech.geomesa.accumulo.index.AttributeIdxStrategy._

  /**
   * Perform scan against the Attribute Index Table and get an iterator returning records from the Record table
   */
  override def getQueryPlan(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType) = {
    val sft = queryPlanner.sft
    val acc = queryPlanner.acc

    // pull out any dates from the filter to help narrow down the attribute ranges
    val dates = {
      val (dateFilters, _) = filter.secondary.map {
        case a: And => partitionPrimaryTemporals(a.getChildren, queryPlanner.sft)
        case f      => partitionPrimaryTemporals(Seq(f), queryPlanner.sft)
      }.getOrElse((Seq.empty, Seq.empty))
      val interval = extractInterval(dateFilters, queryPlanner.sft.getDtgField)
      if (interval == everywhen) None else Some((interval.getStartMillis, interval.getEndMillis))
    }

    // for an attribute query, the primary filters are considered an OR
    // (an AND would never match unless the attribute is a list...)
    val propsAndRanges = filter.primary.map(getPropertyAndRange(queryPlanner.sft, _, dates))
    val attributeSftIndex = propsAndRanges.head._1
    val ranges = propsAndRanges.map(_._2)
    // ensure we only have 1 prop we're working on
    assert(propsAndRanges.forall(_._1 == attributeSftIndex))

    val descriptor = sft.getDescriptor(attributeSftIndex)
    val transform = hints.getTransformSchema
    val hasDupes = descriptor.isMultiValued

    val attrTable = acc.getTableName(sft.getTypeName, AttributeTable)
    val attrThreads = acc.getSuggestedThreads(sft.getTypeName, AttributeTable)
    val priority = FILTERING_ITER_PRIORITY

    // query against the attribute table
    val singleTableScanPlan: ScanPlanFn = (schema, filter, transform) => {
      val iterators = if (filter.isDefined || transform.isDefined) {
        Seq(KryoLazyFilterTransformIterator.configure(schema, filter, transform, priority))
      } else {
        Seq.empty
      }
      // need to use transform to convert key/values if it's defined
      val kvsToFeatures = queryPlanner.kvsToFeatures(transform.map(_._2).getOrElse(schema))
      BatchScanPlan(attrTable, ranges, iterators, Seq.empty, kvsToFeatures, attrThreads, hasDupes)
    }

    if (hints.isBinQuery) {
      if (descriptor.getIndexCoverage() == IndexCoverage.FULL) {
        // can apply the bin aggregating iterator directly to the sft
        val iters = Seq(BinAggregatingIterator.configureDynamic(sft, hints, filter.secondary, priority))
        val kvsToFeatures = BinAggregatingIterator.kvsToFeatures()
        BatchScanPlan(attrTable, ranges, iters, Seq.empty, kvsToFeatures, attrThreads, hasDupes)
      } else {
        // check to see if we can execute against the index values
        val indexSft = IndexValueEncoder.getIndexSft(sft)
        if (indexSft.indexOf(hints.getBinTrackIdField) != -1 &&
            hints.getBinLabelField.forall(indexSft.indexOf(_) != -1) &&
            filter.secondary.forall(IteratorTrigger.supportsFilter(indexSft, _))) {
          val iters = Seq(BinAggregatingIterator.configureDynamic(indexSft, hints, filter.secondary, priority))
          val kvsToFeatures = BinAggregatingIterator.kvsToFeatures()
          BatchScanPlan(attrTable, ranges, iters, Seq.empty, kvsToFeatures, attrThreads, hasDupes)
        } else {
          // have to do a join against the record table
          joinQuery(sft, hints, queryPlanner, hasDupes, singleTableScanPlan)
        }
      }
    } else if (descriptor.getIndexCoverage() == IndexCoverage.FULL) {
      // we have a fully encoded value - can satisfy any query against it
      singleTableScanPlan(sft, filter.secondary, hints.getTransform)
    } else if (IteratorTrigger.canUseIndexValues(sft, filter.secondary, transform)) {
      // we can use the index value
      // transform has to be non-empty to get here
      singleTableScanPlan(IndexValueEncoder.getIndexSft(sft), filter.secondary, hints.getTransform)
    } else {
      // have to do a join against the record table
      joinQuery(sft, hints, queryPlanner, hasDupes, singleTableScanPlan)
    }
  }

  /**
   * Gets a query plan comprised of a join against the record table. This is the slowest way to
   * execute a query, so we avoid it if possible.
   */
  def joinQuery(sft: SimpleFeatureType,
                hints: Hints,
                queryPlanner: QueryPlanner,
                hasDupes: Boolean,
                attributePlan: ScanPlanFn): JoinPlan = {
    // break out the st filter to evaluate against the attribute table
    val (stFilter, ecqlFilter) = filter.secondary.map { f =>
      val (geomFilters, otherFilters) = partitionPrimarySpatials(f, sft)
      val (temporalFilters, nonSTFilters) = partitionPrimaryTemporals(otherFilters, sft)
      (andOption(geomFilters ++ temporalFilters), andOption(nonSTFilters))
    }.getOrElse((None, None))

    // the scan against the attribute table
    val attributeScan = attributePlan(IndexValueEncoder.getIndexSft(sft), stFilter, None)

    // apply any secondary filters or transforms against the record table
    val recordIterators = if (ecqlFilter.isDefined || hints.getTransformSchema.isDefined) {
      Seq(configureRecordTableIterator(sft, queryPlanner.featureEncoding, ecqlFilter, hints))
    } else {
      Seq.empty
    }
    val kvsToFeatures = if (hints.isBinQuery) {
      // TODO GEOMESA-822 we can use the aggregating iterator if the features are kryo encoded
      BinAggregatingIterator.nonAggregatedKvsToFeatures(sft, hints, queryPlanner.featureEncoding)
    } else {
      queryPlanner.defaultKVsToFeatures(hints)
    }

    // function to join the attribute index scan results to the record table
    // have to pull the feature id from the row
    val prefix = sft.getTableSharingPrefix
    val getIdFromRow = AttributeTable.getIdFromRow(sft)
    val joinFunction: JoinFunction =
      (kv) => new AccRange(RecordTable.getRowKey(prefix, getIdFromRow(kv.getKey.getRow.getBytes)))

    val recordTable = queryPlanner.acc.getTableName(sft.getTypeName, RecordTable)
    val recordThreads = queryPlanner.acc.getSuggestedThreads(sft.getTypeName, RecordTable)
    val recordRanges = Seq(new AccRange()) // this will get overwritten in the join method
    val joinQuery = BatchScanPlan(recordTable, recordRanges, recordIterators, Seq.empty,
      kvsToFeatures, recordThreads, hasDupes)

    JoinPlan(attributeScan.table, attributeScan.ranges, attributeScan.iterators,
      attributeScan.columnFamilies, recordThreads, hasDupes, joinFunction, joinQuery)
  }
}

object AttributeIdxStrategy extends StrategyProvider {

  val FILTERING_ITER_PRIORITY = 25
  type ScanPlanFn = (SimpleFeatureType, Option[Filter], Option[(String, SimpleFeatureType)]) => BatchScanPlan

  override def getCost(filter: QueryFilter, sft: SimpleFeatureType, hints: StrategyHints) = {
    val attrsAndCounts = filter.primary
      .flatMap(getAttributeProperty)
      .map(_.name)
      .groupBy((f: String) => f)
      .map { case (name, itr) => (name, itr.size) }

    val cost = attrsAndCounts.map{ case (attr, count) =>
      val descriptor = sft.getDescriptor(attr)
      // join queries are much more expensive than non-join queries
      // TODO we could consider whether a join is actually required based on the filter and transform
      // TODO figure out the actual cost of each additional range...I'll make it 2
      val additionalRangeCost = 1
      val joinCost = 10
      val multiplier =
        if (descriptor.getIndexCoverage() == IndexCoverage.JOIN) joinCost + (additionalRangeCost*(count-1))
        else 1

      // scale attribute cost by expected cardinality
      hints.cardinality(descriptor) match {
        case Cardinality.HIGH    => 1 * multiplier
        case Cardinality.UNKNOWN => 101 * multiplier
        case Cardinality.LOW     => Int.MaxValue
      }
    }.sum
    if (cost == 0) Int.MaxValue else cost // cost == 0 if somehow the filters don't match anything
  }

  /**
   * Gets the property name from the filter and a range that covers the filter in the attribute table.
   * Note that if the filter is not a valid attribute filter this method will throw an exception.
   */
  def getPropertyAndRange(sft: SimpleFeatureType,
                          filter: Filter,
                          dates: Option[(Long, Long)]): (Int, AccRange) = {
    filter match {
      case f: PropertyIsBetween =>
        val prop = sft.indexOf(f.getExpression.asInstanceOf[PropertyName].getPropertyName)
        val lower = f.getLowerBoundary.asInstanceOf[Literal].getValue
        val upper = f.getUpperBoundary.asInstanceOf[Literal].getValue
        (prop, AttributeTable.between(sft, prop, (lower, upper), dates, inclusive = true))

      case f: PropertyIsGreaterThan =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        if (prop.flipped) {
          (idx, AttributeTable.lt(sft, idx, prop.literal.getValue, dates.map(_._2)))
        } else {
          (idx, AttributeTable.gt(sft, idx, prop.literal.getValue, dates.map(_._1)))
        }

      case f: PropertyIsGreaterThanOrEqualTo =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        if (prop.flipped) {
          (idx, AttributeTable.lte(sft, idx, prop.literal.getValue, dates.map(_._2)))
        } else {
          (idx, AttributeTable.gte(sft, idx, prop.literal.getValue, dates.map(_._1)))
        }

      case f: PropertyIsLessThan =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        if (prop.flipped) {
          (idx, AttributeTable.gt(sft, idx, prop.literal.getValue, dates.map(_._1)))
        } else {
          (idx, AttributeTable.lt(sft, idx, prop.literal.getValue, dates.map(_._2)))
        }

      case f: PropertyIsLessThanOrEqualTo =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        if (prop.flipped) {
          (idx, AttributeTable.gte(sft, idx, prop.literal.getValue, dates.map(_._1)))
        } else {
          (idx, AttributeTable.lte(sft, idx, prop.literal.getValue, dates.map(_._2)))
        }

      case f: Before =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        val lit = prop.literal.evaluate(null, classOf[Date])
        if (prop.flipped) {
          (idx, AttributeTable.gt(sft, idx, lit, dates.map(_._1)))
        } else {
          (idx, AttributeTable.lt(sft, idx, lit, dates.map(_._2)))
        }

      case f: After =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        val lit = prop.literal.evaluate(null, classOf[Date])
        if (prop.flipped) {
          (idx, AttributeTable.lt(sft, idx, lit, dates.map(_._2)))
        } else {
          (idx, AttributeTable.gt(sft, idx, lit, dates.map(_._1)))
        }

      case f: During =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        val during = prop.literal.getValue.asInstanceOf[DefaultPeriod]
        val lower = during.getBeginning.getPosition.getDate.getTime
        val upper = during.getEnding.getPosition.getDate.getTime
        // note that during is exclusive
        (idx, AttributeTable.between(sft, idx, (lower, upper), dates, inclusive = false))

      case f: PropertyIsEqualTo =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        (idx, AttributeTable.equals(sft, idx, prop.literal.getValue, dates))

      case f: TEquals =>
        val prop = checkOrderUnsafe(f.getExpression1, f.getExpression2)
        val idx = sft.indexOf(prop.name)
        (idx, AttributeTable.equals(sft, idx, prop.literal.getValue, dates))

      case f: PropertyIsLike =>
        val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
        val idx = sft.indexOf(prop)
        // Remove the trailing wildcard and create a range prefix
        val literal = f.getLiteral
        val value = if (literal.endsWith(MULTICHAR_WILDCARD)) {
          literal.substring(0, literal.length - MULTICHAR_WILDCARD.length)
        } else {
          literal
        }
        (idx, AttributeTable.prefix(sft, idx, value))

      case n: Not =>
        val f = n.getFilter.asInstanceOf[PropertyIsNull] // this should have been verified in getStrategy
        val prop = f.getExpression.asInstanceOf[PropertyName].getPropertyName
        val idx = sft.indexOf(prop)
        (idx, AttributeTable.all(sft, idx))

      case _ =>
        val msg = s"Unhandled filter type in attribute strategy: ${filter.getClass.getName}"
        throw new RuntimeException(msg)
    }
  }

  def tryMergeAttrStrategy(toMerge: QueryFilter, mergeTo: QueryFilter): QueryFilter = {
    // TODO: check disjoint range queries on an attribute
    // e.g. 'height < 5 OR height > 6'
    tryMergeDisjointAttrEquals(toMerge, mergeTo)
  }

  def tryMergeDisjointAttrEquals(toMerge: QueryFilter, mergeTo: QueryFilter): QueryFilter = {
    // determine if toMerge.primary and mergeTo.primary are all Equals filters on the same attribute
    // TODO this will be incorrect for multi-valued properties where we have an AND in the primary filter
    if (isPropertyIsEqualToFilter(toMerge) && isPropertyIsEqualToFilter(mergeTo) && isSameProperty(toMerge, mergeTo)) {
      // if we have disjoint attribute queries with the same secondary filter, merge into a multi-range query
      (toMerge.secondary, mergeTo.secondary) match {
        case (Some(f1), Some(f2)) if f1.equals(f2) =>
          mergeTo.copy(primary = mergeTo.primary ++ toMerge.primary, or = true)

        case (None, None) =>
          mergeTo.copy(primary = mergeTo.primary ++ toMerge.primary, or = true)

        case _ =>
          null
      }
    } else {
      null
    }
  }

  def isPropertyIsEqualToFilter(qf: QueryFilter) = qf.primary.forall(_.isInstanceOf[PropertyIsEqualTo])

  def isSameProperty(l: QueryFilter, r: QueryFilter) = {
    val lp = distinctProperties(l)
    val rp = distinctProperties(r)
    lp.length == 1 && rp.length == 1 && lp.head == rp.head
  }

  def distinctProperties(qf: QueryFilter) = qf.primary.flatMap { f => DataUtilities.attributeNames(f) }.distinct

}
