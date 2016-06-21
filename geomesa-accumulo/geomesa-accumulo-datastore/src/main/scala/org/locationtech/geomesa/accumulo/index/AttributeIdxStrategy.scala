/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Range => AccRange}
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaStats
import org.locationtech.geomesa.accumulo.data.tables.{AttributeTable, RecordTable}
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index.QueryPlanners.JoinFunction
import org.locationtech.geomesa.accumulo.index.Strategy._
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.locationtech.geomesa.utils.stats.{Cardinality, IndexCoverage, Stat}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.{Filter, PropertyIsEqualTo}

import scala.util.Try

class AttributeIdxStrategy(val filter: QueryFilter) extends Strategy with LazyLogging {

  import org.locationtech.geomesa.accumulo.index.AttributeIdxStrategy._

  /**
   * Perform scan against the Attribute Index Table and get an iterator returning records from the Record table
   */
  override def getQueryPlan(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType) = {
    val ds = queryPlanner.ds
    val sft = queryPlanner.sft

    // pull out any dates from the filter to help narrow down the attribute ranges
    val dates = for {
      dtgField  <- sft.getDtgField
      secondary <- filter.secondary
      intervals = FilterHelper.extractIntervals(secondary, dtgField)
      if intervals.nonEmpty
    } yield {
      (intervals.map(_._1.getMillis).min, intervals.map(_._2.getMillis).max)
    }

    // for an attribute query, the primary filters are considered an OR
    // (an AND would never match unless the attribute is a list...)
    val propsAndRanges = {
      val primary = filter.singlePrimary.getOrElse {
        throw new IllegalStateException("Attribute index does not support Filter.INCLUDE")
      }
      getBounds(sft, primary, dates)
    }
    val attribute = propsAndRanges.headOption.map(_.attribute).getOrElse {
      throw new IllegalStateException(s"No ranges found for query filter $filter. " +
          "This should have been checked during query planning")
    }
    val ranges = propsAndRanges.map(_.range)
    // ensure we only have 1 prop we're working on
    assert(propsAndRanges.forall(_.attribute == attribute))

    val descriptor = sft.getDescriptor(attribute)
    val transform = hints.getTransformSchema
    val sampling = hints.getSampling
    val hasDupes = descriptor.isMultiValued

    val attrTable = ds.getTableName(sft.getTypeName, AttributeTable)
    val attrThreads = ds.getSuggestedThreads(sft.getTypeName, AttributeTable)
    val priority = FILTERING_ITER_PRIORITY

    // query against the attribute table
    val singleAttrValueOnlyPlan: ScanPlanFn = (schema, ecql, transform) => {
      val perAttributeIter = sft.getVisibilityLevel match {
        case VisibilityLevel.Feature   => Seq.empty
        case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(sft, AttributeTable))
      }
      val iter = KryoLazyFilterTransformIterator.configure(schema, ecql, transform, sampling).toSeq
      val iters = perAttributeIter ++ iter
      // need to use transform to convert key/values if it's defined
      val kvsToFeatures = queryPlanner.kvsToFeatures(transform.map(_._2).getOrElse(schema))
      BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, kvsToFeatures, attrThreads, hasDupes)
    }

    // query against the attribute table
    val singleAttrPlusValuePlan: ScanPlanFn = (schema, ecql, transform) => {
      val iters =
        AttrKeyPlusValueIterator.configure(sft, schema, sft.indexOf(attribute), ecql, transform, sampling, priority)

      // need to use transform to convert key/values if it's defined
      val kvsToFeatures = queryPlanner.kvsToFeatures(transform.map(_._2).getOrElse(schema))
      BatchScanPlan(filter, attrTable, ranges, Seq(iters), Seq.empty, kvsToFeatures, attrThreads, hasDupes)
    }

    if (hints.isBinQuery) {
      if (descriptor.getIndexCoverage() == IndexCoverage.FULL) {
        // can apply the bin aggregating iterator directly to the sft
        val iters = Seq(BinAggregatingIterator.configureDynamic(sft, filter.secondary, hints, hasDupes))
        val kvsToFeatures = BinAggregatingIterator.kvsToFeatures()
        BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, kvsToFeatures, attrThreads, hasDupes)
      } else {
        // check to see if we can execute against the index values
        val indexSft = IndexValueEncoder.getIndexSft(sft)
        if (indexSft.indexOf(hints.getBinTrackIdField) != -1 &&
            hints.getBinLabelField.forall(indexSft.indexOf(_) != -1) &&
            filter.secondary.forall(IteratorTrigger.supportsFilter(indexSft, _))) {
          val iters = Seq(BinAggregatingIterator.configureDynamic(indexSft, filter.secondary, hints, hasDupes))
          val kvsToFeatures = BinAggregatingIterator.kvsToFeatures()
          BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, kvsToFeatures, attrThreads, hasDupes)
        } else {
          // have to do a join against the record table
          joinQuery(sft, hints, queryPlanner, hasDupes, singleAttrValueOnlyPlan)
        }
      }
    } else if (hints.isStatsIteratorQuery) {
      val kvsToFeatures = KryoLazyStatsIterator.kvsToFeatures(sft)
      if (descriptor.getIndexCoverage() == IndexCoverage.FULL) {
        val iters = Seq(KryoLazyStatsIterator.configure(sft, filter.secondary, hints, hasDupes))
        BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, kvsToFeatures, attrThreads, hasDuplicates = false)
      } else {
        // check to see if we can execute against the index values
        val indexSft = IndexValueEncoder.getIndexSft(sft)
        if (Try(Stat(indexSft, hints.getStatsIteratorQuery)).isSuccess &&
            filter.secondary.forall(IteratorTrigger.supportsFilter(indexSft, _))) {
          val iters = Seq(KryoLazyStatsIterator.configure(indexSft, filter.secondary, hints, hasDupes))
          BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, kvsToFeatures, attrThreads, hasDuplicates = false)
        } else {
          // have to do a join against the record table
          joinQuery(sft, hints, queryPlanner, hasDupes, singleAttrValueOnlyPlan)
        }
      }
    } else if (descriptor.getIndexCoverage() == IndexCoverage.FULL) {
      // we have a fully encoded value - can satisfy any query against it
      singleAttrValueOnlyPlan(sft, filter.secondary, hints.getTransform)
    } else if (IteratorTrigger.canUseAttrIdxValues(sft, filter.secondary, transform)) {
      // we can use the index value.
      // transform has to be non-empty to get here and can only include items
      // in the index value (not the index keys aka the attribute indexed)
      singleAttrValueOnlyPlan(IndexValueEncoder.getIndexSft(sft), filter.secondary, hints.getTransform)
    } else if (IteratorTrigger.canUseAttrKeysPlusValues(descriptor.getLocalName, sft, filter.secondary, transform)) {
      // we can use the index PLUS the value
      singleAttrPlusValuePlan(IndexValueEncoder.getIndexSft(sft), filter.secondary, hints.getTransform)
    } else {
      // have to do a join against the record table
      joinQuery(sft, hints, queryPlanner, hasDupes, singleAttrValueOnlyPlan)
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
    val recordIterators = if (hints.isStatsIteratorQuery) {
      Seq(KryoLazyStatsIterator.configure(sft, ecqlFilter, hints, deduplicate = false))
    } else if (ecqlFilter.isDefined || hints.getTransformSchema.isDefined) {
      Seq(configureRecordTableIterator(sft, queryPlanner.ds.getFeatureEncoding(sft), ecqlFilter, hints))
    } else {
      Seq.empty
    }
    val kvsToFeatures = if (hints.isBinQuery) {
      // TODO GEOMESA-822 we can use the aggregating iterator if the features are kryo encoded
      BinAggregatingIterator.nonAggregatedKvsToFeatures(sft, hints, queryPlanner.ds.getFeatureEncoding(sft))
    } else if (hints.isStatsIteratorQuery) {
      KryoLazyStatsIterator.kvsToFeatures(sft)
    } else {
      queryPlanner.defaultKVsToFeatures(hints)
    }

    // function to join the attribute index scan results to the record table
    // have to pull the feature id from the row
    val prefix = sft.getTableSharingPrefix
    val getIdFromRow = AttributeTable.getIdFromRow(sft)
    val joinFunction: JoinFunction =
      (kv) => new AccRange(RecordTable.getRowKey(prefix, getIdFromRow(kv.getKey.getRow.getBytes)))

    val recordTable = queryPlanner.ds.getTableName(sft.getTypeName, RecordTable)
    val recordThreads = queryPlanner.ds.getSuggestedThreads(sft.getTypeName, RecordTable)
    val recordRanges = Seq(new AccRange()) // this will get overwritten in the join method
    val joinQuery = BatchScanPlan(filter, recordTable, recordRanges, recordIterators, Seq.empty,
      kvsToFeatures, recordThreads, hasDupes)

    JoinPlan(filter, attributeScan.table, attributeScan.ranges, attributeScan.iterators,
      attributeScan.columnFamilies, recordThreads, hasDupes, joinFunction, joinQuery)
  }
}

object AttributeIdxStrategy extends StrategyProvider {

  val FILTERING_ITER_PRIORITY = 25
  type ScanPlanFn = (SimpleFeatureType, Option[Filter], Option[(String, SimpleFeatureType)]) => BatchScanPlan

  override protected def statsBasedCost(sft: SimpleFeatureType,
                                        filter: QueryFilter,
                                        transform: Option[SimpleFeatureType],
                                        stats: GeoMesaStats): Option[Long] = {
    filter.singlePrimary match {
      case None => Some(Long.MaxValue)
      case Some(f) =>
        stats.getCount(sft, f, exact = false).map { count =>
          // account for cardinality and index coverage
          val attribute = FilterHelper.propertyNames(f, sft).head
          val descriptor = sft.getDescriptor(attribute)
          if (descriptor.getCardinality == Cardinality.HIGH) {
            count / 10 // prioritize attributes marked high-cardinality
          } else if (descriptor.getIndexCoverage == IndexCoverage.FULL ||
                       IteratorTrigger.canUseAttrIdxValues(sft, filter.secondary, transform) ||
                       IteratorTrigger.canUseAttrKeysPlusValues(attribute, sft, filter.secondary, transform)) {
            count
          } else {
            count * 10 // de-prioritize join queries, they are much more expensive
          }
        }
    }
  }

  /**
    * full index:
    *   high cardinality - 1
    *   unknown cardinality - 101
    * join index:
    *   high cardinality - 10
    *   unknown cardinality - 1010
    * low cardinality - Long.MaxValue
    *
    * Compare with id lookups at 1, z2/z3 at 200-401
    */
  override protected def indexBasedCost(sft: SimpleFeatureType,
                                        filter: QueryFilter,
                                        transform: Option[SimpleFeatureType]): Long = {
    // note: names should be only a single attribute
    val attrsAndCounts = filter.primary
      .flatMap(getAttributeProperty)
      .map(_.name)
      .groupBy((f: String) => f)
      .map { case (name, itr) => (name, itr.size) }

    val cost = attrsAndCounts.map { case (attr, count) =>
      val descriptor = sft.getDescriptor(attr)
      // join queries are much more expensive than non-join queries
      // TODO figure out the actual cost of each additional range...I'll make it 2
      val additionalRangeCost = 1
      val joinCost = 10
      val multiplier =
        if (descriptor.getIndexCoverage == IndexCoverage.FULL ||
              IteratorTrigger.canUseAttrIdxValues(sft, filter.secondary, transform) ||
              IteratorTrigger.canUseAttrKeysPlusValues(attr, sft, filter.secondary, transform)) {
          1
        } else {
          joinCost + (additionalRangeCost*(count-1))
        }

      // scale attribute cost by expected cardinality
      descriptor.getCardinality() match {
        case Cardinality.HIGH    => 1 * multiplier
        case Cardinality.UNKNOWN => 101 * multiplier
        case Cardinality.LOW     => Long.MaxValue
      }
    }.sum
    if (cost == 0) Long.MaxValue else cost // cost == 0 if somehow the filters don't match anything
  }

  /**
   * Gets the property name from the filter and a range that covers the filter in the attribute table.
   * Note that if the filter is not a valid attribute filter this method will throw an exception.
   */
  def getBounds(sft: SimpleFeatureType, filter: Filter, dates: Option[(Long, Long)]): Seq[PropertyBounds] = {
    val attribute = {
      val names = DataUtilities.attributeNames(filter)
      require(names.length == 1, s"Couldn't extract single attribute name from filter '${filterToString(filter)}'")
      names(0)
    }

    val index = sft.indexOf(attribute)
    require(index != -1, s"Attribute '$attribute' from filter '${filterToString(filter)}' does not exist in '$sft'")

    val binding = {
      val descriptor = sft.getDescriptor(index)
      descriptor.getListType().getOrElse(descriptor.getType.getBinding)
    }

    require(classOf[Comparable[_]].isAssignableFrom(binding), s"Attribute '$attribute' is not comparable")

    val fb = FilterHelper.extractAttributeBounds(filter, attribute, binding).getOrElse {
      throw new RuntimeException(s"Unhandled filter type in attribute strategy: ${filterToString(filter)}")
    }

    fb.bounds.map { bounds =>
      val range = bounds.bounds match {
        case (Some(lower), Some(upper)) =>
          if (lower == upper) {
            AttributeTable.equals(sft, index, lower, dates)
          } else if (lower + WILDCARD_SUFFIX == upper) {
            AttributeTable.prefix(sft, index, lower)
          } else {
            AttributeTable.between(sft, index, (lower, upper), dates, bounds.inclusive)
          }
        case (Some(lower), None) =>
          if (bounds.inclusive) {
            AttributeTable.gte(sft, index, lower, dates.map(_._1))
          } else {
            AttributeTable.gt(sft, index, lower, dates.map(_._1))
          }
        case (None, Some(upper)) =>
          if (bounds.inclusive) {
            AttributeTable.lte(sft, index, upper, dates.map(_._2))
          } else {
            AttributeTable.lt(sft, index, upper, dates.map(_._2))
          }
        case (None, None) => // not null
          AttributeTable.all(sft, index)
      }

      PropertyBounds(attribute, bounds.bounds, range)
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

case class PropertyBounds(attribute: String, bounds: (Option[Any], Option[Any]), range: AccRange)
