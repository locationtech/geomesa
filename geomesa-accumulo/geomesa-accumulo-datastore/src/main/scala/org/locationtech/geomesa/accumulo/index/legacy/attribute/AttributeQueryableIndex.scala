/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.legacy.attribute

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.hadoop.io.Text
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.AccumuloFilterStrategyType
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.index.AccumuloQueryPlan.JoinFunction
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.index.encoders.IndexValueEncoder
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.features.SerializationType
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.filter.visitor.FilterExtractingVisitor
import org.locationtech.geomesa.index.api.FilterStrategy
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.index.{IndexMode, VisibilityLevel}
import org.locationtech.geomesa.utils.stats.{Cardinality, IndexCoverage, Stat}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.temporal.{After, Before, During, TEquals}

import scala.util.Try

trait AttributeQueryableIndex extends AccumuloFeatureIndex with LazyLogging {

  import AccumuloAttributeIndex.requiresJoin
  import AttributeQueryableIndex.attributeCheck
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  type ScanPlanFn = (SimpleFeatureType, Option[Filter], Option[(String, SimpleFeatureType)]) => BatchScanPlan

  override def getQueryPlan(sft: SimpleFeatureType,
                            ds: AccumuloDataStore,
                            filter: AccumuloFilterStrategyType,
                            hints: Hints,
                            explain: Explainer): AccumuloQueryPlan = {

    val primary = filter.primary.getOrElse {
      throw new IllegalStateException("Attribute index does not support Filter.INCLUDE")
    }

    // pull out any dates from the filter to help narrow down the attribute ranges
    val intervals = for {
      dtgField  <- sft.getDtgField
      secondary <- filter.secondary
      intervals =  FilterHelper.extractIntervals(secondary, dtgField)
      if intervals.nonEmpty
    } yield {
      intervals
    }

    lazy val dates = intervals.map { i =>
      (i.values.map(_.lower.value.map(_.toInstant.toEpochMilli).getOrElse(0L)).min,
          i.values.map(_.upper.value.map(_.toInstant.toEpochMilli).getOrElse(Long.MaxValue)).max)
    }
    // TODO GEOMESA-1336 fix exclusive AND handling for list types
    lazy val bounds = AttributeQueryableIndex.getBounds(sft, primary, dates)

    if (intervals.exists(_.disjoint) || bounds.isEmpty) {
      EmptyPlan(filter)
    } else {
      nonEmptyQueryPlan(ds, sft, filter, hints, bounds)
    }
  }

  private def nonEmptyQueryPlan(ds: AccumuloDataStore,
                                sft: SimpleFeatureType,
                                filter: AccumuloFilterStrategyType,
                                hints: Hints,
                                bounds: Seq[PropertyBounds]): AccumuloQueryPlan = {

    val attribute = bounds.head.attribute
    val ranges = bounds.map(_.range)
    // ensure we only have 1 prop we're working on
    require(bounds.forall(_.attribute == attribute), "Found multiple attributes in attribute filter")

    val descriptor = sft.getDescriptor(attribute)
    val transform = hints.getTransformSchema
    val sampling = hints.getSampling
    val hasDupes = descriptor.isMultiValued

    val attrTable = getTableNames(sft, ds, None)
    val attrThreads = ds.config.queryThreads

    def visibilityIter(schema: SimpleFeatureType): Seq[IteratorSetting] = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   => Seq.empty
      case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(schema))
    }

    // query against the attribute table
    val singleAttrValueOnlyPlan: ScanPlanFn = (schema, ecql, transform) => {
      val iter = KryoLazyFilterTransformIterator.configure(schema, this, ecql, transform, sampling)
      val iters = visibilityIter(schema) ++ iter.toSeq
      // need to use transform to convert key/values if it's defined
      val kvsToFeatures = entriesToFeatures(sft, transform.map(_._2).getOrElse(schema))
      BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, kvsToFeatures, None, attrThreads, hasDupes)
    }

    if (hints.isBinQuery) {
      if (descriptor.getIndexCoverage() == IndexCoverage.FULL) {
        // can apply the bin aggregating iterator directly to the sft
        val iter = BinAggregatingIterator.configureDynamic(sft, this, filter.secondary, hints, hasDupes)
        val iters = visibilityIter(sft) :+ iter
        val kvsToFeatures = BinAggregatingIterator.kvsToFeatures()
        BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, kvsToFeatures, None, attrThreads, hasDupes)
      } else {
        // check to see if we can execute against the index values
        val indexSft = IndexValueEncoder.getIndexSft(sft)
        if (indexSft.indexOf(hints.getBinTrackIdField) != -1 &&
            hints.getBinGeomField.forall(indexSft.indexOf(_) != -1) &&
            hints.getBinLabelField.forall(indexSft.indexOf(_) != -1) &&
            filter.secondary.forall(IteratorTrigger.supportsFilter(indexSft, _))) {
          val iter = BinAggregatingIterator.configureDynamic(indexSft, this, filter.secondary, hints, hasDupes)
          val iters = visibilityIter(indexSft) :+ iter
          val kvsToFeatures = BinAggregatingIterator.kvsToFeatures()
          BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, kvsToFeatures, None, attrThreads, hasDupes)
        } else {
          // have to do a join against the record table
          joinQuery(ds, sft, filter, hints, hasDupes, singleAttrValueOnlyPlan)
        }
      }
    } else if (hints.isArrowQuery) {
      if (descriptor.getIndexCoverage() == IndexCoverage.FULL) {
        val (iter, reduce) = ArrowIterator.configure(sft, this, ds.stats, filter.filter, filter.secondary, hints, hasDupes)
        val iters = visibilityIter(sft) :+ iter
        BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, ArrowIterator.kvsToFeatures(), Some(reduce), attrThreads, hasDuplicates = false)
      } else if (IteratorTrigger.canUseAttrIdxValues(sft, filter.secondary, transform)) {
        // ^ check to see if we can execute against the index values
        val indexSft = IndexValueEncoder.getIndexSft(sft)
        val (iter, reduce) = ArrowIterator.configure(indexSft, this, ds.stats, filter.filter, filter.secondary, hints, hasDupes)
        val iters = visibilityIter(indexSft) :+ iter
        BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, ArrowIterator.kvsToFeatures(), Some(reduce), attrThreads, hasDuplicates = false)
      } else if (IteratorTrigger.canUseAttrKeysPlusValues(attribute, sft, filter.secondary, transform)) {
        val transformSft = transform.getOrElse {
          throw new IllegalStateException("Must have a transform for attribute key plus value scan")
        }
        hints.clearTransforms() // clear the transforms as we've already accounted for them
        val indexSft = IndexValueEncoder.getIndexSft(sft)
        // note: ECQL is handled below, so we don't pass it to the arrow iter here
        val (iter, reduce) = ArrowIterator.configure(transformSft, this, ds.stats, filter.filter, None, hints, hasDupes)
        val indexValueIter = AttributeIndexValueIterator.configure(this, indexSft, transformSft, attribute, filter.secondary)
        val iters = visibilityIter(indexSft) :+ indexValueIter :+ iter
        BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, ArrowIterator.kvsToFeatures(), Some(reduce), attrThreads, hasDuplicates = false)
      } else {
        // have to do a join against the record table
        joinQuery(ds, sft, filter, hints, hasDupes, singleAttrValueOnlyPlan)
      }
    } else if (hints.isStatsQuery) {
      val kvsToFeatures = KryoLazyStatsIterator.kvsToFeatures()
      if (descriptor.getIndexCoverage() == IndexCoverage.FULL) {
        val iter = KryoLazyStatsIterator.configure(sft, this, filter.secondary, hints, hasDupes)
        val iters = visibilityIter(sft) :+ iter
        val reduce = Some(StatsScan.reduceFeatures(sft, hints)(_))
        BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, kvsToFeatures, reduce, attrThreads, hasDuplicates = false)
      } else {
        // check to see if we can execute against the index values
        val indexSft = IndexValueEncoder.getIndexSft(sft)
        if (Try(Stat(indexSft, hints.getStatsQuery)).isSuccess &&
            filter.secondary.forall(IteratorTrigger.supportsFilter(indexSft, _))) {
          val iter = KryoLazyStatsIterator.configure(indexSft, this, filter.secondary, hints, hasDupes)
          val iters = visibilityIter(indexSft) :+ iter
          val reduce = Some(AccumuloAttributeIndex.reduceAttributeStats(sft, indexSft, transform, hints))
          BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, kvsToFeatures, reduce, attrThreads, hasDuplicates = false)
        } else {
          // have to do a join against the record table
          joinQuery(ds, sft, filter, hints, hasDupes, singleAttrValueOnlyPlan)
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
      val plan = singleAttrValueOnlyPlan(IndexValueEncoder.getIndexSft(sft), filter.secondary, hints.getTransform)
      val transformSft = hints.getTransformSchema.getOrElse {
        throw new IllegalStateException("Must have a transform for attribute key plus value scan")
      }
      // make sure we set table sharing - required for the iterator
      transformSft.setTableSharing(sft.isTableSharing)
      plan.copy(iterators = plan.iterators :+ KryoAttributeKeyValueIterator.configure(this, transformSft, attribute))
    } else {
      // have to do a join against the record table
      joinQuery(ds, sft, filter, hints, hasDupes, singleAttrValueOnlyPlan)
    }
  }

  /**
   * Gets a query plan comprised of a join against the record table. This is the slowest way to
   * execute a query, so we avoid it if possible.
   */
  def joinQuery(ds: AccumuloDataStore,
                sft: SimpleFeatureType,
                filter: AccumuloFilterStrategyType,
                hints: Hints,
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
    val recordIndex = {
      val indices = AccumuloFeatureIndex.indices(sft, mode = IndexMode.Read)
      indices.find(AccumuloFeatureIndex.RecordIndices.contains).getOrElse {
        throw new RuntimeException("Record index does not exist for join query")
      }
    }
    val (recordIter, kvsToFeatures, reduce) = if (hints.isArrowQuery) {
      val (iter, reduce) = ArrowIterator.configure(sft, recordIndex, ds.stats, filter.filter, ecqlFilter, hints, deduplicate = false)
      val kvs = ArrowIterator.kvsToFeatures()
      (Seq(iter), kvs, Some(reduce))
    } else if (hints.isStatsQuery) {
      val iter = Seq(KryoLazyStatsIterator.configure(sft, recordIndex, ecqlFilter, hints, deduplicate = false))
      val kvs = KryoLazyStatsIterator.kvsToFeatures()
      val reduce = Some(StatsScan.reduceFeatures(sft, hints)(_))
      (iter, kvs, reduce)
    } else if (hints.isBinQuery) {
      // TODO GEOMESA-822 we can use the aggregating iterator if the features are kryo encoded
      val iter = KryoLazyFilterTransformIterator.configure(sft, recordIndex, ecqlFilter, hints).toSeq
      val kvs = BinAggregatingIterator.nonAggregatedKvsToFeatures(sft, recordIndex, hints, SerializationType.KRYO)
      (iter, kvs, None)
    } else {
      val iter = KryoLazyFilterTransformIterator.configure(sft, recordIndex, ecqlFilter, hints).toSeq
      val kvs = recordIndex.entriesToFeatures(sft, hints.getReturnSft)
      (iter, kvs, None)
    }
    val visibilityIter = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   => Seq.empty
      case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(sft))
    }
    val recordIterators = visibilityIter ++ recordIter

    // function to join the attribute index scan results to the record table
    // have to pull the feature id from the row
    val prefix = sft.getTableSharingBytes
    val getId = getIdFromRow(sft)
    val getRowKey = RecordIndex.getRowKey(sft)
    val joinFunction: JoinFunction = (kv) => {
      val row = kv.getKey.getRow
      new AccRange(new Text(getRowKey(prefix, getId(row.getBytes, 0, row.getLength, null))))
    }

    val recordTable = recordIndex.getTableNames(sft, ds, None)
    val recordThreads = ds.config.recordThreads
    val recordRanges = Seq(new AccRange()) // this will get overwritten in the join method
    val joinQuery = BatchScanPlan(filter, recordTable, recordRanges, recordIterators, Seq.empty,
      kvsToFeatures, reduce, recordThreads, hasDupes)

    JoinPlan(filter, attributeScan.tables, attributeScan.ranges, attributeScan.iterators,
      attributeScan.columnFamilies, recordThreads, hasDupes, joinFunction, joinQuery)
  }

  override def getFilterStrategy(sft: SimpleFeatureType,
                                 filter: Filter,
                                 transform: Option[SimpleFeatureType]): Seq[AccumuloFilterStrategyType] = {
    import AttributeIndex.AllowJoinPlans
    val attributes = FilterHelper.propertyNames(filter, sft)
    val indexedAttributes = attributes.flatMap { a =>
      Option(sft.getDescriptor(a)).map(_.getIndexCoverage()).toSeq.collect {
        case coverage if coverage != IndexCoverage.NONE => (a, coverage)
      }
    }
    indexedAttributes.flatMap { case (attribute, coverage) =>
      val (primary, secondary) = FilterExtractingVisitor(filter, attribute, sft, attributeCheck)
      // check to see if we have a join plan and if so, that it's ok to return it
      lazy val joinCheck = coverage match {
        case IndexCoverage.FULL => true
        case IndexCoverage.JOIN => AllowJoinPlans.get || !requiresJoin(sft, attribute, secondary, transform)
      }
      if (primary.isDefined && joinCheck) {
        Seq(FilterStrategy(this, primary, secondary))
      } else {
        Seq.empty
      }
    }
  }

  override def getCost(sft: SimpleFeatureType,
                       stats: Option[GeoMesaStats],
                       filter: AccumuloFilterStrategyType,
                       transform: Option[SimpleFeatureType]): Long = {
    filter.primary match {
      case None => Long.MaxValue
      case Some(f) =>
        val statCost = for { stats <- stats; count <- stats.getCount(sft, f, exact = false) } yield {
          // account for cardinality and index coverage
          val attribute = FilterHelper.propertyNames(f, sft).head
          val descriptor = sft.getDescriptor(attribute)
          if (descriptor.getCardinality == Cardinality.HIGH) {
            count / 10 // prioritize attributes marked high-cardinality
          } else if (requiresJoin(sft, attribute, filter.secondary, transform)) {
            count * 10 // de-prioritize join queries, they are much more expensive
          } else {
            count
          }
        }
        statCost.getOrElse(indexBasedCost(sft, filter, transform))
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
  private def indexBasedCost(sft: SimpleFeatureType,
                             filter: AccumuloFilterStrategyType,
                             transform: Option[SimpleFeatureType]): Long = {
    // note: names should be only a single attribute
    val cost = for {
      f          <- filter.primary
      attribute  <- FilterHelper.propertyNames(f, sft).headOption
      descriptor <- Option(sft.getDescriptor(attribute))
      binding    =  descriptor.getType.getBinding
      bounds     =  FilterHelper.extractAttributeBounds(f, attribute, binding)
      if bounds.nonEmpty
    } yield {
      if (bounds.disjoint) { 0L } else {
        // join queries are much more expensive than non-join queries
        // TODO figure out the actual cost of each additional range...I'll make it 2
        val additionalRangeCost = 1
        val joinCost = 10
        val multiplier =
          if (descriptor.getIndexCoverage == IndexCoverage.FULL ||
              IteratorTrigger.canUseAttrIdxValues(sft, filter.secondary, transform) ||
              IteratorTrigger.canUseAttrKeysPlusValues(attribute, sft, filter.secondary, transform)) {
            1
          } else {
            joinCost + (additionalRangeCost * (bounds.values.length - 1))
          }

        // scale attribute cost by expected cardinality
        descriptor.getCardinality() match {
          case Cardinality.HIGH    => 1 * multiplier
          case Cardinality.UNKNOWN => 101 * multiplier
          case Cardinality.LOW     => Long.MaxValue
        }
      }
    }
    cost.getOrElse(Long.MaxValue)
  }

}

object AttributeQueryableIndex {

  /**
    * Checks for attribute filters that we can satisfy using the attribute index strategy
    *
    * @param filter filter to evaluate
    * @return true if we can process it as an attribute query
    */
  def attributeCheck(filter: Filter): Boolean = {
    filter match {
      case _: And | _: Or => true // note: implies further processing of children
      case _: PropertyIsEqualTo => true
      case _: PropertyIsBetween => true
      case _: PropertyIsGreaterThan | _: PropertyIsLessThan => true
      case _: PropertyIsGreaterThanOrEqualTo | _: PropertyIsLessThanOrEqualTo => true
      case _: During |  _: Before | _: After | _: TEquals => true
      case _: PropertyIsNull => true // we need this to be able to handle 'not null'
      case f: PropertyIsLike => likeEligible(f)
      case f: Not =>  f.getFilter.isInstanceOf[PropertyIsNull]
      case _ => false
    }
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
      if (descriptor.isList) { descriptor.getListType() } else { descriptor.getType.getBinding }
    }

    require(classOf[Comparable[_]].isAssignableFrom(binding), s"Attribute '$attribute' is not comparable")

    val fb = FilterHelper.extractAttributeBounds(filter, attribute, binding)
    if (fb.isEmpty) {
      throw new RuntimeException(s"Unhandled filter type in attribute strategy: ${filterToString(filter)}")
    }

    fb.values.map { bounds =>
      val range = bounds.bounds match {
        case (Some(lower), Some(upper)) =>
          if (lower == upper) {
            AttributeWritableIndex.equals(sft, index, lower, dates)
          } else if (lower + WILDCARD_SUFFIX == upper) {
            AttributeWritableIndex.prefix(sft, index, lower)
          } else {
            AttributeWritableIndex.between(sft, index, (lower, upper), dates, bounds.lower.inclusive || bounds.upper.inclusive)
          }
        case (Some(lower), None) =>
          if (bounds.lower.inclusive) {
            AttributeWritableIndex.gte(sft, index, lower, dates.map(_._1))
          } else {
            AttributeWritableIndex.gt(sft, index, lower, dates.map(_._1))
          }
        case (None, Some(upper)) =>
          if (bounds.upper.inclusive) {
            AttributeWritableIndex.lte(sft, index, upper, dates.map(_._2))
          } else {
            AttributeWritableIndex.lt(sft, index, upper, dates.map(_._2))
          }
        case (None, None) => // not null
          AttributeWritableIndex.all(sft, index)
      }

      PropertyBounds(attribute, bounds.bounds, range)
    }
  }
}

case class PropertyBounds(attribute: String, bounds: (Option[Any], Option[Any]), range: AccRange)
