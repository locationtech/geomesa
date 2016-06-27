/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Value, Range => AccRange}
import org.geotools.data.DataUtilities
import org.geotools.factory.Hints
import org.locationtech.geomesa.accumulo.data.stats.GeoMesaStats
import org.locationtech.geomesa.accumulo.data.tables.{AttributeTable, RecordTable}
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index.QueryPlanners.{FeatureFunction, JoinFunction}
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.filter._
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.index.VisibilityLevel
import org.locationtech.geomesa.utils.stats.{Cardinality, IndexCoverage, Stat}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter._
import org.opengis.filter.temporal.{After, Before, During, TEquals}

import scala.util.Try

class AttributeIdxStrategy(val filter: QueryFilter) extends Strategy with LazyLogging {

  import org.locationtech.geomesa.accumulo.index.AttributeIdxStrategy.{ScanPlanFn, attributeValueKvsToFeatures}

  /**
   * Perform scan against the Attribute Index Table and get an iterator returning records from the Record table
   */
  override def getQueryPlan(queryPlanner: QueryPlanner, hints: Hints, output: ExplainerOutputType): QueryPlan = {
    val sft = queryPlanner.sft

    val primary = filter.primary.getOrElse {
      throw new IllegalStateException("Attribute index does not support Filter.INCLUDE")
    }

    // pull out any dates from the filter to help narrow down the attribute ranges
    val dates = for {
      dtgField  <- sft.getDtgField
      secondary <- filter.secondary
      intervals = FilterHelper.extractIntervals(secondary, dtgField)
      if intervals.nonEmpty
    } yield {
      (intervals.map(_._1.getMillis).min, intervals.map(_._2.getMillis).max)
    }

    // TODO currently exclusive ANDs will return nothing - but if it's a list type it could actually match
    val bounds = AttributeIdxStrategy.getBounds(sft, primary, dates)

    if (bounds.isEmpty) {
      EmptyPlan(filter)
    } else {
      nonEmptyQueryPlan(queryPlanner, hints, bounds)
    }
  }

  private def nonEmptyQueryPlan(queryPlanner: QueryPlanner, hints: Hints, bounds: Seq[PropertyBounds]): QueryPlan = {
    val ds = queryPlanner.ds
    val sft = queryPlanner.sft

    val attribute = bounds.head.attribute
    val ranges = bounds.map(_.range)
    // ensure we only have 1 prop we're working on
    require(bounds.forall(_.attribute == attribute), "Found multiple attributes in attribute filter")

    val descriptor = sft.getDescriptor(attribute)
    val transform = hints.getTransformSchema
    val sampling = hints.getSampling
    val hasDupes = descriptor.isMultiValued

    val attrTable = ds.getTableName(sft.getTypeName, AttributeTable)
    val attrThreads = ds.getSuggestedThreads(sft.getTypeName, AttributeTable)

    def visibilityIter(schema: SimpleFeatureType): Seq[IteratorSetting] = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   => Seq.empty
      case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(schema))
    }

    // query against the attribute table
    val singleAttrValueOnlyPlan: ScanPlanFn = (schema, ecql, transform) => {
      val iter = KryoLazyFilterTransformIterator.configure(schema, ecql, transform, sampling)
      val iters = visibilityIter(schema) ++ iter.toSeq
      // need to use transform to convert key/values if it's defined
      val kvsToFeatures = queryPlanner.kvsToFeatures(sft, transform.map(_._2).getOrElse(schema), AttributeTable)
      BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, kvsToFeatures, attrThreads, hasDupes)
    }

    if (hints.isBinQuery) {
      if (descriptor.getIndexCoverage() == IndexCoverage.FULL) {
        // can apply the bin aggregating iterator directly to the sft
        val iter = BinAggregatingIterator.configureDynamic(sft, AttributeTable, filter.secondary, hints, hasDupes)
        val iters = visibilityIter(sft) :+ iter
        val kvsToFeatures = BinAggregatingIterator.kvsToFeatures()
        BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, kvsToFeatures, attrThreads, hasDupes)
      } else {
        // check to see if we can execute against the index values
        val indexSft = IndexValueEncoder.getIndexSft(sft)
        if (indexSft.indexOf(hints.getBinTrackIdField) != -1 &&
            hints.getBinLabelField.forall(indexSft.indexOf(_) != -1) &&
            filter.secondary.forall(IteratorTrigger.supportsFilter(indexSft, _))) {
          val iter = BinAggregatingIterator.configureDynamic(indexSft, AttributeTable, filter.secondary, hints, hasDupes)
          val iters = visibilityIter(indexSft) :+ iter
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
        val iter = KryoLazyStatsIterator.configure(sft, AttributeTable, filter.secondary, hints, hasDupes)
        val iters = visibilityIter(sft) :+ iter
        BatchScanPlan(filter, attrTable, ranges, iters, Seq.empty, kvsToFeatures, attrThreads, hasDuplicates = false)
      } else {
        // check to see if we can execute against the index values
        val indexSft = IndexValueEncoder.getIndexSft(sft)
        if (Try(Stat(indexSft, hints.getStatsIteratorQuery)).isSuccess &&
            filter.secondary.forall(IteratorTrigger.supportsFilter(indexSft, _))) {
          val iter = KryoLazyStatsIterator.configure(indexSft, AttributeTable, filter.secondary, hints, hasDupes)
          val iters = visibilityIter(indexSft) :+ iter
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
      val indexSft = IndexValueEncoder.getIndexSft(sft)
      val plan = singleAttrValueOnlyPlan(indexSft, filter.secondary, None)
      val transform = hints.getTransform.map(_._2).getOrElse {
        throw new IllegalStateException("Must have a transform for attribute key plus value scan")
      }
      val kvsToFeatures = attributeValueKvsToFeatures(sft, indexSft, transform, attribute)
      plan.copy(kvsToFeatures = kvsToFeatures)
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
    val recordIter = if (hints.isStatsIteratorQuery) {
      Seq(KryoLazyStatsIterator.configure(sft, RecordTable, ecqlFilter, hints, deduplicate = false))
    } else {
      KryoLazyFilterTransformIterator.configure(sft, ecqlFilter, hints).toSeq
    }
    val visibilityIter = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   => Seq.empty
      case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(sft))
    }
    val recordIterators = visibilityIter ++ recordIter

    val kvsToFeatures = if (hints.isBinQuery) {
      // TODO GEOMESA-822 we can use the aggregating iterator if the features are kryo encoded
      BinAggregatingIterator.nonAggregatedKvsToFeatures(sft, RecordTable, hints, queryPlanner.ds.getFeatureEncoding(sft))
    } else if (hints.isStatsIteratorQuery) {
      KryoLazyStatsIterator.kvsToFeatures(sft)
    } else {
      queryPlanner.kvsToFeatures(sft, hints.getReturnSft, RecordTable)
    }

    // function to join the attribute index scan results to the record table
    // have to pull the feature id from the row
    val prefix = sft.getTableSharingPrefix
    val getIdFromRow = AttributeTable.getIdFromRow(sft)
    val joinFunction: JoinFunction =
      (kv) => new AccRange(RecordTable.getRowKey(prefix, getIdFromRow(kv.getKey.getRow)))

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
    filter.primary match {
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
    val cost = for {
      f          <- filter.primary
      attribute  <- FilterHelper.propertyNames(f, sft).headOption
      descriptor <- Option(sft.getDescriptor(attribute))
      binding    =  descriptor.getType.getBinding
      bounds     <- FilterHelper.extractAttributeBounds(f, attribute, binding)
      if bounds.bounds.nonEmpty
    } yield {
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
          joinCost + (additionalRangeCost * (bounds.bounds.length - 1))
        }

      // scale attribute cost by expected cardinality
      descriptor.getCardinality() match {
        case Cardinality.HIGH    => 1 * multiplier
        case Cardinality.UNKNOWN => 101 * multiplier
        case Cardinality.LOW     => Long.MaxValue
      }
    }
    cost.getOrElse(Long.MaxValue)
  }

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

  def attributeValueKvsToFeatures(sft: SimpleFeatureType,
                                  indexSft: SimpleFeatureType,
                                  returnSft: SimpleFeatureType,
                                  attribute: String): FeatureFunction = {
    import scala.collection.JavaConversions._
    val attributeIndex = sft.indexOf(attribute)
    val returnIndex = returnSft.indexOf(attribute)
    val translateIndices =
      indexSft.getAttributeDescriptors.map(d => returnSft.indexOf(d.getLocalName)).zipWithIndex.filter(_._1 != -1)
    // Perform a projecting decode of the simple feature
    if (sft.getSchemaVersion < 9) {
      val kryoFeature = new KryoFeatureSerializer(indexSft).getReusableFeature
      (kv: Entry[Key, Value]) => {
        kryoFeature.setBuffer(kv.getValue.get)
        val sf = new ScalaSimpleFeature(kryoFeature.getID, returnSft)
        translateIndices.foreach { case (to, from) => sf.setAttribute(to, kryoFeature.getAttribute(from)) }
        val decoded = AttributeTable.decodeRow(sft, attributeIndex, kv.getKey.getRow.getBytes).get
        sf.setAttribute(returnIndex, decoded.asInstanceOf[AnyRef])
        QueryPlanner.applyVisibility(sf, kv.getKey)
        sf
      }
    } else {
      val kryoFeature = new KryoFeatureSerializer(indexSft, SerializationOptions.withoutId).getReusableFeature
      val getId = AttributeTable.getIdFromRow(sft)
      (kv: Entry[Key, Value]) => {
        kryoFeature.setBuffer(kv.getValue.get)
        val sf = new ScalaSimpleFeature(getId(kv.getKey.getRow), returnSft)
        translateIndices.foreach { case (to, from) => sf.setAttribute(to, kryoFeature.getAttribute(from)) }
        val decoded = AttributeTable.decodeRow(sft, attributeIndex, kv.getKey.getRow.getBytes).get
        sf.setAttribute(returnIndex, decoded.asInstanceOf[AnyRef])
        QueryPlanner.applyVisibility(sf, kv.getKey)
        sf
      }
    }
  }

  def tryMergeAttrStrategy(toMerge: QueryFilter, mergeTo: QueryFilter): QueryFilter = {
    // TODO this will be incorrect for multi-valued properties where we have an AND in the primary filter
    val leftAttributes = toMerge.primary.map(FilterHelper.propertyNames(_, null))
    val rightAttributes = mergeTo.primary.map(FilterHelper.propertyNames(_, null))

    val canMergePrimary = (leftAttributes, rightAttributes) match {
      case (Some(left), Some(right)) => left.length == 1 && right.length == 1 && left.head == right.head
      case _ => true
    }

    if (canMergePrimary && toMerge.secondary == mergeTo.secondary) {
      QueryFilter(mergeTo.strategy, orOption(toMerge.primary.toSeq ++ mergeTo.primary), mergeTo.secondary)
    } else {
      null
    }
  }
}

case class PropertyBounds(attribute: String, bounds: (Option[Any], Option[Any]), range: AccRange)
