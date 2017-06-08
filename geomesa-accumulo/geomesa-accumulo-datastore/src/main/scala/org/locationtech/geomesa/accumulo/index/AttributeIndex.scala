/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import com.google.common.collect.ImmutableSortedSet
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Mutation, Range}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.accumulo.AccumuloFilterStrategyType
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloQueryPlan.JoinFunction
import org.locationtech.geomesa.accumulo.index.encoders.IndexValueEncoder
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.features.SerializationType
import org.locationtech.geomesa.filter.{FilterHelper, andOption, partitionPrimarySpatials, partitionPrimaryTemporals}
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan}
import org.locationtech.geomesa.index.index.AttributeIndex
import org.locationtech.geomesa.index.iterators.ArrowBatchScan
import org.locationtech.geomesa.index.utils.KryoLazyStatsUtils
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.index.{IndexMode, VisibilityLevel}
import org.locationtech.geomesa.utils.stats.IndexCoverage.IndexCoverage
import org.locationtech.geomesa.utils.stats.{Cardinality, IndexCoverage, Stat}
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.util.Try

case object AttributeIndex extends AccumuloAttributeIndex {
  override val version: Int = 5
}

// secondary z-index
trait AccumuloAttributeIndex extends AccumuloFeatureIndex with AccumuloIndexAdapter
    with AttributeIndex[AccumuloDataStore, AccumuloFeature, Mutation, Range] with AttributeSplittable {

  import scala.collection.JavaConversions._

  type ScanPlanFn = (SimpleFeatureType, Option[Filter], Option[(String, SimpleFeatureType)]) => BatchScanPlan

  override val serializedWithId: Boolean = false

  override val hasPrecomputedBins: Boolean = false

  // hook to allow for not returning join plans
  val AllowJoinPlans = new ThreadLocal[Boolean] {
    override def initialValue: Boolean = true
  }

  override def configureSplits(sft: SimpleFeatureType, ds: AccumuloDataStore): Unit = {
    import scala.collection.JavaConversions._
    val table = getTableName(sft.getTypeName, ds)
    val splits = getSplits(sft).map(new Text(_)).toSet -- ds.tableOps.listSplits(table)
    if (splits.nonEmpty) {
      ds.tableOps.addSplits(table, ImmutableSortedSet.copyOf(splits.toArray))
    }
  }

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val getRows = getRowKeys(sft)
    val coverages = sft.getAttributeDescriptors.map(_.getIndexCoverage()).toArray
    (wf) => getRows(wf).map { case (i, r) => createInsert(r, wf, coverages(i)) }
  }

  override def remover(sft: SimpleFeatureType, ds: AccumuloDataStore): (AccumuloFeature) => Seq[Mutation] = {
    val getRows = getRowKeys(sft)
    val coverages = sft.getAttributeDescriptors.map(_.getIndexCoverage()).toArray
    (wf) => getRows(wf).map { case (i, r) => createDelete(r, wf, coverages(i)) }
  }

  protected def createInsert(row: Array[Byte], feature: AccumuloFeature, coverage: IndexCoverage): Mutation = {
    val mutation = new Mutation(row)
    val values = coverage match {
      case IndexCoverage.FULL => feature.fullValues
      case IndexCoverage.JOIN => feature.indexValues
    }
    values.foreach(v => mutation.put(v.cf, v.cq, v.vis, v.value))
    mutation
  }

  protected def createDelete(row: Array[Byte], feature: AccumuloFeature, coverage: IndexCoverage): Mutation = {
    val mutation = new Mutation(row)
    val values = coverage match {
      case IndexCoverage.FULL => feature.fullValues
      case IndexCoverage.JOIN => feature.indexValues
    }
    values.foreach(v => mutation.putDelete(v.cf, v.cq, v.vis))
    mutation
  }

  // we've overridden createInsert so this shouldn't be called, but it's still
  // part of the API so we can't remove it
  override protected def createInsert(row: Array[Byte], feature: AccumuloFeature): Mutation =
    throw new NotImplementedError("Should be using enhanced version with IndexCoverage")

  // we've overridden createDelete so this shouldn't be called, but it's still
  // part of the API so we can't remove it
  override protected def createDelete(row: Array[Byte], feature: AccumuloFeature): Mutation =
    throw new NotImplementedError("Should be using enhanced version with IndexCoverage")

  override protected def hasDuplicates(sft: SimpleFeatureType, filter: Option[Filter]): Boolean = {
    filter match {
      case None => false
      case Some(f) => FilterHelper.propertyNames(f, sft).exists(p => sft.getDescriptor(p).isMultiValued)
    }
  }

  override def getFilterStrategy(sft: SimpleFeatureType,
                                 filter: Filter,
                                 transform: Option[SimpleFeatureType]): Seq[AccumuloFilterStrategyType] = {
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    val strategies = super.getFilterStrategy(sft, filter, transform)
    // verify that it's ok to return join plans, and filter them out if not
    if (AllowJoinPlans.get) { strategies } else {
      strategies.filterNot { strategy =>
        val attributes = strategy.primary.toSeq.flatMap(FilterHelper.propertyNames(_, sft))
        val joins = attributes.filter(sft.getDescriptor(_).getIndexCoverage() == IndexCoverage.JOIN)
        joins.exists(requiresJoin(sft, _, strategy.secondary, transform))
      }
    }
  }

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: AccumuloDataStore,
                                  filter: FilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation],
                                  hints: Hints,
                                  ranges: Seq[Range],
                                  ecql: Option[Filter]): QueryPlan[AccumuloDataStore, AccumuloFeature, Mutation] = {
    if (ranges.isEmpty) { EmptyPlan(filter) } else {
      val primary = filter.primary.getOrElse {
        throw new IllegalStateException("Attribute index does not support Filter.INCLUDE")
      }
      val attributes = FilterHelper.propertyNames(primary, sft)
      // ensure we only have 1 prop we're working on
      if (attributes.length != 1) {
        throw new IllegalStateException(s"Expected one attribute in filter, got: ${attributes.mkString(", ")}")
      }

      val attribute = attributes.head
      val descriptor = sft.getDescriptor(attribute)

      descriptor.getIndexCoverage() match {
        case IndexCoverage.FULL => super.scanPlan(sft, ds, filter, hints, ranges, ecql)
        case IndexCoverage.JOIN => joinCoveragePlan(sft, ds, filter, hints, ranges, ecql, attribute)
        case coverage => throw new IllegalStateException(s"Expected index coverage, got $coverage")
      }
    }
  }

  private def joinCoveragePlan(sft: SimpleFeatureType,
                               ds: AccumuloDataStore,
                               filter: FilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation],
                               hints: Hints,
                               ranges: Seq[Range],
                               ecql: Option[Filter],
                               attribute: String): QueryPlan[AccumuloDataStore, AccumuloFeature, Mutation] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val table = getTableName(sft.getTypeName, ds)
    val numThreads = queryThreads(ds)
    val dedupe = hasDuplicates(sft, filter.primary)
    val cfs = Seq(AccumuloFeatureIndex.IndexColumnFamily)

    val indexSft = IndexValueEncoder.getIndexSft(sft)
    val transform = hints.getTransformSchema
    val sampling = hints.getSampling

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
      BatchScanPlan(filter, table, ranges, iters, cfs, kvsToFeatures, None, numThreads, dedupe)
    }

    if (hints.isBinQuery) {
      // check to see if we can execute against the index values
      if (indexSft.indexOf(hints.getBinTrackIdField) != -1 &&
          hints.getBinGeomField.forall(indexSft.indexOf(_) != -1) &&
          hints.getBinLabelField.forall(indexSft.indexOf(_) != -1) &&
          filter.secondary.forall(IteratorTrigger.supportsFilter(indexSft, _))) {
        val iter = BinAggregatingIterator.configureDynamic(indexSft, this, filter.secondary, hints, dedupe)
        val iters = visibilityIter(indexSft) :+ iter
        val kvsToFeatures = BinAggregatingIterator.kvsToFeatures()
        BatchScanPlan(filter, table, ranges, iters, cfs, kvsToFeatures, None, numThreads, dedupe)
      } else {
        // have to do a join against the record table
        joinQuery(ds, sft, indexSft, filter, hints, dedupe, singleAttrValueOnlyPlan)
      }
    } else if (hints.isArrowQuery) {
      lazy val dictionaryFields = hints.getArrowDictionaryFields
      lazy val providedDictionaries = hints.getArrowDictionaryEncodedValues
      lazy val dictionaries = ArrowBatchScan.createDictionaries(ds, sft, filter.filter, dictionaryFields, providedDictionaries)
      // check to see if we can execute against the index values
      if (IteratorTrigger.canUseAttrIdxValues(sft, ecql, transform)) {
        val (iter, reduce, kvsToFeatures) = if (hints.getArrowSort.isDefined ||
            hints.isArrowComputeDictionaries || dictionaryFields.forall(providedDictionaries.contains)) {
          val iter = ArrowBatchIterator.configure(indexSft, this, ecql, dictionaries, hints, dedupe)
          val reduce = Some(ArrowBatchScan.reduceFeatures(indexSft, hints, dictionaries)(_))
          (iter, reduce, ArrowBatchIterator.kvsToFeatures())
        } else {
          val iter = ArrowFileIterator.configure(indexSft, this, ecql, dictionaryFields, hints, dedupe)
          (iter, None, ArrowFileIterator.kvsToFeatures())
        }
        val iters = visibilityIter(indexSft) :+ iter
        BatchScanPlan(filter, table, ranges, iters, cfs, kvsToFeatures, reduce, numThreads, hasDuplicates = false)
      } else if (IteratorTrigger.canUseAttrKeysPlusValues(attribute, sft, ecql, transform)) {
        // we can use the index PLUS the value
        val transformSft = hints.getTransformSchema.getOrElse {
          throw new IllegalStateException("Must have a transform for attribute key plus value scan")
        }
        // make sure we set table sharing - required for the iterator
        transformSft.setTableSharing(sft.isTableSharing)
        // the key-value iter needs to run before the arrow iter so that the attribute is available to encode
        val (iter, reduce, kvsToFeatures) = if (hints.getArrowSort.isDefined ||
            hints.isArrowComputeDictionaries || dictionaryFields.forall(providedDictionaries.contains)) {
          val iter = ArrowBatchIterator.configure(transformSft, this, ecql, dictionaries, hints, dedupe)
          val reduce = Some(ArrowBatchScan.reduceFeatures(transformSft, hints, dictionaries)(_))
          (iter, reduce, ArrowBatchIterator.kvsToFeatures())
        } else {
          val iter = ArrowFileIterator.configure(transformSft, this, ecql, dictionaryFields, hints, dedupe)
          (iter, None, ArrowFileIterator.kvsToFeatures())
        }
        val keyValueIter = KryoAttributeKeyValueIterator.configure(this, transformSft, attribute, 23)
        val iters = visibilityIter(transformSft) :+ iter :+ keyValueIter
        BatchScanPlan(filter, table, ranges, iters, cfs, kvsToFeatures, reduce, numThreads, hasDuplicates = false)
      } else {
        // have to do a join against the record table
        joinQuery(ds, sft, indexSft, filter, hints, dedupe, singleAttrValueOnlyPlan)
      }
    } else if (hints.isDensityQuery) {
      // check to see if we can execute against the index values
      val weightIsAttribute = hints.getDensityWeight.exists(_ == attribute)
      if (filter.secondary.forall(IteratorTrigger.supportsFilter(indexSft, _)) &&
          (weightIsAttribute || hints.getDensityWeight.forall(indexSft.indexOf(_) != -1))) {
        val visIter = visibilityIter(indexSft)
        val iters = if (weightIsAttribute) {
          // create a transform sft with the attribute added
          val transform = {
            val builder = new SimpleFeatureTypeBuilder()
            builder.setNamespaceURI(null: String)
            builder.setName(indexSft.getTypeName + "--attr")
            builder.setAttributes(indexSft.getAttributeDescriptors)
            builder.add(sft.getDescriptor(attribute))
            if (indexSft.getGeometryDescriptor != null) {
              builder.setDefaultGeometry(indexSft.getGeometryDescriptor.getLocalName)
            }
            builder.setCRS(indexSft.getCoordinateReferenceSystem)
            val tmp = builder.buildFeatureType()
            tmp.getUserData.putAll(indexSft.getUserData)
            tmp
          }
          // priority needs to be between vis iter (21) and density iter (25)
          val keyValueIter = KryoAttributeKeyValueIterator.configure(this, transform, attribute, 23)
          val densityIter = KryoLazyDensityIterator.configure(transform, this, filter.secondary, hints, dedupe)
          visIter :+ keyValueIter :+ densityIter
        } else {
          visIter :+ KryoLazyDensityIterator.configure(indexSft, this, filter.secondary, hints, dedupe)
        }
        val kvsToFeatures = KryoLazyDensityIterator.kvsToFeatures()
        BatchScanPlan(filter, table, ranges, iters, cfs, kvsToFeatures, None, numThreads, hasDuplicates = false)
      } else {
        // have to do a join against the record table
        joinQuery(ds, sft, indexSft, filter, hints, dedupe, singleAttrValueOnlyPlan)
      }
    } else if (hints.isStatsIteratorQuery) {
      // check to see if we can execute against the index values
      if (Try(Stat(indexSft, hints.getStatsIteratorQuery)).isSuccess &&
          filter.secondary.forall(IteratorTrigger.supportsFilter(indexSft, _))) {
        val iter = KryoLazyStatsIterator.configure(indexSft, this, filter.secondary, hints, dedupe)
        val iters = visibilityIter(indexSft) :+ iter
        val kvsToFeatures = KryoLazyStatsIterator.kvsToFeatures(sft)
        val reduce = Some(KryoLazyStatsUtils.reduceFeatures(indexSft, hints)(_))
        BatchScanPlan(filter, table, ranges, iters, cfs, kvsToFeatures, reduce, numThreads, hasDuplicates = false)
      } else {
        // have to do a join against the record table
        joinQuery(ds, sft, indexSft, filter, hints, dedupe, singleAttrValueOnlyPlan)
      }
    } else if (IteratorTrigger.canUseAttrIdxValues(sft, filter.secondary, transform)) {
      // we can use the index value
      // transform has to be non-empty to get here and can only include items
      // in the index value (not the index keys aka the attribute indexed)
      singleAttrValueOnlyPlan(indexSft, filter.secondary, hints.getTransform)
    } else if (IteratorTrigger.canUseAttrKeysPlusValues(attribute, sft, filter.secondary, transform)) {
      // we can use the index PLUS the value
      val plan = singleAttrValueOnlyPlan(indexSft, filter.secondary, hints.getTransform)
      val transformSft = hints.getTransformSchema.getOrElse {
        throw new IllegalStateException("Must have a transform for attribute key plus value scan")
      }
      // make sure we set table sharing - required for the iterator
      transformSft.setTableSharing(sft.isTableSharing)
      plan.copy(iterators = plan.iterators :+ KryoAttributeKeyValueIterator.configure(this, transformSft, attribute))
    } else {
      // have to do a join against the record table
      joinQuery(ds, sft, indexSft, filter, hints, dedupe, singleAttrValueOnlyPlan)
    }
  }

  /**
    * Gets a query plan comprised of a join against the record table. This is the slowest way to
    * execute a query, so we avoid it if possible.
    */
  private def joinQuery(ds: AccumuloDataStore,
                        sft: SimpleFeatureType,
                        indexSft: SimpleFeatureType,
                        filter: AccumuloFilterStrategyType,
                        hints: Hints,
                        hasDupes: Boolean,
                        attributePlan: ScanPlanFn): JoinPlan = {
    import AccumuloFeatureIndex.{AttributeColumnFamily, FullColumnFamily}
    import org.locationtech.geomesa.filter.ff
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    // break out the st filter to evaluate against the attribute table
    val (stFilter, ecqlFilter) = filter.secondary.map { f =>
      val (geomFilters, otherFilters) = partitionPrimarySpatials(f, sft)
      val (temporalFilters, nonSTFilters) = partitionPrimaryTemporals(otherFilters, sft)
      (andOption(geomFilters ++ temporalFilters), andOption(nonSTFilters))
    }.getOrElse((None, None))

    // the scan against the attribute table
    val attributeScan = attributePlan(indexSft, stFilter, None)

    lazy val dictionaryFields = hints.getArrowDictionaryFields
    lazy val providedDictionaries = hints.getArrowDictionaryEncodedValues
    lazy val arrowDictionaries = ArrowBatchScan.createDictionaries(ds, sft, filter.filter, dictionaryFields, providedDictionaries)

    // apply any secondary filters or transforms against the record table
    val recordIndex = AccumuloFeatureIndex.indices(sft, IndexMode.Read).find(_.name == RecordIndex.name).getOrElse {
      throw new RuntimeException("Record index does not exist for join query")
    }
    val recordIter = if (hints.isArrowQuery) {
      if (hints.getArrowSort.isDefined || hints.isArrowComputeDictionaries ||
          dictionaryFields.forall(providedDictionaries.contains)) {
        Seq(ArrowBatchIterator.configure(sft, recordIndex, ecqlFilter, arrowDictionaries, hints, deduplicate = false))
      } else {
        Seq(ArrowFileIterator.configure(sft, recordIndex, ecqlFilter, dictionaryFields, hints, deduplicate = false))
      }
    } else if (hints.isStatsIteratorQuery) {
      Seq(KryoLazyStatsIterator.configure(sft, recordIndex, ecqlFilter, hints, deduplicate = false))
    } else if (hints.isDensityQuery) {
      Seq(KryoLazyDensityIterator.configure(sft, recordIndex, ecqlFilter, hints, deduplicate = false))
    } else {
      KryoLazyFilterTransformIterator.configure(sft, recordIndex, ecqlFilter, hints).toSeq
    }
    val (visibilityIter, recordCfs) = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   => (Seq.empty, Seq(FullColumnFamily))
      case VisibilityLevel.Attribute => (Seq(KryoVisibilityRowEncoder.configure(sft)), Seq(AttributeColumnFamily))
    }
    val recordIterators = visibilityIter ++ recordIter

    val (kvsToFeatures, reduce) = if (hints.isBinQuery) {
      // aggregating iterator wouldn't be very effective since each range is a single row
      (BinAggregatingIterator.nonAggregatedKvsToFeatures(sft, recordIndex, hints, SerializationType.KRYO), None)
    } else if (hints.isArrowQuery) {
      if (hints.isArrowComputeDictionaries) {
        val reduce = Some(ArrowBatchScan.reduceFeatures(hints.getTransformSchema.getOrElse(sft), hints, arrowDictionaries)(_))
        (ArrowBatchIterator.kvsToFeatures(), reduce)
      } else {
        (ArrowFileIterator.kvsToFeatures(), None)
      }
    } else if (hints.isStatsIteratorQuery) {
      (KryoLazyStatsIterator.kvsToFeatures(sft), Some(KryoLazyStatsUtils.reduceFeatures(sft, hints)(_)))
    } else if (hints.isDensityQuery) {
      (KryoLazyDensityIterator.kvsToFeatures(), None)
    } else {
      (recordIndex.entriesToFeatures(sft, hints.getReturnSft), None)
    }

    // function to join the attribute index scan results to the record table
    // have to pull the feature id from the row
    val prefix = sft.getTableSharingPrefix
    val getId = getIdFromRow(sft)
    val joinFunction: JoinFunction = (kv) => {
      val row = kv.getKey.getRow
      new Range(RecordIndex.getRowKey(prefix, getId(row.getBytes, 0, row.getLength)))
    }

    val recordTable = recordIndex.getTableName(sft.getTypeName, ds)
    val recordThreads = ds.config.recordThreads
    val recordRanges = Seq.empty // this will get overwritten in the join method
    val joinQuery = BatchScanPlan(filter, recordTable, recordRanges, recordIterators, recordCfs,
      kvsToFeatures, reduce, recordThreads, hasDupes)

    JoinPlan(filter, attributeScan.table, attributeScan.ranges, attributeScan.iterators,
      attributeScan.columnFamilies, recordThreads, hasDupes, joinFunction, joinQuery)
  }

  override def getCost(sft: SimpleFeatureType,
                       ds: Option[AccumuloDataStore],
                       filter: AccumuloFilterStrategyType,
                       transform: Option[SimpleFeatureType]): Long = {
    filter.primary match {
      case None => Long.MaxValue
      case Some(f) =>
        val statCost = for { ds <- ds; count <- ds.stats.getCount(sft, f, exact = false) } yield {
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

  /**
    * Does the query require a join against the record table, or can it be satisfied
    * in a single scan. Assumes that the attribute is indexed.
    *
    * @param sft simple feature type
    * @param attribute attribute being queried
    * @param filter non-attribute filter being evaluated, if any
    * @param transform transform being applied, if any
    * @return
    */
  def requiresJoin(sft: SimpleFeatureType,
                   attribute: String,
                   filter: Option[Filter],
                   transform: Option[SimpleFeatureType]): Boolean = {
    sft.getDescriptor(attribute).getIndexCoverage == IndexCoverage.JOIN &&
        !IteratorTrigger.canUseAttrIdxValues(sft, filter, transform) &&
        !IteratorTrigger.canUseAttrKeysPlusValues(attribute, sft, filter, transform)
  }
}

trait AttributeSplittable {
  def configureSplits(sft: SimpleFeatureType, ds: AccumuloDataStore): Unit
}
