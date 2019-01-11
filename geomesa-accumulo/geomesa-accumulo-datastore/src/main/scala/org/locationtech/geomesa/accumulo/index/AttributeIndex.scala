/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Map.Entry

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Mutation, Range, Value}
import org.apache.hadoop.io.Text
import org.geotools.factory.Hints
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.locationtech.geomesa.accumulo.AccumuloFilterStrategyType
import org.locationtech.geomesa.accumulo.data._
import org.locationtech.geomesa.accumulo.index.AccumuloAttributeIndex.{AttributeSplittable, JoinScanConfig}
import org.locationtech.geomesa.accumulo.index.AccumuloIndexAdapter.ScanConfig
import org.locationtech.geomesa.accumulo.index.AccumuloQueryPlan.JoinFunction
import org.locationtech.geomesa.accumulo.index.encoders.IndexValueEncoder
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.features.SerializationType
import org.locationtech.geomesa.filter.{FilterHelper, andOption, partitionPrimarySpatials, partitionPrimaryTemporals}
import org.locationtech.geomesa.index.api.{FilterStrategy, QueryPlan}
import org.locationtech.geomesa.index.conf.partition.TablePartition
import org.locationtech.geomesa.index.index.ShardStrategy
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor
import org.locationtech.geomesa.utils.index.{ByteArrays, IndexMode, VisibilityLevel}
import org.locationtech.geomesa.utils.stats.IndexCoverage.IndexCoverage
import org.locationtech.geomesa.utils.stats.{Cardinality, IndexCoverage, Stat}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.util.Try

case object AttributeIndex extends AccumuloAttributeIndex {
  override val version: Int = 7
}

// secondary z-index
trait AccumuloAttributeIndex extends AccumuloFeatureIndex with AccumuloIndexAdapter
    with AttributeIndex[AccumuloDataStore, AccumuloFeature, Mutation, Range, ScanConfig] with AttributeSplittable {

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConversions._
  import scala.collection.JavaConverters._

  type ScanConfigFn = (SimpleFeatureType, Option[Filter], Option[(String, SimpleFeatureType)]) => ScanConfig

  override val serializedWithId: Boolean = false

  override val hasPrecomputedBins: Boolean = false

  // hook to allow for not returning join plans
  val AllowJoinPlans: ThreadLocal[Boolean] = new ThreadLocal[Boolean] {
    override def initialValue: Boolean = true
  }

  override def configureSplits(sft: SimpleFeatureType, ds: AccumuloDataStore, partition: Option[String]): Unit = {
    val target = getSplits(sft, partition).map(new Text(_)).toSet
    getTableNames(sft, ds, partition).foreach { table =>
      val splits = target -- ds.tableOps.listSplits(table)
      if (splits.nonEmpty) {
        ds.tableOps.addSplits(table, new java.util.TreeSet(splits.asJava))
      }
    }
  }

  override def writer(sft: SimpleFeatureType, ds: AccumuloDataStore): AccumuloFeature => Seq[Mutation] = {
    val sharing = sft.getTableSharingBytes
    val shards = shardStrategy(sft)
    val coverages = sft.getAttributeDescriptors.map(_.getIndexCoverage()).toArray
    val toIndexKey = keySpace.toIndexKeyBytes(sft)
    tieredKeySpace(sft) match {
      case None       => mutator(sharing, shards, toIndexKey, coverages, createValuesInsert)
      case Some(tier) => mutator(sharing, shards, toIndexKey, tier.toIndexKeyBytes(sft), coverages, createValuesInsert)
    }
  }

  override def remover(sft: SimpleFeatureType, ds: AccumuloDataStore): AccumuloFeature => Seq[Mutation] = {
    val sharing = sft.getTableSharingBytes
    val shards = shardStrategy(sft)
    val coverages = sft.getAttributeDescriptors.map(_.getIndexCoverage()).toArray
    val toIndexKey = keySpace.toIndexKeyBytes(sft, lenient = true)
    tieredKeySpace(sft) match {
      case None       => mutator(sharing, shards, toIndexKey, coverages, createValuesDelete)
      case Some(tier) => mutator(sharing, shards, toIndexKey, tier.toIndexKeyBytes(sft, lenient = true), coverages, createValuesDelete)
    }
  }

  private def createValuesInsert(row: Array[Byte], values: Seq[AccumuloFeature.RowValue]): Mutation = {
    val mutation = new Mutation(row)
    values.foreach(v => mutation.put(v.cf, v.cq, v.vis, v.value))
    mutation
  }

  private def createValuesDelete(row: Array[Byte], values: Seq[AccumuloFeature.RowValue]): Mutation = {
    val mutation = new Mutation(row)
    values.foreach(v => mutation.putDelete(v.cf, v.cq, v.vis))
    mutation
  }

  /**
    * Mutator for a single key space
    *
    * @param sharing table sharing bytes
    * @param shards sharding
    * @param toIndexKey function to create the primary index key
    * @param operation operation (create or delete)
    * @param feature feature to operate on
    * @return
    */
  private def mutator(sharing: Array[Byte],
                      shards: ShardStrategy,
                      toIndexKey: (Seq[Array[Byte]], SimpleFeature, Array[Byte]) => Seq[Array[Byte]],
                      coverages: Array[IndexCoverage],
                      operation: (Array[Byte], Seq[AccumuloFeature.RowValue]) => Mutation)
                     (feature: AccumuloFeature): Seq[Mutation] = {
    val shard = shards(feature)
    val iOffset = sharing.length + shard.length
    toIndexKey(Seq(sharing, shard), feature.feature, feature.idBytes).map { row =>
      val values = coverages(ByteArrays.readShort(row, iOffset)) match {
        case IndexCoverage.FULL => feature.fullValues
        case IndexCoverage.JOIN => feature.indexValues
      }
      operation.apply(row, values)
    }
  }

  /**
    * Mutator function for two key spaces
    *
    * @param sharing table sharing bytes
    * @param shards sharding
    * @param toIndexKey function to create the primary index key
    * @param toTieredKey function to create a secondary, tiered index key
    * @param operation operation (create or delete)
    * @param feature feature to operate on
    * @return
    */
  private def mutator(sharing: Array[Byte],
                      shards: ShardStrategy,
                      toIndexKey: (Seq[Array[Byte]], SimpleFeature, Array[Byte]) => Seq[Array[Byte]],
                      toTieredKey: (Seq[Array[Byte]], SimpleFeature, Array[Byte]) => Seq[Array[Byte]],
                      coverages: Array[IndexCoverage],
                      operation: (Array[Byte], Seq[AccumuloFeature.RowValue]) => Mutation)
                     (feature: AccumuloFeature): Seq[Mutation] = {
    val shard = shards(feature)
    val iOffset = sharing.length + shard.length
    for (tier1 <- toIndexKey(Seq(sharing, shard), feature.feature, Array.empty);
         row   <- toTieredKey(Seq(tier1), feature.feature, feature.idBytes)) yield {
      val values = coverages(ByteArrays.readShort(row, iOffset)) match {
        case IndexCoverage.FULL => feature.fullValues
        case IndexCoverage.JOIN => feature.indexValues
      }
      operation.apply(row, values)
    }
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
        joins.exists(AccumuloAttributeIndex.requiresJoin(sft, _, strategy.secondary, transform))
      }
    }
  }

  override protected def scanPlan(sft: SimpleFeatureType,
                                  ds: AccumuloDataStore,
                                  filter: FilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation],
                                  config: ScanConfig): QueryPlan[AccumuloDataStore, AccumuloFeature, Mutation] = {
    if (config.ranges.isEmpty) { EmptyPlan(filter) } else {
      config match {
        case c: JoinScanConfig => joinScanPlan(sft, ds, filter, c)
        case _ => super.scanPlan(sft, ds, filter, config)
      }
    }
  }

  override protected def scanConfig(sft: SimpleFeatureType,
                                    ds: AccumuloDataStore,
                                    filter: FilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation],
                                    ranges: Seq[Range],
                                    ecql: Option[Filter],
                                    hints: Hints): ScanConfig = {
    val primary = filter.primary.getOrElse {
      throw new IllegalStateException("Attribute index does not support Filter.INCLUDE")
    }
    val attributes = FilterHelper.propertyNames(primary, sft)
    // ensure we only have 1 prop we're working on
    if (attributes.lengthCompare(1) != 0) {
      throw new IllegalStateException(s"Expected one attribute in filter, got: ${attributes.mkString(", ")}")
    }

    val attribute = attributes.head
    val descriptor = sft.getDescriptor(attribute)

    descriptor.getIndexCoverage() match {
      case IndexCoverage.FULL => super.scanConfig(sft, ds, filter, ranges, ecql, hints)
      case IndexCoverage.JOIN => joinCoverageConfig(sft, ds, filter, ranges, ecql, attribute, hints)
      case coverage => throw new IllegalStateException(s"Expected index coverage, got $coverage")
    }
  }

  private def joinCoverageConfig(sft: SimpleFeatureType,
                                 ds: AccumuloDataStore,
                                 filter: FilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation],
                                 ranges: Seq[Range],
                                 ecql: Option[Filter],
                                 attribute: String,
                                 hints: Hints): ScanConfig = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val dedupe = hasDuplicates(sft, filter.primary)
    val cf = AccumuloColumnGroups.IndexColumnFamily

    val indexSft = IndexValueEncoder.getIndexSft(sft)
    val transform = hints.getTransformSchema
    val sampling = hints.getSampling

    def visibilityIter(schema: SimpleFeatureType): Seq[IteratorSetting] = sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   => Seq.empty
      case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(schema))
    }

    // query against the attribute table
    val singleAttrValueOnlyConfig: ScanConfigFn = (schema, ecql, transform) => {
      val iter = KryoLazyFilterTransformIterator.configure(schema, this, ecql, transform, sampling)
      val iters = visibilityIter(schema) ++ iter.toSeq
      // need to use transform to convert key/values if it's defined
      val kvsToFeatures = entriesToFeatures(sft, transform.map(_._2).getOrElse(schema))
      ScanConfig(ranges, cf, iters, kvsToFeatures, None, dedupe)
    }

    if (hints.isBinQuery) {
      // check to see if we can execute against the index values
      if (indexSft.indexOf(hints.getBinTrackIdField) != -1 &&
          hints.getBinGeomField.forall(indexSft.indexOf(_) != -1) &&
          hints.getBinLabelField.forall(indexSft.indexOf(_) != -1) &&
          ecql.forall(IteratorTrigger.supportsFilter(indexSft, _))) {
        val iter = BinAggregatingIterator.configureDynamic(indexSft, this, ecql, hints, dedupe)
        val iters = visibilityIter(indexSft) :+ iter
        val kvsToFeatures = BinAggregatingIterator.kvsToFeatures()
        ScanConfig(ranges, cf, iters, kvsToFeatures, None, duplicates = false)
      } else {
        // have to do a join against the record table
        joinConfig(ds, sft, indexSft, filter, ecql, hints, dedupe, singleAttrValueOnlyConfig)
      }
    } else if (hints.isArrowQuery) {
      // check to see if we can execute against the index values
      if (IteratorTrigger.canUseAttrIdxValues(sft, ecql, transform)) {
        val (iter, reduce) = ArrowIterator.configure(indexSft, this, ds.stats, filter.filter, ecql, hints, dedupe)
        val iters = visibilityIter(indexSft) :+ iter
        ScanConfig(ranges, cf, iters, ArrowIterator.kvsToFeatures(), Some(reduce), duplicates = false)
      } else if (IteratorTrigger.canUseAttrKeysPlusValues(attribute, sft, ecql, transform)) {
        val transformSft = transform.getOrElse {
          throw new IllegalStateException("Must have a transform for attribute key plus value scan")
        }
        hints.clearTransforms() // clear the transforms as we've already accounted for them
        // note: ECQL is handled below, so we don't pass it to the arrow iter here
        val (iter, reduce) = ArrowIterator.configure(transformSft, this, ds.stats, filter.filter, None, hints, dedupe)
        val indexValueIter = AttributeIndexValueIterator.configure(this, indexSft, transformSft, attribute, ecql)
        val iters = visibilityIter(indexSft) :+ indexValueIter :+ iter
        ScanConfig(ranges, cf, iters, ArrowIterator.kvsToFeatures(), Some(reduce), duplicates = false)
      } else {
        // have to do a join against the record table
        joinConfig(ds, sft, indexSft, filter, ecql, hints, dedupe, singleAttrValueOnlyConfig)
      }
    } else if (hints.isDensityQuery) {
      // noinspection ExistsEquals
      // check to see if we can execute against the index values
      val weightIsAttribute = hints.getDensityWeight.exists(_ == attribute)
      if (ecql.forall(IteratorTrigger.supportsFilter(indexSft, _)) &&
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
          val densityIter = KryoLazyDensityIterator.configure(transform, this, ecql, hints, dedupe)
          visIter :+ keyValueIter :+ densityIter
        } else {
          visIter :+ KryoLazyDensityIterator.configure(indexSft, this, ecql, hints, dedupe)
        }
        val kvsToFeatures = KryoLazyDensityIterator.kvsToFeatures()
        ScanConfig(ranges, cf, iters, kvsToFeatures, None, duplicates = false)
      } else {
        // have to do a join against the record table
        joinConfig(ds, sft, indexSft, filter, ecql, hints, dedupe, singleAttrValueOnlyConfig)
      }
    } else if (hints.isStatsQuery) {
      // check to see if we can execute against the index values
      if (Try(Stat(indexSft, hints.getStatsQuery)).isSuccess &&
          ecql.forall(IteratorTrigger.supportsFilter(indexSft, _)) &&
          transform.forall(IteratorTrigger.supportsTransform(indexSft, _))) {
        val iter = KryoLazyStatsIterator.configure(indexSft, this, ecql, hints, dedupe)
        val iters = visibilityIter(indexSft) :+ iter
        val kvsToFeatures = KryoLazyStatsIterator.kvsToFeatures()
        val reduce = Some(AccumuloAttributeIndex.reduceAttributeStats(sft, indexSft, transform, hints))
        ScanConfig(ranges, cf, iters, kvsToFeatures, reduce, duplicates = false)
      } else {
        // have to do a join against the record table
        joinConfig(ds, sft, indexSft, filter, ecql, hints, dedupe, singleAttrValueOnlyConfig)
      }
    } else if (IteratorTrigger.canUseAttrIdxValues(sft, ecql, transform)) {
      // we can use the index value
      // transform has to be non-empty to get here and can only include items
      // in the index value (not the index keys aka the attribute indexed)
      singleAttrValueOnlyConfig(indexSft, ecql, hints.getTransform)
    } else if (IteratorTrigger.canUseAttrKeysPlusValues(attribute, sft, ecql, transform)) {
      // we can use the index PLUS the value
      val plan = singleAttrValueOnlyConfig(indexSft, ecql, hints.getTransform)
      val transformSft = hints.getTransformSchema.getOrElse {
        throw new IllegalStateException("Must have a transform for attribute key plus value scan")
      }
      // make sure we set table sharing - required for the iterator
      transformSft.setTableSharing(sft.isTableSharing)
      plan.copy(iterators = plan.iterators :+ KryoAttributeKeyValueIterator.configure(this, transformSft, attribute))
    } else {
      // have to do a join against the record table
      joinConfig(ds, sft, indexSft, filter, ecql, hints, dedupe, singleAttrValueOnlyConfig)
    }
  }

  /**
    * Gets a query plan comprised of a join against the record table. This is the slowest way to
    * execute a query, so we avoid it if possible.
    */
  private def joinConfig(ds: AccumuloDataStore,
                        sft: SimpleFeatureType,
                        indexSft: SimpleFeatureType,
                        filter: AccumuloFilterStrategyType,
                        ecql: Option[Filter],
                        hints: Hints,
                        hasDupes: Boolean,
                        attributePlan: ScanConfigFn): JoinScanConfig = {
    import org.locationtech.geomesa.filter.ff
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    // break out the st filter to evaluate against the attribute table
    val (stFilter, ecqlFilter) = ecql.map { f =>
      val (geomFilters, otherFilters) = partitionPrimarySpatials(f, sft)
      val (temporalFilters, nonSTFilters) = partitionPrimaryTemporals(otherFilters, sft)
      (andOption(geomFilters ++ temporalFilters), andOption(nonSTFilters))
    }.getOrElse((None, None))

    // the scan against the attribute table
    val attributeScan = attributePlan(indexSft, stFilter, None)

    // apply any secondary filters or transforms against the record table
    val recordIndex = {
      val indices = AccumuloFeatureIndex.indices(sft, mode = IndexMode.Read)
      indices.find(AccumuloFeatureIndex.RecordIndices.contains).getOrElse {
        throw new RuntimeException("Record index does not exist for join query")
      }
    }

    val isAttributeLevelVis = sft.getVisibilityLevel == VisibilityLevel.Attribute

    val (recordColFamily, recordSchema) = hints.getTransformDefinition match {
      case _  if isAttributeLevelVis => (AccumuloColumnGroups.AttributeColumnFamily, sft)
      case Some(tdefs)               =>  AccumuloColumnGroups.group(sft, tdefs, ecql)
      case None                      => (AccumuloColumnGroups.default, sft)
    }

    val (recordIter, reduce, kvsToFeatures) = if (hints.isArrowQuery) {
      val (iter, reduce) = ArrowIterator.configure(recordSchema, recordIndex, ds.stats, filter.filter, ecqlFilter, hints, deduplicate = false)
      (Seq(iter), Some(reduce), ArrowIterator.kvsToFeatures())
    } else if (hints.isStatsQuery) {
      val iter = KryoLazyStatsIterator.configure(recordSchema, recordIndex, ecqlFilter, hints, deduplicate = false)
      val reduce = StatsScan.reduceFeatures(recordSchema, hints)(_)
      (Seq(iter), Some(reduce), KryoLazyStatsIterator.kvsToFeatures())
    } else if (hints.isDensityQuery) {
      val iter = KryoLazyDensityIterator.configure(recordSchema, recordIndex, ecqlFilter, hints, deduplicate = false)
      (Seq(iter), None, KryoLazyDensityIterator.kvsToFeatures())
    } else if (hints.isBinQuery) {
      // aggregating iterator wouldn't be very effective since each range is a single row
      val iter = KryoLazyFilterTransformIterator.configure(recordSchema, recordIndex, ecqlFilter, hints).toSeq
      val kvsToFeatures = BinAggregatingIterator.nonAggregatedKvsToFeatures(recordSchema, recordIndex, hints, SerializationType.KRYO)
      (iter, None, kvsToFeatures)
    } else {
      val iter = KryoLazyFilterTransformIterator.configure(recordSchema, recordIndex, ecqlFilter, hints).toSeq
      (iter, None, recordIndex.entriesToFeatures(recordSchema, hints.getReturnSft))
    }

    val recordIterators = if (!isAttributeLevelVis) { recordIter } else {
      Seq(KryoVisibilityRowEncoder.configure(recordSchema)) ++ recordIter
    }

    new JoinScanConfig(recordIndex, recordIterators, recordColFamily, attributeScan.ranges, attributeScan.columnFamily,
      attributeScan.iterators, kvsToFeatures, reduce, hasDupes)
  }

  private def joinScanPlan(sft: SimpleFeatureType,
                           ds: AccumuloDataStore,
                           filter: FilterStrategy[AccumuloDataStore, AccumuloFeature, Mutation],
                           config: JoinScanConfig): QueryPlan[AccumuloDataStore, AccumuloFeature, Mutation] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val (tables, recordTables) = TablePartition(ds, sft) match {
      case None => (getTableNames(sft, ds, None), config.recordIndex.getTableNames(sft, ds, None))
      case Some(tp) =>
        val partitions = filter.filter.map(tp.partitions).getOrElse(Seq.empty)
        if (partitions.nonEmpty) {
          partitions.flatMap { p =>
            for {
              t <- getTableNames(sft, ds, Some(p))
              r <- config.recordIndex.getTableNames(sft, ds, Some(p))
            } yield {
              (t, r)
            }
          }.unzip
        } else {
          (getTableNames(sft, ds, None), config.recordIndex.getTableNames(sft, ds, None))
        }
    }

    val recordThreads = ds.config.recordThreads

    // function to join the attribute index scan results to the record table
    // have to pull the feature id from the row
    val prefix = sft.getTableSharingBytes
    val getId = getIdFromRow(sft)
    val getRowKey = RecordIndex.getRowKey(sft)
    val joinFunction: JoinFunction = kv => {
      val row = kv.getKey.getRow
      new Range(new Text(getRowKey(prefix, getId(row.getBytes, 0, row.getLength, null))))
    }

    val recordRanges = Seq.empty // this will get overwritten in the join method
    val joinQuery = BatchScanPlan(filter, recordTables, recordRanges, config.recordIterators,
      Seq(config.recordColumnFamily), config.entriesToFeatures, config.reduce, recordThreads, hasDuplicates = false)

    JoinPlan(filter, tables, config.ranges, config.iterators,
      Seq(config.columnFamily), recordThreads, config.duplicates, joinFunction, joinQuery)
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
    * full index equals query:
    *   high cardinality - 10
    *   unknown cardinality - 101
    * full index range query:
    *   high cardinality - 100
    *   unknown cardinality - 1010
    * full index not null query:
    *   high cardinality - 500
    *   unknown cardinality - 5050
    * join index equals query:
    *   high cardinality - 100
    *   unknown cardinality - 1010
    * join index range query:
    *   high cardinality - 1000
    *   unknown cardinality - 10100
    * join index not null query:
    *   high cardinality - 5000
    *   unknown cardinality - 50500
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
        // high cardinality attributes and equality queries are prioritized
        // joins and not-null queries are de-prioritized

        val baseCost = descriptor.getCardinality() match {
          case Cardinality.HIGH    => 10
          case Cardinality.UNKNOWN => 101
          case Cardinality.LOW     => 1000
        }
        val secondaryIndexMultiplier = {
          if (!bounds.forall(_.isBounded)) { 50 } // not null
          else if (bounds.precise && !bounds.exists(_.isRange)) { 1 } // equals
          else { 10 } // range
        }
        val joinMultiplier = {
          val notJoin = descriptor.getIndexCoverage == IndexCoverage.FULL ||
            IteratorTrigger.canUseAttrIdxValues(sft, filter.secondary, transform) ||
            IteratorTrigger.canUseAttrKeysPlusValues(attribute, sft, filter.secondary, transform)
          if (notJoin) { 1 } else { 10 + (bounds.values.length - 1) }
        }

        baseCost * secondaryIndexMultiplier * joinMultiplier
      }
    }
    cost.getOrElse(Long.MaxValue)
  }
}

object AccumuloAttributeIndex {

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

  /**
    * Handles transforming stats run against the attribute index back into the expected
    * simple feature type
    *
    * @param sft original simple feature type
    * @param indexSft index simple feature type
    * @param transform transform, if any
    * @param hints query hints
    * @return
    */
  def reduceAttributeStats(sft: SimpleFeatureType,
                           indexSft: SimpleFeatureType,
                           transform: Option[SimpleFeatureType],
                           hints: Hints): CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    if (transform.isDefined || !hints.isStatsEncode) {
      // returned stats will be in the transform schema or in json
      StatsScan.reduceFeatures(indexSft, hints)(_)
    } else {
      // we have to transform back into the original sft after operating on the index values
      val decode = StatsScan.decodeStat(indexSft)
      val encode = StatsScan.encodeStat(sft)
      iter => {
        StatsScan.reduceFeatures(indexSft, hints)(iter).map { feature =>
          // we can create a new stat with the correct sft, then add the result
          // this should set the correct metadata but preserve the underlying data
          val stat = Stat(sft, hints.getStatsQuery)
          stat += decode(feature.getAttribute(0).asInstanceOf[String])
          feature.setAttribute(0, encode(stat))
          feature
        }
      }
    }
  }

  trait AttributeSplittable {
    def configureSplits(sft: SimpleFeatureType, ds: AccumuloDataStore, partition: Option[String]): Unit
  }

  class JoinScanConfig(val recordIndex: AccumuloFeatureIndex,
                       val recordIterators: Seq[IteratorSetting],
                       val recordColumnFamily: Text,
                       ranges: Seq[Range],
                       columnFamily: Text,
                       iterators: Seq[IteratorSetting],
                       entriesToFeatures: Entry[Key, Value] => SimpleFeature,
                       reduce: Option[CloseableIterator[SimpleFeature] => CloseableIterator[SimpleFeature]],
                       duplicates: Boolean)
      extends ScanConfig(ranges, columnFamily, iterators, entriesToFeatures, reduce, duplicates)
}