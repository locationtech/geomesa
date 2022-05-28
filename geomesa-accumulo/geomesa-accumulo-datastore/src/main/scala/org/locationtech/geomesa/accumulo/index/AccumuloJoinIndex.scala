/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index

import java.util.Map.Entry

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Range, Value}
import org.apache.hadoop.io.Text
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.accumulo.data.AccumuloIndexAdapter.AccumuloResultsToFeatures
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan._
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloIndexAdapter, AccumuloQueryPlan}
import org.locationtech.geomesa.accumulo.iterators.ArrowIterator.AccumuloArrowResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator.AccumuloBinResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators.DensityIterator.AccumuloDensityResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators.StatsIterator.AccumuloStatsResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.filter.{FilterHelper, andOption, partitionPrimarySpatials, partitionPrimaryTemporals}
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.attribute.{AttributeIndex, AttributeIndexKey, AttributeIndexValues}
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.planning.LocalQueryRunner.{ArrowDictionaryHook, LocalTransformReducer}
import org.locationtech.geomesa.utils.index.{ByteArrays, IndexMode, VisibilityLevel}
import org.locationtech.geomesa.utils.stats.Stat
import org.opengis.feature.simple.SimpleFeatureType
import org.opengis.filter.Filter

import scala.util.Try

/**
  * Mixin trait to add join support to the normal attribute index class
  */
trait AccumuloJoinIndex extends GeoMesaFeatureIndex[AttributeIndexValues[Any], AttributeIndexKey] {

  this: AttributeIndex =>

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  import scala.collection.JavaConverters._

  private val attribute = attributes.head
  private val attributeIndex = sft.indexOf(attribute)
  private val descriptor = sft.getDescriptor(attributeIndex)
  private val binding = descriptor.getType.getBinding
  val indexSft = IndexValueEncoder.getIndexSft(sft)

  override val name: String = JoinIndex.name
  override val identifier: String = GeoMesaFeatureIndex.identifier(name, version, attributes)

  abstract override def getFilterStrategy(
      filter: Filter,
      transform: Option[SimpleFeatureType]): Option[FilterStrategy] = {
    super.getFilterStrategy(filter, transform).flatMap { strategy =>
      // verify that it's ok to return join plans, and filter them out if not
      if (!requiresJoin(strategy.secondary, transform)) {
        Some(strategy)
      } else if (!JoinIndex.AllowJoinPlans.get) {
        None
      } else {
        val primary = strategy.primary.getOrElse(Filter.INCLUDE)
        val bounds = FilterHelper.extractAttributeBounds(primary, attribute, binding)
        val joinMultiplier = 9f + bounds.values.length // 10 plus 1 per additional range being scanned
        val multiplier = strategy.costMultiplier * joinMultiplier
        Some(FilterStrategy(strategy.index, strategy.primary, strategy.secondary, strategy.temporal, multiplier))
      }
    }
  }

  /**
    * Create a query plan against a join index - if possible, will use the reduced index-values to scan
    * the single table, otherwise will require a join against the id index
    *
    * @param filter filter strategy
    * @param tables tables to scan
    * @param ranges ranges to scan
    * @param colFamily column family to scan, optional
    * @param schema simple feature schema being scanned
    * @param ecql secondary push-down predicates
    * @param hints query hints
    * @param numThreads query threads
    * @return
    */
  def createQueryPlan(filter: FilterStrategy,
                      tables: Seq[String],
                      ranges: Seq[org.apache.accumulo.core.data.Range],
                      colFamily: Option[Text],
                      schema: SimpleFeatureType,
                      ecql: Option[Filter],
                      hints: Hints,
                      numThreads: Int): AccumuloQueryPlan = {

    lazy val sort = hints.getSortFields
    lazy val max = hints.getMaxFeatures
    lazy val project = hints.getProjection

    // for queries that don't require a join, creates a regular batch scan plan
    def plan(
        iters: Seq[IteratorSetting],
        kvsToFeatures: ResultsToFeatures[Entry[Key, Value]],
        reduce: Option[FeatureReducer]) =
      BatchScanPlan(filter, tables, ranges, iters, colFamily, kvsToFeatures, reduce, sort, max, project, numThreads)

    val transform = hints.getTransformSchema

    // used when remote processing is disabled
    lazy val returnSchema = hints.getTransformSchema.getOrElse(indexSft)
    lazy val fti = visibilityIter(indexSft) ++ FilterTransformIterator.configure(indexSft, this, ecql, hints).toSeq
    lazy val resultsToFeatures = AccumuloResultsToFeatures(this, returnSchema)
    lazy val localReducer = {
      val arrowHook = Some(ArrowDictionaryHook(ds.stats, filter.filter))
      Some(new LocalTransformReducer(returnSchema, None, None, None, hints, arrowHook))
    }

    val qp = if (hints.isBinQuery) {
      // check to see if we can execute against the index values
      if (indexSft.indexOf(hints.getBinTrackIdField) != -1 &&
          hints.getBinGeomField.forall(indexSft.indexOf(_) != -1) &&
          hints.getBinLabelField.forall(indexSft.indexOf(_) != -1) &&
          supportsFilter(ecql)) {
        if (ds.asInstanceOf[AccumuloDataStore].config.remote.bin) {
          val iter = BinAggregatingIterator.configure(indexSft, this, ecql, hints)
          val iters = visibilityIter(indexSft) :+ iter
          plan(iters, new AccumuloBinResultsToFeatures(), None)
        } else {
          if (hints.isSkipReduce) {
            // override the return sft to reflect what we're actually returning,
            // since the bin sft is only created in the local reduce step
            hints.hints.put(QueryHints.Internal.RETURN_SFT, returnSchema)
          }
          plan(fti, resultsToFeatures, localReducer)
        }
      } else {
        // have to do a join against the record table
        createJoinPlan(filter, tables, ranges, colFamily, schema, ecql, hints, numThreads)
      }
    } else if (hints.isArrowQuery) {
      // check to see if we can execute against the index values
      if (canUseIndexSchema(ecql, transform)) {
        if (ds.asInstanceOf[AccumuloDataStore].config.remote.bin) {
          val (iter, reduce) = ArrowIterator.configure(indexSft, this, ds.stats, filter.filter, ecql, hints)
          val iters = visibilityIter(indexSft) :+ iter
          plan(iters, new AccumuloArrowResultsToFeatures(), Some(reduce))
        } else {
          if (hints.isSkipReduce) {
            // override the return sft to reflect what we're actually returning,
            // since the arrow sft is only created in the local reduce step
            hints.hints.put(QueryHints.Internal.RETURN_SFT, returnSchema)
          }
          plan(fti, resultsToFeatures, localReducer)
        }
      } else if (canUseIndexSchemaPlusKey(ecql, transform)) {
        val transformSft = transform.getOrElse {
          throw new IllegalStateException("Must have a transform for attribute key plus value scan")
        }
        // first filter and apply the transform
        val filterTransformIter = FilterTransformIterator.configure(indexSft, this, ecql, hints, 23).get
        // clear the transforms as we've already accounted for them
        hints.clearTransforms()
        // next add the attribute value from the row key
        val rowValueIter = AttributeKeyValueIterator.configure(this, transformSft, 24)
        if (ds.asInstanceOf[AccumuloDataStore].config.remote.bin) {
          // finally apply the arrow iterator on the resulting features
          val (iter, reduce) = ArrowIterator.configure(transformSft, this, ds.stats, None, None, hints)
          val iters = visibilityIter(indexSft) ++ Seq(filterTransformIter, rowValueIter, iter)
          plan(iters, new AccumuloArrowResultsToFeatures(), Some(reduce))
        } else {
          if (hints.isSkipReduce) {
            // override the return sft to reflect what we're actually returning,
            // since the arrow sft is only created in the local reduce step
            hints.hints.put(QueryHints.Internal.RETURN_SFT, returnSchema)
          }
          plan(fti, resultsToFeatures, localReducer)
        }
      } else {
        // have to do a join against the record table
        createJoinPlan(filter, tables, ranges, colFamily, schema, ecql, hints, numThreads)
      }
    } else if (hints.isDensityQuery) {
      // check to see if we can execute against the index values
      val weightIsAttribute = hints.getDensityWeight.contains(attribute)
      if (supportsFilter(ecql) && (weightIsAttribute || hints.getDensityWeight.forall(indexSft.indexOf(_) != -1))) {
        if (ds.asInstanceOf[AccumuloDataStore].config.remote.bin) {
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
            val keyValueIter = AttributeKeyValueIterator.configure(this, transform, 23)
            val densityIter = DensityIterator.configure(transform, this, ecql, hints)
            visIter :+ keyValueIter :+ densityIter
          } else {
            visIter :+ DensityIterator.configure(indexSft, this, ecql, hints)
          }
          plan(iters, new AccumuloDensityResultsToFeatures(), None)
        } else {
          if (hints.isSkipReduce) {
            // override the return sft to reflect what we're actually returning,
            // since the density sft is only created in the local reduce step
            hints.hints.put(QueryHints.Internal.RETURN_SFT, returnSchema)
          }
          plan(fti, resultsToFeatures, localReducer)
        }
      } else {
        // have to do a join against the record table
        createJoinPlan(filter, tables, ranges, colFamily, schema, ecql, hints, numThreads)
      }
    } else if (hints.isStatsQuery) {
      // check to see if we can execute against the index values
      if (Try(Stat(indexSft, hints.getStatsQuery)).isSuccess && supportsFilter(ecql)) {
        if (ds.asInstanceOf[AccumuloDataStore].config.remote.bin) {
          val iter = StatsIterator.configure(indexSft, this, ecql, hints)
          val iters = visibilityIter(indexSft) :+ iter
          val reduce = Some(StatsScan.StatsReducer(indexSft, hints))
          plan(iters, new AccumuloStatsResultsToFeatures(), reduce)
        } else {
          if (hints.isSkipReduce) {
            // override the return sft to reflect what we're actually returning,
            // since the stats sft is only created in the local reduce step
            hints.hints.put(QueryHints.Internal.RETURN_SFT, returnSchema)
          }
          plan(fti, resultsToFeatures, localReducer)
        }
      } else {
        // have to do a join against the record table
        createJoinPlan(filter, tables, ranges, colFamily, schema, ecql, hints, numThreads)
      }
    } else if (canUseIndexSchema(ecql, transform)) {
      // we can use the index value
      // transform has to be non-empty to get here and can only include items
      // in the index value (not the index keys aka the attribute indexed)
      val transformSft = transform.getOrElse {
        throw new IllegalStateException("Must have a transform for attribute value scan")
      }
      val iter = FilterTransformIterator.configure(indexSft, this, ecql, hints.getTransform, hints.getSampling)
      // add the attribute-level vis iterator if necessary
      val iters = visibilityIter(schema) ++ iter.toSeq
      // need to use transform to convert key/values
      val toFeatures = AccumuloResultsToFeatures(this, transformSft)
      plan(iters, toFeatures, None)
    } else if (canUseIndexSchemaPlusKey(ecql, transform)) {
      // we can use the index PLUS the value
      val transformSft = transform.getOrElse {
        throw new IllegalStateException("Must have a transform for attribute key plus value scan")
      }
      val iter = FilterTransformIterator.configure(indexSft, this, ecql, hints.getTransform, hints.getSampling)
      // add the attribute-level vis iterator if necessary
      val iters = visibilityIter(schema) ++ iter.toSeq :+ AttributeKeyValueIterator.configure(this, transformSft)
      // need to use transform to convert key/values
      val toFeatures = AccumuloResultsToFeatures(this, transformSft)
      plan(iters, toFeatures, None)
    } else {
      // have to do a join against the record table
      createJoinPlan(filter, tables, ranges, colFamily, schema, ecql, hints, numThreads)
    }

    if (ranges.nonEmpty) { qp } else { EmptyPlan(qp.filter, qp.reducer) }
  }

  /**
    * Gets a query plan comprised of a join against the record table. This is the slowest way to
    * execute a query, so we avoid it if possible.
    */
  private def createJoinPlan(filter: FilterStrategy,
                             tables: Seq[String],
                             ranges: Seq[org.apache.accumulo.core.data.Range],
                             colFamily: Option[Text],
                             schema: SimpleFeatureType,
                             ecql: Option[Filter],
                             hints: Hints,
                             numThreads: Int): AccumuloQueryPlan = {
    import org.locationtech.geomesa.filter.ff
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    // apply any secondary filters or transforms against the record table
    val recordIndex = ds.manager.indices(sft, IndexMode.Read).find(_.name == IdIndex.name).getOrElse {
      throw new RuntimeException("Id index does not exist for join query: " +
          ds.manager.indices(sft, IndexMode.Read).map(_.identifier).mkString(", "))
    }

    // break out the st filter to evaluate against the attribute table
    val (stFilter, ecqlFilter) = ecql.map { f =>
      val (geomFilters, otherFilters) = partitionPrimarySpatials(f, sft)
      val (temporalFilters, nonSTFilters) = partitionPrimaryTemporals(otherFilters, sft)
      (andOption(geomFilters ++ temporalFilters), andOption(nonSTFilters))
    }.getOrElse((None, None))

    val (recordColFamily, recordSchema) = {
      val (cf, s) = ds.adapter.groups.group(sft, hints.getTransformDefinition, ecqlFilter)
      (Some(new Text(AccumuloIndexAdapter.mapColumnFamily(recordIndex)(cf))), s)
    }

    // since each range is a single row, it wouldn't be very efficient to do any aggregating scans
    // instead, handle them with the local query runner
    val resultSft = hints.getTransformSchema.getOrElse(sft)
    val recordIterators = {
      val recordIter = FilterTransformIterator.configure(recordSchema, recordIndex, ecqlFilter, hints).toSeq
      if (sft.getVisibilityLevel != VisibilityLevel.Attribute) { recordIter } else {
        Seq(KryoVisibilityRowEncoder.configure(recordSchema)) ++ recordIter
      }
    }
    val toFeatures = AccumuloResultsToFeatures(recordIndex, resultSft)
    val hook = Some(ArrowDictionaryHook(ds.stats, filter.filter))
    val reducer = new LocalTransformReducer(resultSft, None, None, None, hints, hook)

    val recordTables = recordIndex.getTablesForQuery(filter.filter)
    val recordThreads = ds.asInstanceOf[AccumuloDataStore].config.queries.recordThreads

    // function to join the attribute index scan results to the record table
    // have to pull the feature id from the row
    val joinFunction: JoinFunction = {
      val prefix = sft.getTableSharingBytes
      val idToBytes = GeoMesaFeatureIndex.idToBytes(sft)
      kv => {
        val row = kv.getKey.getRow
        new Range(new Text(ByteArrays.concat(prefix, idToBytes(getIdFromRow(row.getBytes, 0, row.getLength, null)))))
      }
    }

    val joinQuery = BatchScanPlan(filter, recordTables, Seq.empty, recordIterators, recordColFamily, toFeatures,
      Some(reducer), hints.getSortFields, hints.getMaxFeatures, hints.getProjection, recordThreads)

    val attributeIters = visibilityIter(indexSft) ++
        FilterTransformIterator.configure(indexSft, this, stFilter, None, hints.getSampling).toSeq

    JoinPlan(filter, tables, ranges, attributeIters, colFamily, recordThreads, joinFunction, joinQuery)
  }

  private def visibilityIter(schema: SimpleFeatureType): Seq[IteratorSetting] = sft.getVisibilityLevel match {
    case VisibilityLevel.Feature   => Seq.empty
    case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(schema))
  }

  /**
    * Does the query require a join against the record table, or can it be satisfied
    * in a single scan
    *
    * @param filter non-attribute filter being evaluated, if any
    * @param transform transform being applied, if any
    * @return
    */
  private def requiresJoin(filter: Option[Filter], transform: Option[SimpleFeatureType]): Boolean =
    !canUseIndexSchema(filter, transform) && !canUseIndexSchemaPlusKey(filter, transform)

  /**
    * Determines if the given filter and transform can operate on index encoded values.
    */
  private def canUseIndexSchema(filter: Option[Filter], transform: Option[SimpleFeatureType]): Boolean = {
    // verify that transform *does* exist and only contains fields in the index sft,
    // and that filter *does not* exist or can be fulfilled by the index sft
    supportsTransform(transform) && supportsFilter(filter)
  }

  /**
    * Determines if the given filter and transform can operate on index encoded values
    * in addition to the values actually encoded in the attribute index keys
    */
  private def canUseIndexSchemaPlusKey(filter: Option[Filter], transform: Option[SimpleFeatureType]): Boolean = {
    transform.exists { t =>
      val attributes = t.getAttributeDescriptors.asScala.map(_.getLocalName)
      attributes.forall(a => a == attribute || indexSft.indexOf(a) != -1) && supportsFilter(filter)
    }
  }

  private def supportsTransform(transform: Option[SimpleFeatureType]): Boolean =
    transform.exists(_.getAttributeDescriptors.asScala.map(_.getLocalName).forall(indexSft.indexOf(_) != -1))

  private def supportsFilter(filter: Option[Filter]): Boolean =
    filter.forall(FilterHelper.propertyNames(_, sft).forall(indexSft.indexOf(_) != -1))
}
