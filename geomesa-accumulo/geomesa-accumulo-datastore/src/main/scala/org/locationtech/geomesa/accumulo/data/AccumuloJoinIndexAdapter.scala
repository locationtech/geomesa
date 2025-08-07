/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{Key, Range, Value}
import org.apache.hadoop.io.Text
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.accumulo.data.AccumuloIndexAdapter.AccumuloResultsToFeatures
import org.locationtech.geomesa.accumulo.data.AccumuloQueryPlan._
import org.locationtech.geomesa.accumulo.data.writer.ColumnFamilyMapper
import org.locationtech.geomesa.accumulo.index.AttributeJoinIndex
import org.locationtech.geomesa.accumulo.iterators.ArrowIterator.AccumuloArrowResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator.AccumuloBinResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators.DensityIterator.AccumuloDensityResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators.StatsIterator.AccumuloStatsResultsToFeatures
import org.locationtech.geomesa.accumulo.iterators._
import org.locationtech.geomesa.filter.{andOption, partitionPrimarySpatials, partitionPrimaryTemporals}
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.index.attribute.AttributeIndex
import org.locationtech.geomesa.index.index.id.IdIndex
import org.locationtech.geomesa.index.iterators.StatsScan
import org.locationtech.geomesa.index.planning.LocalQueryRunner.LocalTransformReducer
import org.locationtech.geomesa.utils.index.{ByteArrays, IndexMode, VisibilityLevel}
import org.locationtech.geomesa.utils.stats.Stat

import java.util.Map.Entry
import scala.util.Try

/**
  * Mixin trait to add join support to the normal attribute index class
  */
object AccumuloJoinIndexAdapter {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

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
  def createQueryPlan(
      ds: AccumuloDataStore,
      index: AttributeJoinIndex,
      filter: FilterStrategy,
      tables: Seq[String],
      ranges: Seq[org.apache.accumulo.core.data.Range],
      colFamily: Option[Text],
      schema: SimpleFeatureType,
      ecql: Option[Filter],
      hints: Hints,
      numThreads: Int): AccumuloQueryPlan = {

    // TODO seems like this should be using 'schema' here, which may be a reduced version of the indexSft due to col groups
    val indexSft = index.indexSft
    val transform = hints.getTransformSchema

    // for queries that don't require a join, creates a regular batch scan plan
    def plan(
        iters: Seq[IteratorSetting],
        kvsToFeatures: ResultsToFeatures[Entry[Key, Value]],
        reduce: Option[FeatureReducer]): BatchScanPlan = {
      // add the attribute-level vis iterator if necessary
      val iterators = visibilityIter(index) ++ iters
      val sort = hints.getSortFields
      val max = hints.getMaxFeatures
      val project = hints.getProjection
      BatchScanPlan(filter, tables, ranges, iterators, colFamily, kvsToFeatures, reduce, sort, max, project, numThreads)
    }

    // used when remote processing is disabled
    def localPlan(overrides: Option[Seq[IteratorSetting]] = None): BatchScanPlan = {
      val returnSchema = transform.getOrElse(indexSft)
      if (hints.isSkipReduce) {
        // override the return sft to reflect what we're actually returning,
        // since the bin sft is only created in the local reduce step
        hints.put(QueryHints.Internal.RETURN_SFT, returnSchema)
      }
      val iters = overrides.getOrElse(FilterTransformIterator.configure(indexSft, index, ecql, hints).toSeq)
      val localReducer = Some(new LocalTransformReducer(returnSchema, None, None, None, hints))
      plan(iters, AccumuloResultsToFeatures(index, returnSchema), localReducer)
    }

    val qp = if (hints.isBinQuery) {
      // check to see if we can execute against the index values
      if (indexSft.indexOf(hints.getBinTrackIdField) != -1 &&
          hints.getBinGeomField.forall(indexSft.indexOf(_) != -1) &&
          hints.getBinLabelField.forall(indexSft.indexOf(_) != -1) &&
          index.supportsFilter(ecql)) {
        if (ds.config.remote.bin) {
          val binIter = BinAggregatingIterator.configure(indexSft, index, ecql, hints)
          plan(Seq(binIter), new AccumuloBinResultsToFeatures(), None)
        } else {
          localPlan()
        }
      } else {
        // have to do a join against the record table
        createJoinPlan(ds, index, filter, tables, ranges, colFamily, ecql, hints)
      }
    } else if (hints.isArrowQuery) {
      // check to see if we can execute against the index values
      if (index.canUseIndexSchema(ecql, transform)) {
        if (ds.config.remote.arrow) {
          val (arrowIter, reduce) = ArrowIterator.configure(indexSft, index, ds.stats, filter.filter, ecql, hints)
          plan(Seq(arrowIter), new AccumuloArrowResultsToFeatures(), Some(reduce))
        } else {
          localPlan()
        }
      } else if (index.canUseIndexSchemaPlusKey(ecql, transform)) {
        val transformSft = transform.getOrElse {
          throw new IllegalStateException("Must have a transform for attribute key plus value scan")
        }
        // first filter and apply the transform
        val filterTransformIter = FilterTransformIterator.configure(indexSft, index, ecql, hints, 23).get
        // next add the attribute value from the row key
        val rowValueIter = AttributeKeyValueIterator.configure(index.asInstanceOf[AttributeIndex], transformSft, 24)
        if (ds.config.remote.arrow) {
          // clear the transforms as we've already accounted for them
          val newHints = new Hints(hints)
          newHints.clearTransforms()
          // finally apply the arrow iterator on the resulting features
          val (arrowIter, reduce) = ArrowIterator.configure(transformSft, index, ds.stats, None, None, newHints)
          val iters = Seq(filterTransformIter, rowValueIter, arrowIter)
          plan(iters, new AccumuloArrowResultsToFeatures(), Some(reduce))
        } else {
          localPlan(Some(Seq(filterTransformIter, rowValueIter)))
        }
      } else {
        // have to do a join against the record table
        createJoinPlan(ds, index, filter, tables, ranges, colFamily, ecql, hints)
      }
    } else if (hints.isDensityQuery) {
      // check to see if we can execute against the index values
      val weightIsAttribute = hints.getDensityWeight.contains(index.attributes.head)
      if (index.supportsFilter(ecql) && (weightIsAttribute || hints.getDensityWeight.forall(indexSft.indexOf(_) != -1))) {
        if (ds.config.remote.density) {
          val iters = if (weightIsAttribute) {
            // create a transform sft with the attribute added
            val transform = {
              val builder = new SimpleFeatureTypeBuilder()
              builder.setNamespaceURI(null: String)
              builder.setName(indexSft.getTypeName + "--attr")
              builder.setAttributes(indexSft.getAttributeDescriptors)
              builder.add(index.sft.getDescriptor(index.attributes.head))
              if (indexSft.getGeometryDescriptor != null) {
                builder.setDefaultGeometry(indexSft.getGeometryDescriptor.getLocalName)
              }
              builder.setCRS(indexSft.getCoordinateReferenceSystem)
              val tmp = builder.buildFeatureType()
              tmp.getUserData.putAll(indexSft.getUserData)
              tmp
            }
            // priority needs to be between vis iter (21) and density iter (25)
            val keyValueIter = AttributeKeyValueIterator.configure(index.asInstanceOf[AttributeIndex], transform, 23)
            val densityIter = DensityIterator.configure(transform, index, ecql, hints)
            Seq(keyValueIter, densityIter)
          } else {
            Seq(DensityIterator.configure(indexSft, index, ecql, hints))
          }
          plan(iters, new AccumuloDensityResultsToFeatures(), None)
        } else {
          localPlan()
        }
      } else {
        // have to do a join against the record table
        createJoinPlan(ds, index, filter, tables, ranges, colFamily, ecql, hints)
      }
    } else if (hints.isStatsQuery) {
      // check to see if we can execute against the index values
      if (Try(Stat(indexSft, hints.getStatsQuery)).isSuccess && index.supportsFilter(ecql)) {
        if (ds.config.remote.stats) {
          val statsIter = StatsIterator.configure(indexSft, index, ecql, hints)
          val reduce = Some(StatsScan.StatsReducer(indexSft, hints))
          plan(Seq(statsIter), new AccumuloStatsResultsToFeatures(), reduce)
        } else {
          localPlan()
        }
      } else {
        // have to do a join against the record table
        createJoinPlan(ds, index, filter, tables, ranges, colFamily, ecql, hints)
      }
    } else if (index.canUseIndexSchema(ecql, transform)) {
      // we can use the index value
      // transform has to be non-empty to get here and can only include items
      // in the index value (not the index keys aka the attribute indexed)
      val transformSft = transform.getOrElse {
        throw new IllegalStateException("Must have a transform for attribute value scan")
      }
      val fti = FilterTransformIterator.configure(indexSft, index, ecql, hints.getTransform, hints.getSampling)
      // need to use transform to convert key/values
      val toFeatures = AccumuloResultsToFeatures(index, transformSft)
      plan(fti.toSeq, toFeatures, None)
    } else if (index.canUseIndexSchemaPlusKey(ecql, transform)) {
      // we can use the index PLUS the value
      val transformSft = transform.getOrElse {
        throw new IllegalStateException("Must have a transform for attribute key plus value scan")
      }
      val fti = FilterTransformIterator.configure(indexSft, index, ecql, hints.getTransform, hints.getSampling)
      val iters = fti.toSeq :+ AttributeKeyValueIterator.configure(index.asInstanceOf[AttributeIndex], transformSft)
      // need to use transform to convert key/values
      val toFeatures = AccumuloResultsToFeatures(index, transformSft)
      plan(iters, toFeatures, None)
    } else {
      // have to do a join against the record table
      createJoinPlan(ds, index, filter, tables, ranges, colFamily, ecql, hints)
    }

    if (ranges.nonEmpty) { qp } else { EmptyPlan(qp.filter, qp.reducer) }
  }

  /**
    * Gets a query plan comprised of a join against the record table. This is the slowest way to
    * execute a query, so we avoid it if possible.
    */
  private def createJoinPlan(
      ds: AccumuloDataStore,
      index: AttributeJoinIndex,
      filter: FilterStrategy,
      tables: Seq[String],
      ranges: Seq[org.apache.accumulo.core.data.Range],
      colFamily: Option[Text],
      ecql: Option[Filter],
      hints: Hints): AccumuloQueryPlan = {
    import org.locationtech.geomesa.filter.ff
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    // apply any secondary filters or transforms against the record table
    val recordIndex = ds.manager.indices(index.sft, IndexMode.Read).find(_.name == IdIndex.name).getOrElse {
      throw new RuntimeException("Id index does not exist for join query: " +
          ds.manager.indices(index.sft, IndexMode.Read).map(_.identifier).mkString(", "))
    }

    // break out the st filter to evaluate against the attribute table
    val (stFilter, ecqlFilter) = ecql.map { f =>
      val (geomFilters, otherFilters) = partitionPrimarySpatials(f, index.sft)
      val (temporalFilters, nonSTFilters) = partitionPrimaryTemporals(otherFilters, index.sft)
      (andOption(geomFilters ++ temporalFilters), andOption(nonSTFilters))
    }.getOrElse((None, None))

    val (recordColFamily, recordSchema) = {
      val (cf, s) = ds.adapter.groups.group(index.sft, hints.getTransformDefinition, ecqlFilter)
      (Some(new Text(ColumnFamilyMapper(recordIndex)(cf))), s)
    }

    // since each range is a single row, it wouldn't be very efficient to do any aggregating scans
    // instead, handle them with the local query runner
    val resultSft = hints.getTransformSchema.getOrElse(index.sft)
    val recordIterators = {
      val recordIter = FilterTransformIterator.configure(recordSchema, recordIndex, ecqlFilter, hints).toSeq
      if (index.sft.getVisibilityLevel != VisibilityLevel.Attribute) { recordIter } else {
        Seq(KryoVisibilityRowEncoder.configure(recordSchema)) ++ recordIter
      }
    }
    val toFeatures = AccumuloResultsToFeatures(recordIndex, resultSft)
    val reducer = new LocalTransformReducer(resultSft, None, None, None, hints)
    if (hints.isSkipReduce) {
      // override the return sft to reflect what we're actually returning,
      // since the arrow sft is only created in the local reduce step
      hints.put(QueryHints.Internal.RETURN_SFT, resultSft)
    }

    val recordTables = recordIndex.getTablesForQuery(filter.filter)
    val recordThreads = ds.config.queries.recordThreads

    // function to join the attribute index scan results to the record table
    // have to pull the feature id from the row
    val joinFunction: JoinFunction = {
      val prefix = index.sft.getTableSharingBytes
      val idToBytes = GeoMesaFeatureIndex.idToBytes(index.sft)
      kv => {
        val row = kv.getKey.getRow
        new Range(new Text(ByteArrays.concat(prefix, idToBytes(index.getIdFromRow(row.getBytes, 0, row.getLength, null)))))
      }
    }

    val joinQuery = BatchScanPlan(filter, recordTables, Seq.empty, recordIterators, recordColFamily, toFeatures,
      Some(reducer), hints.getSortFields, hints.getMaxFeatures, hints.getProjection, recordThreads)

    val attributeIters = visibilityIter(index) ++
        FilterTransformIterator.configure(index.indexSft, index, stFilter, None, hints.getSampling).toSeq

    JoinPlan(filter, tables, ranges, attributeIters, colFamily, recordThreads, joinFunction, joinQuery)
  }

  private def visibilityIter(index: AttributeJoinIndex): Seq[IteratorSetting] = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    index.sft.getVisibilityLevel match {
      case VisibilityLevel.Feature   => Seq.empty
      case VisibilityLevel.Attribute => Seq(KryoVisibilityRowEncoder.configure(index.indexSft))
    }
  }
}
