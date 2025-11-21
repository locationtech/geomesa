/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument.Tags
import org.geotools.api.data.{DataStore, FeatureReader, Query, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.index.api.QueryPlan
import org.locationtech.geomesa.index.api.QueryPlan.ResultsToFeatures.IdentityResultsToFeatures
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.ArrowScan.DeltaReducer
import org.locationtech.geomesa.index.iterators.{DensityScan, StatsScan}
import org.locationtech.geomesa.index.planning.LocalQueryRunner._
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.index.view.MergedQueryRunner.Queryable
import org.locationtech.geomesa.metrics.micrometer.utils.TagUtils
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureOrdering, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.iterators.DeduplicatingSimpleFeatureIterator

/**
 * Query runner for merging results from multiple stores
 *
 * @param ds merged data store
 * @param stores delegate stores
 * @param deduplicate deduplicate the results between stores
 * @param parallel run scans in parallel (vs sequentially)
 */
class MergedQueryRunner(
    ds: HasGeoMesaStats,
    stores: Seq[(Queryable, Option[Filter])],
    deduplicate: Boolean,
    parallel: Boolean
  ) extends QueryRunner with LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  // query interceptors are handled by the individual data stores
  override protected val interceptors: QueryInterceptorFactory = QueryInterceptorFactory.empty()

  override protected def tags(typeName: String): Tags = TagUtils.typeNameTag(typeName).and("store", "merged", "catalog", "")

  override protected def getQueryPlans(sft: SimpleFeatureType, query: Query, explain: Explainer): Seq[QueryPlan] = {
    // TODO deduplicate arrow, bin, density queries...
    val hints = query.getHints
    val toFeatures = new IdentityResultsToFeatures(hints.getReturnSft)
    val plan = if (hints.isArrowQuery) {
      val scan = () => scanner(sft, query, org.locationtech.geomesa.arrow.ArrowEncodedSft, new ArrowProcessor(_, hints))
      val reducer = new DeltaReducer(hints.getTransformSchema.getOrElse(sft), hints, sorted = false)
      LocalQueryPlan(scan, toFeatures, Some(reducer), None, None, None)
    } else if (hints.isBinQuery) {
      if (query.getSortBy != null && !query.getSortBy.isEmpty) {
        logger.warn("Ignoring sort for BIN query")
      }
      val scan = () => scanner(sft, query, BinaryOutputEncoder.BinEncodedSft, new BinProcessor(_, hints))
      LocalQueryPlan(scan, toFeatures, None, None, None, None)
    } else if (hints.isDensityQuery) {
      val scan = () => scanner(sft, query, DensityScan.DensitySft, new DensityProcessor(_, hints))
      LocalQueryPlan(scan, toFeatures, None, None, None, None)
    } else if (hints.isStatsQuery) {
      val scan = () => scanner(sft, query, StatsScan.StatsSft, new StatsProcessor(_, hints))
      LocalQueryPlan(scan, toFeatures, Some(StatsScan.StatsReducer(sft, hints)), None, None, None)
    } else {
      // we assume the delegate stores can handle normal transforms/etc appropriately
      val scanner = () => {
        val readers = getReaders(sft, query).map(CloseableIterator(_))
        val deduped = if (deduplicate) {
          // we re-use the feature id cache across readers
          val cache = scala.collection.mutable.HashSet.empty[String]
          readers.map(new DeduplicatingSimpleFeatureIterator(_, cache))
        } else {
          readers
        }
        hints.getSortFields match {
          case None => doParallelScan(deduped)
          // the delegate stores should sort their results, so we can sort merge them
          case Some(sort) => new SortedMergeIterator(deduped)(SimpleFeatureOrdering(hints.getReturnSft, sort))
        }
      }
      LocalQueryPlan(scanner, toFeatures, None, None, hints.getMaxFeatures, None)
    }
    Seq(plan)
  }

  // query each delegate store
  private def getReaders(sft: SimpleFeatureType, query: Query): Seq[FeatureReader[SimpleFeatureType, SimpleFeature]] = {
    stores.map { case (store, filter) =>
      val copy = new Query(query) // make sure to coy the hints so they aren't shared
      // suppress the reduce step for gm stores so that we can do the merge here
      copy.getHints.put(QueryHints.Internal.SKIP_REDUCE, java.lang.Boolean.TRUE) // TODO only for arrow and bin?
      store.getFeatureReader(mergeFilter(sft, copy, filter), Transaction.AUTO_COMMIT)
    }
  }

  private def scanner(
      sft: SimpleFeatureType,
      query: Query,
      encodedType: SimpleFeatureType,
      fallbackProcessor: SimpleFeatureType => LocalScanProcessor): CloseableIterator[SimpleFeature] = {
    val readers = getReaders(sft, query).map { reader =>
      val schema = reader.getFeatureType
      if (schema == encodedType) {
        // processing has been handled by the store already
        CloseableIterator(reader)
      } else {
        // the store just returned normal features, do the processing here
        val copy = SimpleFeatureTypes.immutable(schema, sft.getUserData) // copy default dtg, etc if necessary
        fallbackProcessor(copy).apply(CloseableIterator(reader))
      }
    }
    doParallelScan(readers)
  }

  private def doParallelScan(readers: Seq[CloseableIterator[SimpleFeature]]): CloseableIterator[SimpleFeature] = {
    if (parallel) {
      // not truly parallel but should kick them all off up front
      readers.par.foreach(_.hasNext)
    }
    CloseableIterator(readers.iterator).flatMap(i => i)
  }
}

object MergedQueryRunner {

  trait Queryable {
    def getFeatureReader(q: Query, t: Transaction): FeatureReader[SimpleFeatureType, SimpleFeature]
  }

  case class DataStoreQueryable(ds: DataStore) extends Queryable {
    override def getFeatureReader(q: Query, t: Transaction): FeatureReader[SimpleFeatureType, SimpleFeature] =
      ds.getFeatureReader(q, t)
  }
}
