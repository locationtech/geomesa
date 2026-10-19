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
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.QueryPlan
import org.locationtech.geomesa.index.api.QueryPlan.ResultsToFeatures.IdentityResultsToFeatures
import org.locationtech.geomesa.index.api.QueryPlan.{FeatureReducer, ResultsToFeatures}
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.iterators.ArrowScan.DeltaReducer
import org.locationtech.geomesa.index.iterators.{DensityScan, StatsScan}
import org.locationtech.geomesa.index.planning.LocalQueryRunner._
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.index.utils.Reprojection.QueryReferenceSystems
import org.locationtech.geomesa.index.view.MergedDataStoreView.{MergedSource, TaskDecorator}
import org.locationtech.geomesa.index.view.MergedQueryRunner.{MergedQueryPlan, ParallelScanIterator, Queryable}
import org.locationtech.geomesa.metrics.micrometer.utils.TagUtils
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.concurrent.CachedThreadPool
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureOrdering, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.locationtech.geomesa.utils.iterators.DeduplicatingSimpleFeatureIterator

import java.time.Instant
import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

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
    stores: Seq[MergedSource[Queryable]],
    deduplicate: Boolean,
    parallel: Boolean,
    decorator: TaskDecorator = new TaskDecorator(None),
  ) extends QueryRunner with LazyLogging {

  import MergedQueryRunner.openReaders
  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  // query interceptors are handled by the individual data stores
  override protected val interceptors: QueryInterceptorFactory = QueryInterceptorFactory.empty()

  override protected def tags(typeName: String): Tags = TagUtils.typeNameTag(typeName).and("store", "merged", "catalog", "")

  override protected def getQueryPlans(sft: SimpleFeatureType, query: Query, explain: Explainer): Seq[QueryPlan] = {
    // TODO deduplicate arrow, bin, density queries...
    val hints = query.getHints
    val toFeatures = new IdentityResultsToFeatures(hints.getReturnSft)
    val plan = if (hints.isArrowQuery) {
      val scan = () => scanner(sft, query, org.locationtech.geomesa.arrow.ArrowEncodedSft, new ArrowProcessor(_, hints), copyFeatures = true)
      val reducer = new DeltaReducer(hints.getTransformSchema.getOrElse(sft), hints, sorted = false)
      MergedQueryPlan(scan, toFeatures, Some(reducer), hints.getMaxFeatures)
    } else if (hints.isBinQuery) {
      if (query.getSortBy != null && !query.getSortBy.isEmpty) {
        logger.warn("Ignoring sort for BIN query")
      }
      val scan = () => scanner(sft, query, BinaryOutputEncoder.BinEncodedSft, new BinProcessor(_, hints))
      MergedQueryPlan(scan, toFeatures, None, hints.getMaxFeatures)
    } else if (hints.isDensityQuery) {
      val scan = () => scanner(sft, query, DensityScan.DensitySft, new DensityProcessor(_, hints))
      MergedQueryPlan(scan, toFeatures, None, hints.getMaxFeatures)
    } else if (hints.isStatsQuery) {
      val scan = () => scanner(sft, query, StatsScan.StatsSft, new StatsProcessor(_, hints))
      val reducer = StatsScan.StatsReducer(sft, hints)
      MergedQueryPlan(scan, toFeatures, Some(reducer), hints.getMaxFeatures)
    } else {
      // we assume the delegate stores can handle normal transforms/etc appropriately
      val scanner = () => {
        val readers = getReaders(query).map(CloseableIterator(_))
        val sorted = hints.getSortFields match {
          case None => doParallelScan(readers)
            // the delegate stores should sort their results, so we can sort merge them
          case Some(sort) => new SortedMergeIterator(openReaders(readers))(SimpleFeatureOrdering(hints.getReturnSft, sort))
        }
        val deduped = if (deduplicate) { new DeduplicatingSimpleFeatureIterator(sorted) } else { sorted }
        deduped
      }
      MergedQueryPlan(scanner, toFeatures, None, hints.getMaxFeatures)
    }
    Seq(plan)
  }

  // query each delegate store - note, it's important that this is an iterator (lazily evaluated) in order for things to be
  // cleaned up correctly
  private def getReaders(query: Query): Iterator[FeatureReader[SimpleFeatureType, SimpleFeature]] = {
    val now = Instant.now() // shared current time passed to all stores for consistency in bounds calculations
    stores.iterator.map { store =>
      val copy = new Query(query) // make sure to coy the hints so they aren't shared
      // suppress the reduce step for gm stores so that we can do the merge here
      copy.getHints.put(QueryHints.Internal.SKIP_REDUCE, java.lang.Boolean.TRUE)
      copy.setFilter(store.filter(query.getTypeName, query.getFilter, now))
      store.source.getFeatureReader(copy, Transaction.AUTO_COMMIT)
    }
  }

  private def scanner(
      sft: SimpleFeatureType,
      query: Query,
      encodedType: SimpleFeatureType,
      fallbackProcessor: SimpleFeatureType => LocalScanProcessor,
      copyFeatures: Boolean = false): CloseableIterator[SimpleFeature] = {
    val readers = getReaders(query).map { reader =>
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
    doParallelScan(readers, copyFeatures)
  }

  private def doParallelScan(
      readers: Iterator[CloseableIterator[SimpleFeature]],
      copyFeatures: Boolean = false): CloseableIterator[SimpleFeature] = {
    if (parallel) {
      new ParallelScanIterator(openReaders(readers), decorator, copyFeatures, 32768)
    } else {
      CloseableIterator(readers).flatMap(i => i)
    }
  }
}

object MergedQueryRunner {

  private def openReaders(readers: Iterator[CloseableIterator[SimpleFeature]]): Seq[CloseableIterator[SimpleFeature]] = {
    val opened = ArrayBuffer.empty[CloseableIterator[SimpleFeature]]
    try {
      readers.foreach(r => opened += r)
      opened.toSeq
    } catch {
      case e: Exception => CloseQuietly(opened).foreach(e.addSuppressed); throw e
    }
  }

  trait Queryable {
    def sft(typeName: String): SimpleFeatureType
    def getFeatureReader(q: Query, t: Transaction): FeatureReader[SimpleFeatureType, SimpleFeature]
  }

  case class DataStoreQueryable(ds: DataStore) extends Queryable {
    override def sft(typeName: String): SimpleFeatureType = ds.getSchema(typeName)
    override def getFeatureReader(q: Query, t: Transaction): FeatureReader[SimpleFeatureType, SimpleFeature] =
      ds.getFeatureReader(q, t)
  }

  case class MergedQueryPlan(
      scanner: () => CloseableIterator[SimpleFeature],
      resultsToFeatures: ResultsToFeatures[SimpleFeature],
      reducer: Option[FeatureReducer],
      maxFeatures: Option[Int],
    ) extends QueryPlan {
    override type Results = SimpleFeature
    override def sort: Option[Seq[(String, Boolean)]] = None
    override def projection: Option[QueryReferenceSystems] = None
    override def scan(): CloseableIterator[SimpleFeature] = scanner()
    override def explain(explainer: Explainer): Unit = {
      explainer.pushLevel("MergedQueryPlan:")
      explainer(s"Reduce: ${reducer.getOrElse("none")}")
      explainer.popLevel()
    }
  }

  private class ParallelScanIterator(
      readers: Seq[CloseableIterator[SimpleFeature]],
      decorator: TaskDecorator,
      copyFeatures: Boolean,
      batchSize: Int,
    ) extends CloseableIterator[SimpleFeature] {

    private val queue = new LinkedBlockingQueue[SimpleFeature](batchSize)
    private val running = new AtomicInteger(readers.length)
    private val error = new AtomicReference[Throwable]()

    private val ex = new CachedThreadPool(readers.length)
    private val tasks = {
      val tasks = readers.map(r => ex.submit(decorator(new ParallelScanTask(r))))
      ex.shutdown()
      tasks
    }

    private var staged: SimpleFeature = _

    @tailrec
    override final def hasNext: Boolean = {
      if (staged != null) {
        return true
      }
      val e = error.getAndSet(null)
      if (e != null) {
        throw e
      }
      staged = queue.poll()
      if (staged != null) {
        return true
      } else if (running.get() == 0 || ex.isTerminated) {
        staged = queue.poll()
        return staged != null
      }
      staged = queue.poll(10, TimeUnit.MILLISECONDS)
      hasNext
    }

    override def next(): SimpleFeature = {
      if (hasNext) {
        val res = staged
        staged = null
        res
      } else {
        Iterator.empty.next()
      }
    }

    override def close(): Unit = {
      var e: Throwable = null
      def addError(err: Throwable): Unit = if (e == null) { e = err } else { e.addSuppressed(err) }
      try {
        tasks.foreach(_.cancel(true))
        ex.shutdownNow()
        e = error.getAndSet(null)
      } finally {
        CloseQuietly(readers).foreach(addError)
      }
      if (e != null) {
        throw e
      }
    }

    private class ParallelScanTask(features: CloseableIterator[SimpleFeature]) extends Runnable {
      override def run(): Unit = {
        try {
          val toQueue = if (copyFeatures) { features.map(ScalaSimpleFeature.copy) } else { features }
          // don't use foreach as that will close the iterator, which may cause duplicate close errors
          while (toQueue.hasNext) {
            queue.put(toQueue.next())
          }
        } catch {
          case _: InterruptedException => // no-op
          case e: Throwable => error.getAndUpdate(cur => if (cur == null) { e } else { cur.addSuppressed(e); cur })
        } finally {
          running.decrementAndGet()
        }
      }
    }
  }
}
