/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.view

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.{DataStore, FeatureReader, Query, Transaction}
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.arrow.io.FormatVersion
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.conf.QueryHints.ARROW_DICTIONARY_CACHED
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.iterators.{ArrowScan, DensityScan, StatsScan}
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.index.planning.{LocalQueryRunner, QueryPlanner, QueryRunner}
import org.locationtech.geomesa.index.stats.HasGeoMesaStats
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.index.view.MergedQueryRunner.Queryable
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureOrdering, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.iterators.SortedMergeIterator
import org.locationtech.geomesa.utils.stats._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

/**
  * Query runner for merging results from multiple stores
  *
  * @param ds merged data store
  * @param stores delegate stores
  */
class MergedQueryRunner(ds: HasGeoMesaStats, stores: Seq[(Queryable, Option[Filter])])
    extends QueryRunner with LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  // query interceptors are handled by the individual data stores
  override protected val interceptors: QueryInterceptorFactory = QueryInterceptorFactory.empty()

  override def runQuery(
      sft: SimpleFeatureType,
      original: Query,
      explain: Explainer): CloseableIterator[SimpleFeature] = {

    val query = configureQuery(sft, original)
    val hints = query.getHints

    if (hints.isStatsQuery || hints.isArrowQuery) {
      // for stats and arrow queries, suppress the reduce step for gm stores so that we can do the merge here
      hints.put(QueryHints.Internal.SKIP_REDUCE, java.lang.Boolean.TRUE)
    }

    if (hints.isArrowQuery) {
      arrowQuery(sft, query)
    } else {
      // query each delegate store
      val readers = stores.map { case (store, filter) =>
        val q = new Query(query)
        // make sure to coy the hints so they aren't shared
        q.setHints(new Hints(hints))
        store.getFeatureReader(mergeFilter(q, filter), Transaction.AUTO_COMMIT)
      }

      if (hints.isDensityQuery) {
        densityQuery(sft, readers, hints)
      } else if (hints.isStatsQuery) {
        statsQuery(sft, readers, hints)
      } else if (hints.isBinQuery) {
        if (query.getSortBy != null && !query.getSortBy.isEmpty) {
          logger.warn("Ignoring sort for BIN query")
        }
        binQuery(sft, readers, hints)
      } else {
        Option(query.getSortBy).filterNot(_.isEmpty) match {
          case None => SelfClosingIterator(readers.iterator).flatMap(SelfClosingIterator(_))
          case Some(sort) =>
            val sortSft = QueryPlanner.extractQueryTransforms(sft, query).map(_._1).getOrElse(sft)
            // the delegate stores should sort their results, so we can sort merge them
            new SortedMergeIterator(readers.map(SelfClosingIterator(_)))(SimpleFeatureOrdering(sortSft, sort))
        }
      }
    }
  }

  /**
    * We pull out thread-local hints and view params, but don't handle transforms, etc as that
    * may interfere with non-gm delegate stores
    *
    * @param sft simple feature type associated with the query
    * @param original query to configure
    * @return
    */
  override protected [geomesa] def configureQuery(sft: SimpleFeatureType, original: Query): Query = {
    val query = new Query(original) // note: this ends up sharing a hints object between the two queries

    // set the thread-local hints once, so that we have them for each data store that is being queried
    QueryPlanner.getPerThreadQueryHints.foreach { hints =>
      hints.foreach { case (k, v) => query.getHints.put(k, v) }
      // clear any configured hints so we don't process them again
      QueryPlanner.clearPerThreadQueryHints()
    }

    // handle view params if present
    ViewParams.setHints(query)

    query
  }

  private def arrowQuery(sft: SimpleFeatureType, query: Query): CloseableIterator[SimpleFeature] = {
    val hints = query.getHints

    // handle any sorting here
    QueryPlanner.setQuerySort(sft, query)

    val arrowSft = QueryPlanner.extractQueryTransforms(sft, query).map(_._1).getOrElse(sft)
    val sort = hints.getArrowSort
    val batchSize = ArrowScan.getBatchSize(hints)
    val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid, hints.isArrowProxyFid)
    val ipcOpts = FormatVersion.options(hints.getArrowFormatVersion.getOrElse(FormatVersion.ArrowFormatVersion.get))

    val dictionaryFields = hints.getArrowDictionaryFields
    val providedDictionaries = hints.getArrowDictionaryEncodedValues(sft)
    val cachedDictionaries: Map[String, TopK[AnyRef]] = if (!hints.isArrowCachedDictionaries) { Map.empty } else {
      // get merged dictionary values from all stores and suppress any delegate lookup attempts
      hints.put(ARROW_DICTIONARY_CACHED, false)
      val toLookup = dictionaryFields.filterNot(providedDictionaries.contains)
      toLookup.flatMap(ds.stats.getTopK[AnyRef](sft, _)).map(k => k.property -> k).toMap
    }

    // do the reduce here, as we can't merge finalized arrow results
    val reduce = if (hints.isArrowDoublePass ||
        dictionaryFields.forall(f => providedDictionaries.contains(f) || cachedDictionaries.contains(f))) {
      // we have all the dictionary values, or we will run a query to determine them up front
      val filter = Option(query.getFilter).filter(_ != Filter.INCLUDE).map(FastFilterFactory.optimize(sft, _))
      val dictionaries = ArrowScan.createDictionaries(ds.stats, sft, filter, dictionaryFields,
        providedDictionaries, cachedDictionaries)
      // set the merged dictionaries in the query where they'll be picked up by our delegates
      hints.setArrowDictionaryEncodedValues(dictionaries.map { case (k, v) => (k, v.iterator.toSeq) })
      new ArrowScan.BatchReducer(arrowSft, dictionaries, encoding, ipcOpts, batchSize, sort, sorted = false)
    } else if (hints.isArrowMultiFile) {
      new ArrowScan.FileReducer(arrowSft, dictionaryFields, encoding, ipcOpts, sort)
    } else {
      new ArrowScan.DeltaReducer(arrowSft, dictionaryFields, encoding, ipcOpts, batchSize, sort, sorted = false)
    }

    // now that we have standardized dictionaries, we can query the delegate stores
    val readers = stores.map { case (store, filter) =>
      val q = new Query(query)
      q.setHints(new Hints(hints))
      store.getFeatureReader(mergeFilter(q, filter), Transaction.AUTO_COMMIT)
    }

    val results = SelfClosingIterator(readers.iterator).flatMap { reader =>
      val schema = reader.getFeatureType
      if (schema == org.locationtech.geomesa.arrow.ArrowEncodedSft) {
        // arrow processing has been handled by the store already
        CloseableIterator(reader)
      } else {
        // the store just returned normal features, do the arrow processing here
        val copy = SimpleFeatureTypes.immutable(schema, sft.getUserData) // copy default dtg, etc if necessary
        // note: we don't need to pass in the transform or filter, as the transform should have already been
        // applied and the dictionaries calculated up front (if needed)
        LocalQueryRunner.transform(copy, CloseableIterator(reader), None, hints, None)
      }
    }

    reduce(results)
  }

  private def densityQuery(sft: SimpleFeatureType,
                           readers: Seq[FeatureReader[SimpleFeatureType, SimpleFeature]],
                           hints: Hints): CloseableIterator[SimpleFeature] = {
    SelfClosingIterator(readers.iterator).flatMap { reader =>
      val schema = reader.getFeatureType
      if (schema == DensityScan.DensitySft) {
        // density processing has been handled by the store already
        CloseableIterator(reader)
      } else {
        // the store just returned regular features, do the density processing here
        val copy = SimpleFeatureTypes.immutable(schema, sft.getUserData) // copy default dtg, etc if necessary
        LocalQueryRunner.transform(copy, CloseableIterator(reader), None, hints, None)
      }
    }
  }

  private def statsQuery(sft: SimpleFeatureType,
                         readers: Seq[FeatureReader[SimpleFeatureType, SimpleFeature]],
                         hints: Hints): CloseableIterator[SimpleFeature] = {
    // do the reduce here, as we can't merge json stats
    val results = SelfClosingIterator(readers.iterator).flatMap { reader =>
      val schema = reader.getFeatureType
      if (schema == StatsScan.StatsSft) {
        // stats processing has been handled by the store already
        CloseableIterator(reader)
      } else {
        // the store just returned regular features, do the stats processing here
        val copy = SimpleFeatureTypes.immutable(schema, sft.getUserData) // copy default dtg, etc if necessary
        LocalQueryRunner.transform(copy, CloseableIterator(reader), None, hints, None)
      }
    }
    StatsScan.StatsReducer(sft, hints)(results)
  }

  private def binQuery(sft: SimpleFeatureType,
                       readers: Seq[FeatureReader[SimpleFeatureType, SimpleFeature]],
                       hints: Hints): CloseableIterator[SimpleFeature] = {
    SelfClosingIterator(readers.iterator).flatMap { reader =>
      val schema = reader.getFeatureType
      if (schema == BinaryOutputEncoder.BinEncodedSft) {
        // bin processing has been handled by the store already
        CloseableIterator(reader)
      } else {
        // the store just returned regular features, do the bin processing here
        val copy = SimpleFeatureTypes.immutable(schema, sft.getUserData) // copy default dtg, etc if necessary
        LocalQueryRunner.transform(copy, CloseableIterator(reader), None, hints, None)
      }
    }
  }

  override protected [geomesa] def getReturnSft(sft: SimpleFeatureType, hints: Hints): SimpleFeatureType = {
    if (hints.isBinQuery) {
      BinaryOutputEncoder.BinEncodedSft
    } else if (hints.isArrowQuery) {
      org.locationtech.geomesa.arrow.ArrowEncodedSft
    } else if (hints.isDensityQuery) {
      DensityScan.DensitySft
    } else if (hints.isStatsQuery) {
      StatsScan.StatsSft
    } else {
      super.getReturnSft(sft, hints)
    }
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
