/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import com.github.benmanes.caffeine.cache.LoadingCache
import org.geotools.data.{DataStore, Query, Transaction}
import org.geotools.factory.Hints
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.iterators.{ArrowScan, DensityScan, StatsScan}
import org.locationtech.geomesa.index.planning.QueryInterceptor.QueryInterceptorFactory
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.utils.Explainer
import org.locationtech.geomesa.lambda.stream.TransientStore
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.locationtech.geomesa.utils.stats.TopK
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class LambdaQueryRunner(persistence: DataStore, transients: LoadingCache[String, TransientStore], stats: GeoMesaStats)
    extends QueryRunner {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  // TODO pass explain through?

  // query interceptors are handled separately by the persistent and transient layers
  override protected val interceptors: QueryInterceptorFactory = QueryInterceptorFactory.empty()

  override def runQuery(sft: SimpleFeatureType, query: Query, explain: Explainer): CloseableIterator[SimpleFeature] = {
    val hints = configureQuery(sft, query).getHints // configure the query so we get viewparams, transforms, etc
    if (hints.isLambdaQueryPersistent && hints.isLambdaQueryTransient) {
      runMergedQuery(sft, query, explain)
    } else if (hints.isLambdaQueryPersistent) {
      SelfClosingIterator(persistence.getFeatureReader(query, Transaction.AUTO_COMMIT))
    } else {
      // ensure that we still audit the query
      val audit = Option(persistence).collect { case ds: GeoMesaDataStore[_] => ds.config.audit }.flatten
      audit.foreach { case (writer, provider, typ) =>
        val stat = QueryEvent(
          s"$typ-lambda",
          sft.getTypeName,
          System.currentTimeMillis(),
          provider.getCurrentUserId,
          filterToString(query.getFilter),
          ViewParams.getReadableHints(query),
          0,
          0,
          0
        )
        writer.writeEvent(stat) // note: implementations should be asynchronous
      }
      CloseableIterator(transients.get(sft.getTypeName).read(Option(query.getFilter),
        Option(query.getPropertyNames), Option(query.getHints), explain))
    }
  }

  private def runMergedQuery(sft: SimpleFeatureType, query: Query, explain: Explainer): CloseableIterator[SimpleFeature] = {
    val hints = query.getHints
    if (hints.isStatsQuery) {
      // do the reduce here, as we can't merge json stats
      hints.put(QueryHints.Internal.SKIP_REDUCE, java.lang.Boolean.TRUE)
      StatsScan.reduceFeatures(sft, hints)(standardQuery(sft, query, explain))
    } else if (hints.isArrowQuery) {
      val arrowSft = hints.getTransformSchema.getOrElse(sft)

      val sort = hints.getArrowSort
      val batchSize = ArrowScan.getBatchSize(hints)
      val encoding = SimpleFeatureEncoding.min(hints.isArrowIncludeFid, hints.isArrowProxyFid)

      val dictionaryFields = hints.getArrowDictionaryFields
      val providedDictionaries = hints.getArrowDictionaryEncodedValues(sft)
      val cachedDictionaries: Map[String, TopK[AnyRef]] = if (!hints.isArrowCachedDictionaries) { Map.empty } else {
        val toLookup = dictionaryFields.filterNot(providedDictionaries.contains)
        stats.getStats[TopK[AnyRef]](sft, toLookup).map(k => k.property -> k).toMap
      }

      if (hints.isArrowDoublePass ||
          dictionaryFields.forall(f => providedDictionaries.contains(f) || cachedDictionaries.contains(f))) {
        val filter = Option(query.getFilter).filter(_ != Filter.INCLUDE).map(FastFilterFactory.optimize(sft, _))
        // we have all the dictionary values, or we will run a query to determine them up front
        val dictionaries = ArrowScan.createDictionaries(stats, sft, filter, dictionaryFields,
          providedDictionaries, cachedDictionaries)
        // set the merged dictionaries in the query where they'll be picked up by our delegates
        hints.setArrowDictionaryEncodedValues(dictionaries.map { case (k, v) => (k, v.iterator.toSeq) })
        hints.put(QueryHints.Internal.SKIP_REDUCE, java.lang.Boolean.TRUE)

        ArrowScan.mergeBatches(arrowSft, dictionaries, encoding, batchSize, sort)(standardQuery(sft, query, explain))
      } else if (hints.isArrowMultiFile) {
        hints.put(QueryHints.Internal.SKIP_REDUCE, java.lang.Boolean.TRUE)
        ArrowScan.mergeFiles(arrowSft, dictionaryFields, encoding, sort)(standardQuery(sft, query, explain))
      } else {
        hints.put(QueryHints.Internal.SKIP_REDUCE, java.lang.Boolean.TRUE)
        ArrowScan.mergeDeltas(arrowSft, dictionaryFields, encoding, batchSize, sort)(standardQuery(sft, query, explain))
      }
    } else {
      standardQuery(sft, query, explain)
    }
  }

  private def standardQuery(sft: SimpleFeatureType, query: Query, explain: Explainer): CloseableIterator[SimpleFeature] = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val transientFeatures = CloseableIterator(transients.get(sft.getTypeName).read(Option(query.getFilter),
      Option(query.getPropertyNames), Option(query.getHints), explain))
    val fc = persistence.getFeatureSource(sft.getTypeName).getFeatures(query)
    // kick off the persistent query in a future, but don't wait for results yet
    val persistentFeatures = Future(CloseableIterator(fc.features))
    // ++ is evaluated lazily, so we will block on the persistent features once the transient iterator is exhausted
    transientFeatures ++ Await.result(persistentFeatures, Duration.Inf)
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
