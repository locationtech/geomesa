/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import com.github.benmanes.caffeine.cache.LoadingCache
import org.geotools.data.{DataStore, Query, Transaction}
import org.geotools.factory.Hints
import org.locationtech.geomesa.filter.factory.FastFilterFactory
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.index.conf.QueryHints
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.iterators.{ArrowBatchScan, DensityScan}
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.stats.GeoMesaStats
import org.locationtech.geomesa.index.utils.{Explainer, KryoLazyStatsUtils}
import org.locationtech.geomesa.lambda.stream.TransientStore
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

class LambdaQueryRunner(persistence: DataStore, transients: LoadingCache[String, TransientStore], stats: GeoMesaStats)
    extends QueryRunner {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  // TODO pass explain through?

  override def runQuery(sft: SimpleFeatureType, query: Query, explain: Explainer): CloseableIterator[SimpleFeature] = {
    if (query.getHints.isLambdaQueryPersistent && query.getHints.isLambdaQueryTransient) {
      runMergedQuery(sft, query, explain)
    } else if (query.getHints.isLambdaQueryPersistent) {
      SelfClosingIterator(persistence.getFeatureReader(query, Transaction.AUTO_COMMIT))
    } else {
      // ensure that we still audit the query
      val audit = Option(persistence).collect { case ds: GeoMesaDataStore[_, _, _] => ds.config.audit }.flatten
      audit.foreach { case (writer, provider, typ) =>
        val stat = QueryEvent(
          s"$typ-lambda",
          sft.getTypeName,
          System.currentTimeMillis(),
          provider.getCurrentUserId,
          filterToString(query.getFilter),
          QueryEvent.hintsToString(query.getHints),
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
    val hints = configureQuery(sft, query).getHints // configure the query so we get transforms, etc
    if (hints.isStatsQuery) {
      // do the reduce here, as we can't merge json stats
      hints.put(QueryHints.Internal.SKIP_REDUCE, java.lang.Boolean.TRUE)
      KryoLazyStatsUtils.reduceFeatures(sft, hints)(standardQuery(sft, query, explain))
    } else if (query.getHints.isArrowQuery) {
      // calculate merged dictionaries up front if required
      val dictionaryFields = hints.getArrowDictionaryFields
      val providedDictionaries = hints.getArrowDictionaryEncodedValues
      if (hints.getArrowSort.isDefined || hints.isArrowComputeDictionaries ||
          dictionaryFields.forall(providedDictionaries.contains)) {
        val filter = Option(query.getFilter).filter(_ != Filter.INCLUDE).map(FastFilterFactory.optimize(sft, _))
        val dictionaries = ArrowBatchScan.createDictionaries(stats, sft, filter, dictionaryFields, providedDictionaries)
        // set the merged dictionaries in the query where they'll be picked up by our delegates
        hints.setArrowDictionaryEncodedValues(dictionaries.map { case (k, v) => (k, v.values) })
        hints.put(QueryHints.Internal.SKIP_REDUCE, java.lang.Boolean.TRUE)
        val arrowSft = hints.getTransformSchema.getOrElse(sft)
        ArrowBatchScan.reduceFeatures(arrowSft, hints, dictionaries)(standardQuery(sft, query, explain))
      } else {
        standardQuery(sft, query, explain)
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
      KryoLazyStatsUtils.StatsSft
    } else {
      super.getReturnSft(sft, hints)
    }
  }
}
