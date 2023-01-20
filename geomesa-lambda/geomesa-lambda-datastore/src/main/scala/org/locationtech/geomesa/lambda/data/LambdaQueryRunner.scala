/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.lambda.data

import com.github.benmanes.caffeine.cache.LoadingCache
import com.typesafe.scalalogging.StrictLogging
import org.geotools.data.simple.SimpleFeatureReader
import org.geotools.data.{DataStore, FeatureReader, Query, Transaction}
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.planning.QueryRunner.QueryResult
import org.locationtech.geomesa.index.utils.{ExplainLogger, Explainer}
import org.locationtech.geomesa.index.view.MergedQueryRunner
import org.locationtech.geomesa.index.view.MergedQueryRunner.DataStoreQueryable
import org.locationtech.geomesa.lambda.data.LambdaQueryRunner.TransientQueryable
import org.locationtech.geomesa.lambda.stream.TransientStore
import org.locationtech.geomesa.utils.collection.{CloseableIterator, SelfClosingIterator}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class LambdaQueryRunner(ds: LambdaDataStore, persistence: DataStore, transients: LoadingCache[String, TransientStore])
    extends MergedQueryRunner(
      ds, Seq(TransientQueryable(transients) -> None, DataStoreQueryable(persistence) -> None), true, false) {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  // TODO pass explain through?

  override def runQuery(sft: SimpleFeatureType, original: Query, explain: Explainer): QueryResult = {
    // configure the query so we get viewparams and threaded hints
    val query = configureQuery(sft, original)
    val result = super.runQuery(sft, query, explain)
    if (query.getHints.isLambdaQueryPersistent && query.getHints.isLambdaQueryTransient) {
      result
    } else if (query.getHints.isLambdaQueryPersistent) {
      result.copy(iterator = () => SelfClosingIterator(persistence.getFeatureReader(query, Transaction.AUTO_COMMIT)))
    } else {
      def run(): CloseableIterator[SimpleFeature] = {
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
        transients.get(sft.getTypeName)
            .read(Option(query.getFilter), Option(query.getPropertyNames), Option(query.getHints), explain)
            .iterator()
      }
      result.copy(iterator = run)
    }
  }
}

object LambdaQueryRunner {

  case class TransientQueryable(transients: LoadingCache[String, TransientStore])
      extends MergedQueryRunner.Queryable with StrictLogging {
    override def getFeatureReader(q: Query, t: Transaction): FeatureReader[SimpleFeatureType, SimpleFeature] = {
      val store = transients.get(q.getTypeName)
      val explain = new ExplainLogger(logger)
      val result = store.read(Option(q.getFilter), Option(q.getPropertyNames), Option(q.getHints), explain)

      new SimpleFeatureReader() {
        private val iter = result.iterator()
        override def getFeatureType: SimpleFeatureType = result.schema
        override def hasNext: Boolean = iter.hasNext
        override def next(): SimpleFeature = iter.next()
        override def close(): Unit = iter.close()
      }
    }
  }
}
