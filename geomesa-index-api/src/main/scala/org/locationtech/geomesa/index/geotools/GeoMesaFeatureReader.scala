/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import org.geotools.api.data.{Query, SimpleFeatureReader, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.audit.AuditWriter
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.planning.QueryRunner.QueryResult
import org.locationtech.geomesa.utils.metrics.MethodProfiling

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

class GeoMesaFeatureReader(result: QueryResult) {

  def schema: SimpleFeatureType = result.schema
  def hints: Hints = result.hints

  def reader(): SimpleFeatureReader = new ResultReader()

  private class ResultReader extends SimpleFeatureReader {

    private val iter = result.iterator()

    override def getFeatureType: SimpleFeatureType = result.schema
    override def hasNext: Boolean = iter.hasNext
    override def next(): SimpleFeature = iter.next()
    override def close(): Unit = iter.close()
  }
}

object GeoMesaFeatureReader extends MethodProfiling {

  def apply(
      sft: SimpleFeatureType,
      query: Query,
      qp: QueryRunner,
      audit: Option[AuditWriter]): GeoMesaFeatureReader = {
    audit match {
      case None => new GeoMesaFeatureReader(qp.runQuery(sft, query))
      case Some(a) => new GeoMesaFeatureReaderWithAudit(qp.runQuery(sft, query), a, sft.getTypeName, query.getFilter)
    }
  }

  trait HasGeoMesaFeatureReader {

    /**
     * Internal method to get a feature reader without reloading the simple feature type. We don't expose this
     * widely as we want to ensure that the sft has been loaded from our catalog
     *
     * @param sft simple feature type
     * @param transaction transaction
     * @param query query
     * @return
     */
    private[geomesa] def getFeatureReader(
        sft: SimpleFeatureType,
        transaction: Transaction,
        query: Query): GeoMesaFeatureReader
  }

  private class GeoMesaFeatureReaderWithAudit(
      result: QueryResult,
      auditWriter: AuditWriter,
      typeName: String,
      filter: Filter,
    ) extends GeoMesaFeatureReader(result) {

    override def reader(): SimpleFeatureReader = new ResultReaderWithAudit()

    private class ResultReaderWithAudit extends SimpleFeatureReader with MethodProfiling {

      private val start = System.currentTimeMillis()
      private val user = auditWriter.auditProvider.getCurrentUserId
      private val closed = new AtomicBoolean(false)
      private val count = new AtomicLong(0L)
      private val runtimeNanos = new AtomicLong(0L)
      private val iter = result.iterator(runtimeNanos.set, count.set)

      override def getFeatureType: SimpleFeatureType = result.schema
      override def hasNext: Boolean = iter.hasNext
      override def next(): SimpleFeature = iter.next()

      override def close(): Unit = {
        if (closed.compareAndSet(false, true)) {
          try { iter.close() } finally {
            // note: implementations should be asynchronous
            auditWriter.writeQueryEvent(typeName, user, filter, hints, result.plans, start, System.currentTimeMillis(),
              result.planTimeNanos / 1000000L, runtimeNanos.get() / 1000000L, count.get)
          }
        }
      }
    }
  }
}
