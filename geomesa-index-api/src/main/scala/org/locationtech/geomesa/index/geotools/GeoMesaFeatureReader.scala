/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import org.geotools.api.data.{Query, SimpleFeatureReader, Transaction}
import org.geotools.api.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.geotools.api.filter.Filter
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.audit.AuditWriter
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.index.planning.QueryPlanner.QueryPlanResult
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.index.planning.QueryRunner.QueryResult
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timings, TimingsImpl}

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
      case Some(a) =>
        val timings = new TimingsImpl()
        val result = profile(time => timings.occurrence("planning", time))(qp.runQuery(sft, query))
        new GeoMesaFeatureReaderWithAudit(result, timings, a, sft.getTypeName, query.getFilter)
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

  class GeoMesaFeatureReaderWithAudit(
      result: QueryResult,
      timings: Timings,
      auditWriter: AuditWriter,
      typeName: String,
      filter: Filter
    ) extends GeoMesaFeatureReader(result) {

    override def reader(): SimpleFeatureReader = new ResultReaderWithAudit()

    private class ResultReaderWithAudit extends SimpleFeatureReader with MethodProfiling {

      private val start = System.currentTimeMillis()
      private val user = auditWriter.auditProvider.getCurrentUserId
      private val closed = new AtomicBoolean(false)
      private val count = new AtomicLong(0L)
      private val iter = profile(time => timings.occurrence("planning", time)) {
        val base = result.iterator()
        // note: has to be done after running the query so hints are set
        if (result.schema == BinaryOutputEncoder.BinEncodedSft) {
          // bin queries pack multiple records into each feature
          // to count the records, we have to count the total bytes coming back, instead of the number of features
          val bytesPerHit = if (result.hints.getBinLabelField.isDefined) { 24 } else { 16 }
          base.map { sf => count.addAndGet(sf.getAttribute(0).asInstanceOf[Array[Byte]].length / bytesPerHit); sf }
        } else {
          base.map { sf => count.incrementAndGet(); sf }
        }
      }

      override def getFeatureType: SimpleFeatureType = result.schema

      override def hasNext: Boolean = profile(time => timings.occurrence("hasNext", time))(iter.hasNext)
      override def next(): SimpleFeature = profile(time => timings.occurrence("next", time))(iter.next())

      override def close(): Unit = {
        if (closed.compareAndSet(false, true)) {
          try { iter.close() } finally {
            val plans = result match {
              case r: QueryPlanResult[_] => r.plans
              case _ => Seq.empty
            }
            // note: implementations should be asynchronous
            auditWriter.writeQueryEvent(typeName, user, filter, hints, plans, start, System.currentTimeMillis(),
              timings.time("planning"), timings.time("next") + timings.time("hasNext"), count.get)
          }
        }
      }
    }
  }
}
