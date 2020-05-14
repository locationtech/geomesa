/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import org.geotools.data.Query
import org.geotools.data.simple.SimpleFeatureReader
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.index.audit.QueryEvent
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.index.geoserver.ViewParams
import org.locationtech.geomesa.index.planning.QueryRunner
import org.locationtech.geomesa.utils.audit.{AuditProvider, AuditWriter}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timings, TimingsImpl}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class GeoMesaFeatureReader private (sft: SimpleFeatureType, qp: QueryRunner, query: Query, timeout: Option[Long])
    extends SimpleFeatureReader {

  private val closed = new AtomicBoolean(false)
  private val iter = runQuery()

  override def hasNext: Boolean = iter.hasNext

  override def next(): SimpleFeature = iter.next()

  override def getFeatureType: SimpleFeatureType = query.getHints.getReturnSft

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      closeOnce()
    }
  }

  protected def runQuery(): CloseableIterator[SimpleFeature] = qp.runQuery(sft, query)

  protected def closeOnce(): Unit = iter.close()
}

object GeoMesaFeatureReader {

  def apply(
      sft: SimpleFeatureType,
      query: Query,
      qp: QueryRunner,
      timeout: Option[Long],
      audit: Option[(AuditWriter, AuditProvider, String)]): GeoMesaFeatureReader = {
    audit match {
      case None            => new GeoMesaFeatureReader(sft, qp, query, timeout)
      case Some((s, p, t)) => new GeoMesaFeatureReaderWithAudit(sft, qp, query, timeout, s, p, t)
    }
  }

  /**
    * Feature reader with method profiling for auditing
    */
  class GeoMesaFeatureReaderWithAudit private [GeoMesaFeatureReader] (
      sft: SimpleFeatureType,
      qp: QueryRunner,
      query: Query,
      timeout: Option[Long],
      auditWriter: AuditWriter,
      auditProvider: AuditProvider,
      storeType: String,
      timings: Timings = new TimingsImpl
    ) extends GeoMesaFeatureReader(sft, qp, query, timeout) with MethodProfiling {

    private val count = new AtomicLong(0L)

    override def hasNext: Boolean = profile(time => timings.occurrence("hasNext", time))(super.hasNext)
    override def next(): SimpleFeature = profile(time => timings.occurrence("next", time))(super.next())

    override protected def runQuery(): CloseableIterator[SimpleFeature] = {
      profile(time => timings.occurrence("planning", time)) {
        val base = super.runQuery()
        // note: has to be done after running the query so hints are set
        if (query.getHints.getReturnSft == BinaryOutputEncoder.BinEncodedSft) {
          // bin queries pack multiple records into each feature
          // to count the records, we have to count the total bytes coming back, instead of the number of features
          val bytesPerHit = if (query.getHints.getBinLabelField.isDefined) { 24 } else { 16 }
          base.map { sf => count.addAndGet(sf.getAttribute(0).asInstanceOf[Array[Byte]].length / bytesPerHit); sf }
        } else {
          base.map { sf => count.incrementAndGet(); sf }
        }
      }
    }

    override protected def closeOnce(): Unit = {
      try { super.closeOnce() } finally {
        val stat = QueryEvent(
          storeType,
          sft.getTypeName,
          System.currentTimeMillis(),
          auditProvider.getCurrentUserId,
          filterToString(query.getFilter),
          ViewParams.getReadableHints(query),
          timings.time("planning"),
          timings.time("next") + timings.time("hasNext"),
          count.get
        )
        auditWriter.writeEvent(stat) // note: implementations should be asynchronous
      }
    }
  }
}
