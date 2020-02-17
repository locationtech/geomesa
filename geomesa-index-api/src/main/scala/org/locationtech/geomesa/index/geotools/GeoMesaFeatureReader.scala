/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.index.utils.ThreadManagement
import org.locationtech.geomesa.index.utils.ThreadManagement.ManagedQuery
import org.locationtech.geomesa.utils.audit.{AuditProvider, AuditWriter}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.stats.{MethodProfiling, TimingsImpl}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class GeoMesaFeatureReader private (sft: SimpleFeatureType, qp: QueryRunner, protected val query: Query, timeout: Option[Long])
    extends SimpleFeatureReader with ManagedQuery {

  private val closed = new AtomicBoolean(false)
  private val iter = runQuery()
  private val cancel = timeout.map(_ => ThreadManagement.register(this))

  override def hasNext: Boolean = iter.hasNext

  override def next(): SimpleFeature = iter.next()

  override def getFeatureType: SimpleFeatureType = query.getHints.getReturnSft

  override def getTimeout: Long = timeout.getOrElse(-1L)

  override def isClosed: Boolean = closed.get()

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      try { closeOnce() } finally {
        cancel.foreach(_.cancel(false))
      }
    }
  }

  protected def runQuery(): CloseableIterator[SimpleFeature] = qp.runQuery(sft, query)

  protected def closeOnce(): Unit = iter.close()

  override def debug: String =
    s"query on schema '${query.getTypeName}' with filter '${filterToString(query.getFilter)}'"
}

object GeoMesaFeatureReader {

  def apply(sft: SimpleFeatureType,
            query: Query,
            qp: QueryRunner,
            timeout: Option[Long],
            audit: Option[(AuditWriter, AuditProvider, String)]): GeoMesaFeatureReader = {
    val maxFeatures = if (query.isMaxFeaturesUnlimited) None else Some(query.getMaxFeatures)
    (audit, maxFeatures) match {
      case (None, None)               => new GeoMesaFeatureReader(sft, qp, query, timeout)
      case (None, Some(m))            => new GeoMesaFeatureReader(sft, qp, query, timeout) with FeatureLimiting { override protected val max: Long = m }
      case (Some((s, p, t)), None)    => new GeoMesaFeatureReaderWithAudit(sft, query, qp, timeout, s, p, t)
      case (Some((s, p, t)), Some(m)) => new GeoMesaFeatureReaderWithAudit(sft, query, qp, timeout, s, p, t) with FeatureLimiting { override protected val max: Long = m }
    }
  }

  /**
   * Basic feature reader with method profiling for stat gathering.
   */
  class GeoMesaFeatureReaderWithAudit(
      sft: SimpleFeatureType,
      query: Query,
      qp: QueryRunner,
      timeout: Option[Long],
      auditWriter: AuditWriter,
      auditProvider: AuditProvider,
      storeType: String,
      maxFeatures: Long = 0L
    ) extends {
      private val timings = new TimingsImpl
    } with GeoMesaFeatureReader(sft, qp, query, timeout) with FeatureCounting with MethodProfiling {

    override def next(): SimpleFeature = profile(time => timings.occurrence("next", time))(super.next())
    override def hasNext: Boolean = profile(time => timings.occurrence("hasNext", time))(super.hasNext)

    override protected def runQuery(): CloseableIterator[SimpleFeature] =
      profile(time => timings.occurrence("planning", time))(super.runQuery())

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
          counter.get
        )
        auditWriter.writeEvent(stat) // note: implementations should be asynchronous
      }
    }
  }

  trait FeatureCounting extends GeoMesaFeatureReader {

    protected val counter = new AtomicLong(0L)
    // because the query planner configures the query hints, we can't check for bin hints
    // until after setting up the iterator
    private val sfCount: SimpleFeature => Unit = if (getFeatureType == BinaryOutputEncoder.BinEncodedSft) {
      // bin queries pack multiple records into each feature
      // to count the records, we have to count the total bytes coming back, instead of the number of features
      val bytesPerHit = if (query.getHints.getBinLabelField.isDefined) 24 else 16
      sf => counter.addAndGet(sf.getAttribute(0).asInstanceOf[Array[Byte]].length / bytesPerHit)
    } else {
      _ => counter.incrementAndGet()
    }

    abstract override def next(): SimpleFeature = {
      val sf = super.next()
      sfCount(sf)
      sf
    }
  }

  trait FeatureLimiting extends FeatureCounting {
    protected def max: Long
    abstract override def hasNext: Boolean = counter.get < max && super.hasNext
  }
}
