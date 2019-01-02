/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.geotools

import java.util.concurrent.atomic.AtomicBoolean

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
import org.locationtech.geomesa.utils.stats.{MethodProfiling, TimingsImpl}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

abstract class GeoMesaFeatureReader(val query: Query, timeout: Option[Long], val maxFeatures: Long)
    extends SimpleFeatureReader with ManagedQuery {

  private val closed = new AtomicBoolean(false)
  private val cancel = timeout.map(_ => ThreadManagement.register(this))

  def count: Long = -1L

  protected def closeOnce(): Unit

  override def isClosed: Boolean = closed.get()

  override def getTimeout: Long = timeout.getOrElse(-1L)

  override def getFeatureType: SimpleFeatureType = query.getHints.getReturnSft

  override def close(): Unit = {
    if (closed.compareAndSet(false, true)) {
      try { closeOnce() } finally {
        cancel.foreach(_.cancel(false))
      }
    }
  }

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
      case (None, None)                 => new GeoMesaFeatureReaderImpl(sft, query, qp, timeout)
      case (None, Some(max))            => new GeoMesaFeatureReaderImpl(sft, query, qp, timeout, max) with FeatureLimiting
      case (Some((s, p, t)), None)      => new GeoMesaFeatureReaderWithAudit(sft, query, qp, timeout, s, p, t) with FeatureCounting
      case (Some((s, p, t)), Some(max)) => new GeoMesaFeatureReaderWithAudit(sft, query, qp, timeout, s, p, t, max) with FeatureLimiting
    }
  }

  /**
    * Basic feature reader that wraps the underlying iterator of simple features.
    */
  class GeoMesaFeatureReaderImpl(sft: SimpleFeatureType,
                                 query: Query,
                                 qp: QueryRunner,
                                 timeout: Option[Long],
                                 maxFeatures: Long = 0L) extends GeoMesaFeatureReader(query, timeout, maxFeatures) {

    private val iter = qp.runQuery(sft, query)

    override def hasNext: Boolean = iter.hasNext
    override def next(): SimpleFeature = iter.next()
    override protected def closeOnce(): Unit = iter.close()
  }

  /**
    * Basic feature reader with method profiling for stat gathering.
    */
  class GeoMesaFeatureReaderWithAudit(sft: SimpleFeatureType,
                                      query: Query,
                                      qp: QueryRunner,
                                      timeout: Option[Long],
                                      auditWriter: AuditWriter,
                                      auditProvider: AuditProvider,
                                      storeType: String,
                                      maxFeatures: Long = 0L)
      extends GeoMesaFeatureReader(query, timeout, maxFeatures) with MethodProfiling {

    private val timings = new TimingsImpl
    private val iter = profile(time => timings.occurrence("planning", time))(qp.runQuery(sft, query))

    override def next(): SimpleFeature = profile(time => timings.occurrence("next", time))(iter.next())
    override def hasNext: Boolean = profile(time => timings.occurrence("hasNext", time))(iter.hasNext)

    override protected def closeOnce(): Unit = {
      iter.close()
      val stat = QueryEvent(
        storeType,
        sft.getTypeName,
        System.currentTimeMillis(),
        auditProvider.getCurrentUserId,
        filterToString(query.getFilter),
        ViewParams.getReadableHints(query),
        timings.time("planning"),
        timings.time("next") + timings.time("hasNext"),
        count
      )
      auditWriter.writeEvent(stat) // note: implementations should be asynchronous
    }
  }

  trait FeatureCounting extends GeoMesaFeatureReader {

    protected var counter = 0L
    // because the query planner configures the query hints, we can't check for bin hints
    // until after setting up the iterator
    protected val sfCount: SimpleFeature => Int = if (getFeatureType == BinaryOutputEncoder.BinEncodedSft) {
      // bin queries pack multiple records into each feature
      // to count the records, we have to count the total bytes coming back, instead of the number of features
      val bytesPerHit = if (query.getHints.getBinLabelField.isDefined) 24 else 16
      sf => sf.getAttribute(0).asInstanceOf[Array[Byte]].length / bytesPerHit
    } else {
      _ => 1
    }

    abstract override def next(): SimpleFeature = {
      val sf = super.next()
      counter += sfCount(sf)
      sf
    }

    abstract override def count: Long = counter
  }

  trait FeatureLimiting extends FeatureCounting {
    abstract override def hasNext: Boolean = counter < maxFeatures && super.hasNext
  }
}
