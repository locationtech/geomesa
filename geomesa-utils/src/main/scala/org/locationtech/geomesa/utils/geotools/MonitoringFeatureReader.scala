/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.util.concurrent.atomic.AtomicBoolean

import org.geotools.data.store.ContentFeatureSource
import org.geotools.data.{DelegatingFeatureReader, FeatureReader, Query}
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditedEvent}
import org.locationtech.geomesa.utils.stats.{MethodProfiling, TimingsImpl}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class MonitoringFeatureReader(storeType: String, query: Query, delegate: FeatureReader[SimpleFeatureType, SimpleFeature])
  extends DelegatingFeatureReader[SimpleFeatureType, SimpleFeature] with MethodProfiling with AuditLogger {

  protected var counter = 0L

  private val closed = new AtomicBoolean(false)

  implicit val timings = new TimingsImpl

  override def getDelegate: FeatureReader[SimpleFeatureType, SimpleFeature] = delegate
  override def getFeatureType: SimpleFeatureType = delegate.getFeatureType

  override def next(): SimpleFeature = {
    counter += 1
    profile("next")(delegate.next())
  }
  override def hasNext: Boolean = profile("hasNext")(delegate.hasNext)

  override def close() = if (!closed.getAndSet(true)) {
    closeOnce()
  }

  protected def closeOnce(): Unit = {
    val stat = GeneralUsageStat(storeType,
      getFeatureType.getTypeName,
      System.currentTimeMillis(),
      ECQL.toCQL(query.getFilter),
      timings.time("next") + timings.time("hasNext"),
      counter
    )
    delegate.close()
    writeEvent(stat)
  }
}

trait MonitoringFeatureSourceSupport extends ContentFeatureSourceSupport {
  self: ContentFeatureSource =>

  override def addSupport(query: Query, reader: FR): FR = {
    val parentReader = super.addSupport(query, reader)
    new MonitoringFeatureReader(self.getClass.getSimpleName, query, parentReader)
  }
}

case class GeneralUsageStat(storeType: String, typeName: String, date: Long, filter: String, totalTime: Long, count: Long) extends AuditedEvent
