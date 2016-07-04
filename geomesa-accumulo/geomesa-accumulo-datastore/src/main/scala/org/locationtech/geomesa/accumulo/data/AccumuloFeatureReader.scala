/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.util.concurrent.atomic.AtomicBoolean

import org.geotools.data.{FeatureReader, Query}
import org.locationtech.geomesa.accumulo.data.stats.usage.{GeoMesaUsageStats, QueryStat, QueryStatTransform}
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator.BIN_ATTRIBUTE_INDEX
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.security.AuditProvider
import org.locationtech.geomesa.utils.stats.{MethodProfiling, TimingsImpl}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

abstract class AccumuloFeatureReader(val query: Query, val timeout: Option[Long], val maxFeatures: Long)
    extends FeatureReader[SimpleFeatureType, SimpleFeature] {

  private val closed = new AtomicBoolean(false)
  private lazy val start = System.currentTimeMillis()

  timeout.foreach(t => ThreadManagement.register(this, start, t))

  def isClosed: Boolean = closed.get()
  def count: Long = -1L

  protected def closeOnce(): Unit

  override def getFeatureType = query.getHints.getReturnSft

  override def close() = if (!closed.getAndSet(true)) {
    try {
      timeout.foreach(t => ThreadManagement.unregister(this, start, t))
    } finally {
      closeOnce()
    }
  }
}

object AccumuloFeatureReader {
  def apply(query: Query, qp: QueryPlanner, timeout: Option[Long], stats: Option[(GeoMesaUsageStats, AuditProvider)]) = {
    val maxFeatures = if (query.isMaxFeaturesUnlimited) None else Some(query.getMaxFeatures)

    (stats, maxFeatures) match {
      case (None, None)                => new AccumuloFeatureReaderImpl(query, qp, timeout)
      case (None, Some(max))           => new AccumuloFeatureReaderImpl(query, qp, timeout, max) with FeatureLimiting
      case (Some((sw, ap)), None)      => new AccumuloFeatureReaderWithStats(query, qp, timeout, sw, ap) with FeatureCounting
      case (Some((sw, ap)), Some(max)) => new AccumuloFeatureReaderWithStats(query, qp, timeout, sw, ap, max) with FeatureLimiting
    }
  }
}

/**
 * Basic feature reader that wraps the underlying iterator of simple features.
 */
class AccumuloFeatureReaderImpl(query: Query, qp: QueryPlanner, timeout: Option[Long], maxFeatures: Long = 0L)
    extends AccumuloFeatureReader(query, timeout, maxFeatures) {

  private val iter = qp.runQuery(query)

  override def hasNext: Boolean = iter.hasNext
  override def next(): SimpleFeature = iter.next()
  override protected def closeOnce(): Unit = iter.close()
}

/**
 * Basic feature reader with method profiling for stat gathering.
 */
class AccumuloFeatureReaderWithStats(query: Query,
                                     qp: QueryPlanner,
                                     timeout: Option[Long],
                                     sw: GeoMesaUsageStats,
                                     auditProvider: AuditProvider,
                                     maxFeatures: Long = 0L)
    extends AccumuloFeatureReader(query, timeout, maxFeatures) with MethodProfiling {

  implicit val timings = new TimingsImpl
  private val iter = profile(qp.runQuery(query), "planning")

  override def next(): SimpleFeature = profile(iter.next(), "next")
  override def hasNext: Boolean = profile(iter.hasNext, "hasNext")

  override protected def closeOnce(): Unit = {
    iter.close()
    val stat = QueryStat(qp.sft.getTypeName,
      System.currentTimeMillis(),
      auditProvider.getCurrentUserId,
      filterToString(query.getFilter),
      QueryStatTransform.hintsToString(query.getHints),
      timings.time("planning"),
      timings.time("next") + timings.time("hasNext"),
      count
    )
    sw.writeUsageStat(stat) // note: implementation is asynchronous
  }
}

trait FeatureCounting extends AccumuloFeatureReader {

  protected var counter = 0L
  // because the query planner configures the query hints, we can't check for bin hints
  // until after setting up the iterator
  protected val sfCount: (SimpleFeature) => Int = if (query.getHints.isBinQuery) {
    // bin queries pack multiple records into each feature
    // to count the records, we have to count the total bytes coming back, instead of the number of features
    val bytesPerHit = if (query.getHints.getBinLabelField.isDefined) 24 else 16
    (sf) => sf.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]].length / bytesPerHit
  } else {
    (_) => 1
  }

  abstract override def next(): SimpleFeature = {
    val sf = super.next()
    counter += sfCount(sf)
    sf
  }

  abstract override def count = counter
}

trait FeatureLimiting extends FeatureCounting {
  abstract override def hasNext: Boolean = counter < maxFeatures && super.hasNext
}
