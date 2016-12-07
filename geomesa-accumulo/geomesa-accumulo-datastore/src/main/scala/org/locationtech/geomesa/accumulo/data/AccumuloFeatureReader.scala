/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.util.concurrent.atomic.AtomicBoolean

import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.{FeatureReader, Query}
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index.QueryPlanner.SFIter
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.stats._
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.security.AuditProvider
import org.locationtech.geomesa.utils.stats.{MethodProfiling, Timings, TimingsImpl}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait AccumuloFeatureReader extends FeatureReader[SimpleFeatureType, SimpleFeature] {

  private val closed = new AtomicBoolean(false)
  private lazy val start = System.currentTimeMillis()

  timeout.foreach(t => ThreadManagement.register(this, start, t))

  def query: Query
  def timeout: Option[Long]
  def isClosed: Boolean = closed.get()

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

object AccumuloFeatureReader extends MethodProfiling {
  def apply(query: Query,
            qp: QueryPlanner,
            timeout: Option[Long],
            writer: Option[StatWriter],
            auditProvider: AuditProvider) = {
    writer match {
      case None => new AccumuloFeatureReaderImpl(query, qp, timeout)
      case Some(sw) => new AccumuloFeatureReaderWithStats(query, qp, timeout, sw, auditProvider)
    }
  }
}

class AccumuloFeatureReaderImpl(val query: Query, qp: QueryPlanner, val timeout: Option[Long])
    extends AccumuloFeatureReader {

  private val iter = qp.runQuery(query)

  override def next(): SimpleFeature = iter.next()
  override def hasNext: Boolean = iter.hasNext
  override protected def closeOnce(): Unit = iter.close()
}

class AccumuloFeatureReaderWithStats(val query: Query,
                                     qp: QueryPlanner,
                                     val timeout: Option[Long],
                                     sw: StatWriter,
                                     auditProvider: AuditProvider)
    extends AccumuloFeatureReader with MethodProfiling {

  implicit val timings = new TimingsImpl
  private val iter = profile(qp.runQuery(query), "planning")

  // because the query planner configures the query hints, we can't check for bin hints
  // until after setting up the iterator
  private val delegate = if (query.getHints.isBinQuery) {
    new AccumuloFeatureReaderBinDelegate(iter, query, qp)
  } else {
    new AccumuloFeatureReaderDelegate(iter, query, qp)
  }

  override def next(): SimpleFeature = delegate.next()
  override def hasNext: Boolean = delegate.hasNext

  override protected def closeOnce(): Unit = {
    delegate.close()
    val count = delegate.getCount
    val stat = QueryStat(qp.sft.getTypeName,
      System.currentTimeMillis(),
      auditProvider.getCurrentUserId,
      filterToString(query.getFilter),
      QueryStatTransform.hintsToString(query.getHints),
      timings.time("planning"),
      timings.time("next") + timings.time("hasNext"),
      count
    )
    sw.writeStat(stat)
  }
}

private class AccumuloFeatureReaderDelegate(iter: SFIter, query: Query, qp: QueryPlanner)(implicit timings: Timings)
    extends FeatureReader[SimpleFeatureType, SimpleFeature] with MethodProfiling {

  def isValid(feature: SimpleFeature): Boolean = if (feature == null) {
    false
  } else {
    val geom = feature.getDefaultGeometry()
    if (geom == null) {
      false
    } else {
      geom.asInstanceOf[Geometry].isValid()
    }
  }

  // mutable pre-fetch
  var nextFeature: SimpleFeature = getNextFeature()

  override def getFeatureType: SimpleFeatureType = query.getHints.getReturnSft

  // simple pass-through
  override def next(): SimpleFeature = {
    val returnValue = nextFeature
    nextFeature = getNextFeature()
    returnValue
  }

  // pre-fetch check
  override def hasNext: Boolean = (nextFeature != null)

  // does the real work of fetching the next (valid) feature; null otherwise
  def getNextFeature(): SimpleFeature = {
    // ensure this is reasonable
    if (!delegateHasNext) return null

    // intentionally mutable state
    var candidate = profile(iter.next(), "next")

    // skip all invalid records until you either exhaust the iterator or find something acceptable
    while (!isValid(candidate)) {
      // TODO:  change to logging?
      println(s"[ERROR] AccumuloFeatureReaderDelegate.getNextFeature():  Invalid geometry\n  candidate:  $candidate")
      if (candidate != null) {
        println(s"  candidate geometry:  ${candidate.getDefaultGeometry}")
        if (candidate.getDefaultGeometry != null) {
          println(s"  is candidate geometry valid?  ${candidate.getDefaultGeometry.asInstanceOf[Geometry].isValid}")
        }
      }

      // bail, if there are no more records
      if (!delegateHasNext) return null

      // fetch the next candidate
      candidate = profile(iter.next(), "next")
    }

    // if you get this far, then you have necessarily found a valid candidate
    candidate
  }

  def delegateHasNext: Boolean = profile(iter.hasNext, "hasNext")

  override def close(): Unit = iter.close()

  def getCount: Int = timings.occurrences("next").toInt
}

private class AccumuloFeatureReaderBinDelegate(iter: SFIter, query: Query, qp: QueryPlanner)(implicit timings: Timings)
    extends AccumuloFeatureReaderDelegate(iter, query, qp)(timings) {

  import org.locationtech.geomesa.accumulo.iterators.BinAggregatingIterator.BIN_ATTRIBUTE_INDEX

  private var count = 0
  private val bytesPerHit = if (query.getHints.getBinLabelField.isDefined) 24 else 16

  override def next(): SimpleFeature = {
    val sf = super.next()
    
    count += (sf.getAttribute(BIN_ATTRIBUTE_INDEX).asInstanceOf[Array[Byte]].length / bytesPerHit)
    sf
  }

  override def getCount: Int = count
}
