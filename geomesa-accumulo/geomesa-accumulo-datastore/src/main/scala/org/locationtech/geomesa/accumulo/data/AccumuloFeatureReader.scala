/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.util.concurrent.atomic.AtomicBoolean

import org.geotools.data.{FeatureReader, Query}
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index.QueryPlanner.SFIter
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.stats._
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.utils.stats.{Timings, MethodProfiling, TimingsImpl}
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
    timeout.foreach(t => ThreadManagement.unregister(this, start, t))
    closeOnce()
  }
}

object AccumuloFeatureReader extends MethodProfiling {
  def apply(query: Query, qp: QueryPlanner, timeout: Option[Long], writer: Option[StatWriter]) = {
    writer match {
      case None => new AccumuloFeatureReaderImpl(query, qp, timeout)
      case Some(sw) => new AccumuloFeatureReaderWithStats(query, qp, timeout, sw)
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
                                     sw: StatWriter)
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

  override def getFeatureType: SimpleFeatureType = query.getHints.getReturnSft
  override def next(): SimpleFeature = profile(iter.next(), "next")
  override def hasNext: Boolean = profile(iter.hasNext, "hasNext")
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