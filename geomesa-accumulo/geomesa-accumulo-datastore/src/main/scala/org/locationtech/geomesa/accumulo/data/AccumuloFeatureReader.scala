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
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.stats._
import org.locationtech.geomesa.filter.filterToString
import org.locationtech.geomesa.utils.stats.{MethodProfiling, NoOpTimings, TimingsImpl}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class AccumuloFeatureReader(queryPlanner: QueryPlanner, val query: Query, dataStore: AccumuloDataStore)
    extends FeatureReader[SimpleFeatureType, SimpleFeature] with MethodProfiling {

  private val start = System.currentTimeMillis()
  private val closed = new AtomicBoolean(false)

  dataStore.queryTimeoutMillis.foreach(timeout => ThreadManagement.register(this, start, timeout))

  private val writeStats = dataStore.isInstanceOf[StatWriter]
  implicit val timings = if (writeStats) new TimingsImpl else NoOpTimings

  private val iter = profile(queryPlanner.runQuery(query), "planning")

  override def next() = profile(iter.next(), "next")

  override def hasNext = profile(iter.hasNext, "hasNext")

  override def close() = if (!closed.getAndSet(true)) {
    iter.close()
    dataStore.queryTimeoutMillis.foreach(timeout => ThreadManagement.unregister(this, start, timeout))
    if (writeStats) {
      val stat = QueryStat(queryPlanner.sft.getTypeName,
          System.currentTimeMillis(),
          filterToString(query.getFilter),
          QueryStatTransform.hintsToString(query.getHints),
          timings.time("planning"),
          timings.time("next") + timings.time("hasNext"),
          timings.occurrences("next").toInt)
      dataStore.asInstanceOf[StatWriter].writeStat(stat, dataStore.getQueriesTableName(queryPlanner.sft))
    }
  }

  override def getFeatureType = query.getHints.getReturnSft

  def isClosed: Boolean = closed.get()
}
