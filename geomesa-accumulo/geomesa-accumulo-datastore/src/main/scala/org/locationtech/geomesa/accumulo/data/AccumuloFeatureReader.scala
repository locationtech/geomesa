/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.data

import java.util.concurrent.atomic.AtomicBoolean

import org.geotools.data.{FeatureReader, Query}
import org.locationtech.geomesa.accumulo.index.QueryHints.RichHints
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.accumulo.stats._
import org.locationtech.geomesa.utils.stats.{MethodProfiling, NoOpTimings, TimingsImpl}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class AccumuloFeatureReader(queryPlanner: QueryPlanner, val query: Query, dataStore: AccumuloDataStore)
    extends FeatureReader[SimpleFeatureType, SimpleFeature] with MethodProfiling {

  private val start = System.currentTimeMillis()
  private val closed = new AtomicBoolean(false)

  ThreadManagement.register(this, start, dataStore.queryTimeoutMillis)

  private val writeStats = dataStore.isInstanceOf[StatWriter]
  implicit val timings = if (writeStats) new TimingsImpl else NoOpTimings

  private val iter = profile(queryPlanner.runQuery(query), "planning")

  override def next() = profile(iter.next(), "next")

  override def hasNext = profile(iter.hasNext, "hasNext")

  override def close() = if (!closed.getAndSet(true)) {
    iter.close()
    ThreadManagement.unregister(this, start, dataStore.queryTimeoutMillis)
    if (writeStats) {
      val stat = QueryStat(queryPlanner.sft.getTypeName,
          System.currentTimeMillis(),
          QueryStatTransform.filterToString(query.getFilter),
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
