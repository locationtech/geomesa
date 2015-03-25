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

package org.locationtech.geomesa.core.data

import org.geotools.data.{FeatureReader, Query}
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.core.stats._
import org.locationtech.geomesa.feature.FeatureEncoding.FeatureEncoding
import org.locationtech.geomesa.utils.stats.{MethodProfiling, TimingsImpl}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class AccumuloFeatureReader(queryPlanner: QueryPlanner, query: Query, dataStore: AccumuloDataStore)
    extends FeatureReader[SimpleFeatureType, SimpleFeature] with MethodProfiling {

  implicit val timings = new TimingsImpl

  private val iter = profile(queryPlanner.query(query), "planning")

  override def getFeatureType = queryPlanner.sft

  override def next() = profile(iter.next(), "next")

  override def hasNext = profile(iter.hasNext, "hasNext")

  override def close() = {
    iter.close()

    dataStore match {
      case sw: StatWriter =>
        val stat =
          QueryStat(queryPlanner.sft.getTypeName,
                    System.currentTimeMillis(),
                    QueryStatTransform.filterToString(query.getFilter),
                    QueryStatTransform.hintsToString(query.getHints),
                    timings.time("planning"),
                    timings.time("next") + timings.time("hasNext"),
                    timings.occurrences("next").toInt)
        sw.writeStat(stat, dataStore.getQueriesTableName(queryPlanner.sft))
      case _ => // do nothing
    }
  }
}
