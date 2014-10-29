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
import org.locationtech.geomesa.core.stats.{MethodProfiling, QueryStat, QueryStatTransform, StatWriter}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class AccumuloFeatureReader(dataStore: AccumuloDataStore,
                            query: Query,
                            indexSchemaFmt: String,
                            sft: SimpleFeatureType,
                            featureEncoder: SimpleFeatureEncoder)
  extends FeatureReader[SimpleFeatureType, SimpleFeature] with MethodProfiling {

  private var scanTime = 0L
  private var hitsSeen = 0

  private val indexSchema = IndexSchema(indexSchemaFmt, sft, featureEncoder)
  private val queryPlanner = indexSchema.planner

  def explainQuery(q: Query = query, o: ExplainerOutputType = ExplainPrintln) {
    val (_, explainTime) = profile {
      indexSchema.explainQuery(q, o)
    }
    o(s"Query Planning took $explainTime milliseconds.")
  }

  private lazy val (iter, planningTime) = profile {
    queryPlanner.query(query, dataStore)
  }

  override def getFeatureType = sft

  override def next() = {
    val (result, time) = profile(iter.next())
    scanTime += time
    hitsSeen += 1
    result
  }

  override def hasNext = {
    val (result, time) = profile(iter.hasNext)
    scanTime += time
    result
  }

  override def close() = {
    iter.close()

    dataStore match {
      case sw: StatWriter =>
        val stat = QueryStat(sft.getTypeName,
                             System.currentTimeMillis(),
                             QueryStatTransform.filterToString(query.getFilter),
                             QueryStatTransform.hintsToString(query.getHints),
                             planningTime,
                             scanTime - planningTime, // planning time gets added to scan time due to lazy val... Revisit in GEOMESA-408.
                             hitsSeen)
        sw.writeStat(stat, dataStore.getQueriesTableName(sft))
      case _ => // do nothing
    }
  }
}
