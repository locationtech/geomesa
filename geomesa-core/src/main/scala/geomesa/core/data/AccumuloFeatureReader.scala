/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.core.data

import geomesa.core.index._
import geomesa.core.stats.{StatWriter, QueryStat}
import geomesa.core.util.CloseableIterator
import org.geotools.data.{Query, FeatureReader}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

class AccumuloFeatureReader(dataStore: AccumuloDataStore,
                            query: Query,
                            indexSchemaFmt: String,
                            sft: SimpleFeatureType,
                            featureEncoder: SimpleFeatureEncoder)
  extends FeatureReader[SimpleFeatureType, SimpleFeature] {

  private val planningStartTime = System.currentTimeMillis()
  val indexSchema = IndexSchema(indexSchemaFmt, sft, featureEncoder)
  val iter = indexSchema.query(query, dataStore)
  private val planningTime = (System.currentTimeMillis() - planningStartTime)

  private var scanTime = 0L
  private var hitsSeen = 0

  override def getFeatureType = sft

  override def next() = {
    val time = System.currentTimeMillis()
    val tmp = iter.next()
    scanTime += (System.currentTimeMillis() - time)
    hitsSeen += 1
    tmp
  }

  override def hasNext = {
    val time = System.currentTimeMillis()
    val tmp = iter.hasNext
    scanTime += (System.currentTimeMillis() - time)
    tmp
  }

  override def close() = {
    iter.close()
    if (dataStore.isInstanceOf[StatWriter]) {
      val stat = QueryStat(dataStore.catalogTable,
                              sft.getTypeName,
                              System.currentTimeMillis(),
                              query.getFilter,
                              planningTime,
                              scanTime,
                              hitsSeen)
      dataStore.asInstanceOf[StatWriter].writeStat(stat)
    }
  }
}
