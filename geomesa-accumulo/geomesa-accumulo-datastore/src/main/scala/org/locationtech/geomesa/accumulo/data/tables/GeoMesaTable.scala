/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.accumulo.data.tables

import org.apache.accumulo.core.client.{BatchDeleter, Connector}
import org.apache.accumulo.core.data.{Range => AccRange}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.index._
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConversions._

trait GeoMesaTable {

  def deleteFeaturesFromTable(conn: Connector, bd: BatchDeleter, sft: SimpleFeatureType): Unit = {
    val MIN_START = "\u0000"
    val MAX_END = "~"

    val prefix = getTableSharingPrefix(sft)

    //val range = new data.Range(prefix + MIN_START, prefix + MAX_END)
    val range = new AccRange(new Text(prefix), true, AccRange.followingPrefix(new Text(prefix)), false)

    bd.setRanges(Seq(range))
    bd.delete()
    bd.close()
  }

}
