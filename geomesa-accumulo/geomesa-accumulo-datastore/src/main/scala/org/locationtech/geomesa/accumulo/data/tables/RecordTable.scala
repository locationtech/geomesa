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

package org.locationtech.geomesa.accumulo.data.tables

import org.apache.accumulo.core.data.Mutation
import org.locationtech.geomesa.accumulo.data.AccumuloFeatureWriter._
import org.locationtech.geomesa.accumulo.data._
import org.opengis.feature.simple.SimpleFeatureType

// TODO: Implement as traits and cache results to gain flexibility and speed-up.
// https://geomesa.atlassian.net/browse/GEOMESA-344
object RecordTable extends GeoMesaTable {

  override def supports(sft: SimpleFeatureType) = true

  override val suffix: String = "records"

  override def writer(sft: SimpleFeatureType): Option[FeatureToMutations] = {
    val rowIdPrefix = org.locationtech.geomesa.accumulo.index.getTableSharingPrefix(sft)
    val fn = (toWrite: FeatureToWrite) => {
      val m = new Mutation(getRowKey(rowIdPrefix, toWrite.feature.getID))
      m.put(SFT_CF, EMPTY_COLQ, toWrite.columnVisibility, toWrite.dataValue)
      Seq(m)
    }
    Some(fn)
  }

  override def remover(sft: SimpleFeatureType): Option[FeatureToMutations] = {
    val rowIdPrefix = org.locationtech.geomesa.accumulo.index.getTableSharingPrefix(sft)
    val fn = (toWrite: FeatureToWrite) => {
      val m = new Mutation(getRowKey(rowIdPrefix, toWrite.feature.getID))
      m.putDelete(SFT_CF, EMPTY_COLQ, toWrite.columnVisibility)
      Seq(m)
    }
    Some(fn)
  }

  def getRowKey(rowIdPrefix: String, id: String): String = rowIdPrefix + id
}
