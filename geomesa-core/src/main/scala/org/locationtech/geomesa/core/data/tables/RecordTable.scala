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

package org.locationtech.geomesa.core.data.tables

import org.apache.accumulo.core.client.BatchWriter
import org.apache.accumulo.core.data.{Mutation, Value}
import org.apache.accumulo.core.security.ColumnVisibility
import org.locationtech.geomesa.core.data._
import org.locationtech.geomesa.feature.SimpleFeatureEncoder
import org.opengis.feature.simple.SimpleFeature

// TODO: Implement as traits and cache results to gain flexibility and speed-up.
// https://geomesa.atlassian.net/browse/GEOMESA-344
object RecordTable extends GeoMesaTable {

  type Visibility = String
  type Feature2Mutation = (SimpleFeature, Visibility) => Mutation

  def buildWrite(encoder: SimpleFeatureEncoder, rowIdPrefix: String): (SimpleFeature, Visibility) => Mutation =
    (feature: SimpleFeature, visibility: Visibility) => {
      val m = new Mutation(getRowKey(rowIdPrefix, feature.getID))
      m.put(SFT_CF, EMPTY_COLQ, new ColumnVisibility(visibility), new Value(encoder.encode(feature)))
      m
    }

  def buildDelete(encoder: SimpleFeatureEncoder, rowIdPrefix: String): Feature2Mutation =
    (feature: SimpleFeature, visibility: Visibility) => {
      val m = new Mutation(getRowKey(rowIdPrefix, feature.getID))
      m.putDelete(SFT_CF, EMPTY_COLQ, new ColumnVisibility(visibility))
      m
    }

  /** Creates a function to write a feature to the Record Table **/
  def recordWriter(bw: BatchWriter, encoder: SimpleFeatureEncoder, rowIdPrefix: String) = {
    val builder = buildWrite(encoder, rowIdPrefix)
    (feature: SimpleFeature, visibility: Visibility) => bw.addMutation(builder(feature, visibility))
  }

  def recordDeleter(bw: BatchWriter, encoder: SimpleFeatureEncoder, rowIdPrefix: String) = {
    val builder = buildDelete(encoder, rowIdPrefix)
    (feature: SimpleFeature, visibility: Visibility) => bw.addMutation(builder(feature, visibility))
  }

  def getRowKey(rowIdPrefix: String, id: String): String = rowIdPrefix + id
}
