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
import org.opengis.feature.simple.SimpleFeature

object RecordTable extends GeoMesaTable {

  def buildWrite(encoder: SimpleFeatureEncoder, visibility: String, rowIdPrefix: String): SimpleFeature => Mutation =
    (feature: SimpleFeature) => {
      val m = new Mutation(rowIdPrefix + feature.getID)
      m.put(SFT_CF, EMPTY_COLQ, new ColumnVisibility(visibility), new Value(encoder.encode(feature)))
      m
  }

  def buildDelete(encoder: SimpleFeatureEncoder, visibility: String, rowIdPrefix: String): SimpleFeature => Mutation =
    (feature: SimpleFeature) => {
      val m = new Mutation(rowIdPrefix + feature.getID)
      m.putDelete(SFT_CF, EMPTY_COLQ, new ColumnVisibility(visibility))
      m
    }

  /** Creates a function to write a feature to the Record Table **/
  def recordWriter(bw: BatchWriter, encoder: SimpleFeatureEncoder, visibility: String, rowIdPrefix: String): SimpleFeature => Unit = {
    (feature: SimpleFeature) => {
      val builder = buildWrite(encoder, visibility, rowIdPrefix)
      bw.addMutation(builder(feature))
    }
  }

  def recordDeleter(bw: BatchWriter, encoder: SimpleFeatureEncoder, visibility: String, rowIdPrefix: String): SimpleFeature => Unit =
    (feature: SimpleFeature) => {
      val builder = buildDelete(encoder, visibility, rowIdPrefix)
      bw.addMutation(builder(feature))
    }

}
