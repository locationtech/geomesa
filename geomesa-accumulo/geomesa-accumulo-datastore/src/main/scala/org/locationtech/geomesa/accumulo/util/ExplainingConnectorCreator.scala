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

package org.locationtech.geomesa.accumulo.util

import org.locationtech.geomesa.accumulo.data.{AccumuloConnectorCreator, AccumuloDataStore}
import org.locationtech.geomesa.accumulo.index.ExplainerOutputType
import org.opengis.feature.simple.SimpleFeatureType


class ExplainingConnectorCreator(ds: AccumuloDataStore, output: ExplainerOutputType)
    extends AccumuloConnectorCreator {

  override def getBatchScanner(table: String, numThreads: Int) = new ExplainingBatchScanner(output)

  override def getScanner(table: String) = new ExplainingScanner(output)

  override def getSpatioTemporalTable(sft: SimpleFeatureType) = ds.getSpatioTemporalTable(sft)

  override def getZ3Table(sft: SimpleFeatureType): String = ds.getZ3Table(sft)

  override def getAttributeTable(sft: SimpleFeatureType) = ds.getAttributeTable(sft)

  override def getRecordTable(sft: SimpleFeatureType) = ds.getRecordTable(sft)

  override def getSuggestedSpatioTemporalThreads(sft: SimpleFeatureType) =
    ds.getSuggestedSpatioTemporalThreads(sft)

  override def getSuggestedRecordThreads(sft: SimpleFeatureType) = ds.getSuggestedRecordThreads(sft)

  override def getSuggestedAttributeThreads(sft: SimpleFeatureType) = ds.getSuggestedAttributeThreads(sft)

  override def getSuggestedZ3Threads(sft: SimpleFeatureType) = ds.getSuggestedZ3Threads(sft)

  override def getGeomesaVersion(sft: SimpleFeatureType): Int = ds.getGeomesaVersion(sft)
}
