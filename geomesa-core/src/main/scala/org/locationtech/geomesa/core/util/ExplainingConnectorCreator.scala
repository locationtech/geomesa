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

package org.locationtech.geomesa.core.util

import org.apache.accumulo.core.client.{IteratorSetting, BatchScanner, Scanner}
import org.apache.accumulo.core.data.Range
import org.locationtech.geomesa.core.data.{AccumuloDataStore, AccumuloConnectorCreator, INTERNAL_GEOMESA_VERSION}
import org.locationtech.geomesa.core.index.ExplainerOutputType
import org.locationtech.geomesa.core.iterators.ScanConfig
import org.opengis.feature.simple.SimpleFeatureType


class ExplainingConnectorCreator(ds: AccumuloDataStore, output: ExplainerOutputType)
    extends AccumuloConnectorCreator {

  var table: String = null
  var config: ExplainingConfig = null

  /**
   * Create a BatchScanner for the SpatioTemporal Index Table
   *
   * @param numThreads number of threads for the BatchScanner
   */
  override def createSpatioTemporalIdxScanner(sft: SimpleFeatureType, numThreads: Int): BatchScanner = {
    table = ds.getSpatioTemporalIdxTableName(sft)
    val s = new ExplainingBatchScanner(output)
    config = s
    s
  }

  /**
   * Create a BatchScanner for the SpatioTemporal Index Table
   */
  override def createSTIdxScanner(sft: SimpleFeatureType): BatchScanner = {
    table = ds.getSpatioTemporalIdxTableName(sft)
    val s = new ExplainingBatchScanner(output)
    config = s
    s
  }

  /**
   * Create a Scanner for the Attribute Table (Inverted Index Table)
   */
  override def createAttrIdxScanner(sft: SimpleFeatureType): Scanner = {
    table = ds.getAttrIdxTableName(sft)
    val s= new ExplainingScanner(output)
    config = s
    s
  }

  /**
   * Create a BatchScanner to retrieve only Records (SimpleFeatures)
   */
  override def createRecordScanner(sft: SimpleFeatureType, numThreads: Int): BatchScanner = {
    table = ds.getRecordTableForType(sft)
    val s = new ExplainingBatchScanner(output)
    config = s
    s
  }

  override def getGeomesaVersion(sft: SimpleFeatureType): Int = ds.getGeomesaVersion(sft)

  // TODO what should we do in the attribute join case? doesn't really translate to an input format...
  def getScanConfig(): ScanConfig =
    ScanConfig(table, config.getRanges(), config.getIterators(), config.getColumnFamilies())
}
