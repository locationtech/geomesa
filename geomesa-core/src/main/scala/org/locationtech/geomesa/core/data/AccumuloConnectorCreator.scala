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

package org.locationtech.geomesa.core.data

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.{BatchScanner, Scanner}
import org.opengis.feature.simple.SimpleFeatureType

trait AccumuloConnectorCreator extends Logging {

  /**
   * Create a BatchScanner for the SpatioTemporal Index Table
   *
   * @param numThreads number of threads for the BatchScanner
   */
  def createSpatioTemporalIdxScanner(sft: SimpleFeatureType, numThreads: Int): BatchScanner

  /**
   * Create a BatchScanner for the SpatioTemporal Index Table
   */
  def createSTIdxScanner(sft: SimpleFeatureType): BatchScanner

  /**
   * Create a Scanner for the Attribute Table (Inverted Index Table)
   */
  def createAttrIdxScanner(sft: SimpleFeatureType): Scanner

  /**
   * Create a BatchScanner to retrieve only Records (SimpleFeatures)
   */
  def createRecordScanner(sft: SimpleFeatureType, numThreads: Int = 0) : BatchScanner

  def catalogTableFormat(sft: SimpleFeatureType): Boolean
}
