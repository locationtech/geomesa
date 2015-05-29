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

package org.locationtech.geomesa.accumulo.data

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.{BatchScanner, Scanner}
import org.opengis.feature.simple.SimpleFeatureType

trait AccumuloConnectorCreator extends Logging {

  /**
   * Get the name of the spatio temporal index table in accumulo for the given feature
   */
  def getSpatioTemporalTable(sft: SimpleFeatureType): String

  /**
   * Get the name of the Z3 index table in accumulo for the given feature
   */
  def getZ3Table(sft: SimpleFeatureType): String

  /**
   * Get the name of the attribute index table in accumulo for the given feature
   */
  def getAttributeTable(sft: SimpleFeatureType): String

  /**
   * Get the name of the records table in accumulo for the given feature
   */
  def getRecordTable(sft: SimpleFeatureType): String

  /**
   * Gets a suggested number of threads for querying the spatio temporal index for the given feature
   */
  def getSuggestedSpatioTemporalThreads(sft: SimpleFeatureType): Int

  /**
   * Gets a suggested number of threads for querying the attribute index for the given feature
   */
  def getSuggestedAttributeThreads(sft: SimpleFeatureType): Int

  /**
   * Gets a suggested number of threads for querying the record table for the given feature
   */
  def getSuggestedRecordThreads(sft: SimpleFeatureType): Int

  /**
   * Gets a suggested number of threads for querying the z3 table for the given feature
   */
  def getSuggestedZ3Threads(sft: SimpleFeatureType): Int

  /**
   * Gets a single-range scanner for the given table
   */
  def getScanner(table: String): Scanner

  /**
   * Gets a batch scanner for the given table
   */
  def getBatchScanner(table: String, numThreads: Int): BatchScanner

  /**
   * Gets the geomesa internal version for the feature type
   *
   * @param sft
   * @return
   */
  def getGeomesaVersion(sft: SimpleFeatureType): Int
}
