/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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
package org.locationtech.geomesa.kafka

import org.opengis.feature.simple.SimpleFeatureType

/** This class is intended to manage external resources related to KafkaDataStore SimpleFeatureTypes. */
class KafkaDataStoreSFTManager(zookeepers: String,
                               zkPath: String) {

/* create, list, get, delete - implementations to come...*/

  def createType(sft: SimpleFeatureType) : Unit  = { ??? }

  def getType(name: String) : SimpleFeatureType= { ??? }

  def list() : Seq[String] = { ??? }

  def deleteType(name: String) = { ??? }

}
