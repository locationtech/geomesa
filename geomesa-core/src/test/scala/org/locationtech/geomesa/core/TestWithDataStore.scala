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

package org.locationtech.geomesa.core

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.DataStoreFinder
import org.geotools.feature.DefaultFeatureCollection
import org.locationtech.geomesa.core.data.{AccumuloDataStore, AccumuloFeatureStore}
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConverters._

/**
 * Trait to simplify tests that require reading and writing features from an AccumuloDataStore
 */
trait TestWithDataStore {

  def spec: String
  def dtgField: String = "dtg"

  /**
   * Return the features you want to populate the data store with
   *
   * @return
   */
  def getTestFeatures(): Seq[SimpleFeature]

  // we use class name to prevent spillage between unit tests in the mock connector
  val sftName = getClass.getSimpleName
  lazy val sft = {
    val sft = SimpleFeatureTypes.createType(sftName, spec)
    sft.getUserData.put(SF_PROPERTY_START_TIME, dtgField)
    sft
  }

  lazy val connector = new MockInstance("mycloud").getConnector("user", new PasswordToken("password"))

  lazy val ds = {
    val ds = DataStoreFinder.getDataStore(Map("connector" -> connector,
      // note the table needs to be different to prevent testing errors
      "tableName" -> sftName).asJava).asInstanceOf[AccumuloDataStore]
    ds.createSchema(sft)
    ds
  }

  lazy val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

  /**
   * Call to load the test features into the data store
   *
   * @return
   */
  def populateFeatures = {
    val featureCollection = new DefaultFeatureCollection(sftName, sft)
    getTestFeatures().foreach(featureCollection.add)
    // write the feature to the store
    fs.addFeatures(featureCollection)
  }
}
