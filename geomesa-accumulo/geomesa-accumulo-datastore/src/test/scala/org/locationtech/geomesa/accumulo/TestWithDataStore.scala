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

package org.locationtech.geomesa.accumulo

import org.apache.accumulo.core.client.mock.MockInstance
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.geotools.data.{Transaction, Query, DataStoreFinder}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeatureStore}
import org.locationtech.geomesa.accumulo.index._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

import scala.collection.JavaConverters._

/**
 * Trait to simplify tests that require reading and writing features from an AccumuloDataStore
 */
trait TestWithDataStore {

  def spec: String
  def dtgField: String = "dtg"

  // we use class name to prevent spillage between unit tests in the mock connector
  val sftName = getClass.getSimpleName
  lazy val sft = {
    val sft = SimpleFeatureTypes.createType(sftName, spec)
    sft.getUserData.put(SF_PROPERTY_START_TIME, dtgField)
    sft
  }

  lazy val connector = new MockInstance("mycloud").getConnector("user", new PasswordToken("password"))

  lazy val ds = {
    val ds = DataStoreFinder.getDataStore(Map(
      "connector" -> connector,
      "caching"   -> false,
      // note the table needs to be different to prevent testing errors
      "tableName" -> sftName).asJava).asInstanceOf[AccumuloDataStore]
    ds.createSchema(sft)
    ds
  }

  lazy val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

  /**
   * Call to load the test features into the data store
   */
  def addFeatures(features: Seq[SimpleFeature]): Unit = {
    val featureCollection = new DefaultFeatureCollection(sftName, sft)
    features.foreach { f =>
      f.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
      featureCollection.add(f)
    }
    // write the feature to the store
    fs.addFeatures(featureCollection)
  }

  def clearFeatures(): Unit = {
    val writer = ds.getFeatureWriter(sftName, Filter.INCLUDE, Transaction.AUTO_COMMIT)
    while (writer.hasNext) {
      writer.next()
      writer.remove()
    }
    writer.close()
  }

  def explain(query: Query): String = {
    val o = new ExplainString
    ds.explainQuery(sftName, query, o)
    o.toString()
  }
}
