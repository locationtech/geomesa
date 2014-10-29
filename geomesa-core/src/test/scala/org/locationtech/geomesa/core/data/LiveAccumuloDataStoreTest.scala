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

package org.locationtech.geomesa.core.data

import org.geotools.data._
import org.geotools.data.collection.ListFeatureCollection
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.factory.Hints
import org.geotools.filter.text.cql2.CQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.SimpleFeatureType
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class LiveAccumuloDataStoreTest extends Specification {

  sequential

  /**
   * WARNING: this test runs against a live accumulo instance and drops the table you run against
   */

  val params = Map(
    "instanceId"        -> "mycloud",
    "zookeepers"        -> "zoo1,zoo2,zoo3",
    "user"              -> "user",
    "password"          -> "password",
    "auths"             -> "user,admin",
    "visibilities"      -> "user&admin",
    "tableName"         -> "test_auths",
    "useMock"           -> "false",
    "featureEncoding"   -> "avro")

  val sftName = "authwritetest"

  def getDataStore: AccumuloDataStore = {
    DataStoreFinder.getDataStore(params).asInstanceOf[AccumuloDataStore]
  }

  def createSimpleFeatureType: SimpleFeatureType = {
    SimpleFeatureTypes.createType(sftName, s"name:String,dtg:Date,*geom:Point:srid=4326")
  }

  def initializeDataStore(ds: AccumuloDataStore): Unit = {
    // truncate the table, if it exists
    if (ds.connector.tableOperations.exists(params("tableName"))) {
      ds.connector.tableOperations.deleteRows(params("tableName"), null, null)
    }

    // create the schema
    ds.createSchema(createSimpleFeatureType)
  }

  def writeSampleData(ds: AccumuloDataStore): Unit = {
    // write some data
    val sft = createSimpleFeatureType
    val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]
    val written = fs.addFeatures(new ListFeatureCollection(sft, getFeatures(sft).toList))
  }

  def getFeatures(sft: SimpleFeatureType) = {
    val builder = AvroSimpleFeatureFactory.featureBuilder(sft)
    (0 until 6).map { i =>
      builder.reset()
      builder.set("geom", WKTUtils.read("POINT(45.0 45.0)"))
      builder.set("dtg", "2012-01-02T05:06:07.000Z")
      builder.set("name",i.toString)
      val sf = builder.buildFeature(i.toString)
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      sf
    }
  }

  def printFeatures(features: SimpleFeatureIterator): Unit = {
    val resultItr = new Iterator[String] {
      def hasNext = {
        val next = features.hasNext
        if (!next)
          features.close
        next
      }

      def next = features.next.getProperty("name").getValue.toString
    }
    println(resultItr.toList + "\n")
  }

  "AccumuloDataStore" should {

    "restrict users with insufficient auths from writing data" in {

      skipped("Meant for integration testing")

      val ds = getDataStore
//      initializeDataStore(ds)
//      writeSampleData(ds)

      val query = new Query(sftName, CQL.toFilter("INCLUDE"))

      // get the feature store used to query the GeoMesa data
      val featureStore = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      // execute the query
      val results = featureStore.getFeatures(query)

      // loop through all results
      printFeatures(results.features)

      success
    }
  }

}
