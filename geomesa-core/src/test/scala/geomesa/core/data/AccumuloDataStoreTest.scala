/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.core.data

import collection.JavaConversions._
import geomesa.utils.text.WKTUtils
import org.geotools.data.{Query, DataUtilities, Transaction, DataStoreFinder}
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.cql2.CQL
import geomesa.core.index._
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class AccumuloDataStoreTest extends Specification {

  val geotimeAttributes = IndexEntryType.getTypeSpec

  def createStore: AccumuloDataStore =
    // the specific parameter values should not matter, as we
    // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(Map(
      "instanceId" -> "mycloud",
      "zookeepers" -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"->"myuser",
      "password"->"mypassword",
      "auths"->"A,B,C",
      "tableName"->"testwrite",
      "useMock"->"true")).asInstanceOf[AccumuloDataStore]

  "AccumuloDataStore" should {
    "be accessible through DataStoreFinder" in {
      val ds = createStore
      ds should not be null
    }
  }

  "AccumuloDataStore" should {
    "provide ability to create a new store" in {
      val ds = createStore
      val sft = DataUtilities.createType("testType",
        s"NAME:String,$geotimeAttributes")
      ds.createSchema(sft)
      val tx = Transaction.AUTO_COMMIT
      val fw = ds.getFeatureWriterAppend("testType", tx)
      val liveFeature = fw.next()
      liveFeature.setDefaultGeometry(WKTUtils.read("POINT(45.0 49.0)"))
      fw.write()
      tx.commit()
    }
  }

  "AccumuloDataStore" should {
    "provide ability to write using the feature source and read what it wrote" in {
      // create the data store
      val ds = createStore
      val sftName = "testType"
      val sft = DataUtilities.createType(sftName,
        s"NAME:String,$geotimeAttributes")
      ds.createSchema(sft)
      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      // create a feature
      val liveFeature = SimpleFeatureBuilder.build(sft, List(), "fid-1")
      val geom = WKTUtils.read("POINT(45.0 49.0)")
      liveFeature.setDefaultGeometry(geom)

      // make sure we ask the system to re-use the provided feature-ID
      liveFeature.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      featureCollection.add(liveFeature)

      // write the feature to the store
      val res = fs.addFeatures(featureCollection)

      // compose a CQL query that uses a reasonably-sized polygon for searching
      val cqlFilter = CQL.toFilter(s"BBOX($SF_PROPERTY_GEOMETRY, 44.9,48.9,45.1,49.1)")
      val query = new Query(sftName, cqlFilter)

      // Let's read out what we wrote.
      val results = fs.getFeatures(query)
      val features = results.features
      var containsGeometry = false

      while(features.hasNext) {
        containsGeometry = containsGeometry | features.next.getDefaultGeometry.equals(geom)
      }

      results.getSchema should be equalTo(sft)
      containsGeometry should be equalTo(true)
      res.length should be equalTo(1)
    }

    "return an empty iterator correctly" in {
      // create the data store
      val ds = createStore
      val sftName = "testType"
      val sft = DataUtilities.createType(sftName,
        s"NAME:String,$geotimeAttributes")
      ds.createSchema(sft)
      val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

      // create a feature
      val liveFeature = SimpleFeatureBuilder.build(sft, List(), "fid-1")
      val geom = WKTUtils.read("POINT(45.0 49.0)")
      liveFeature.setDefaultGeometry(geom)

      // make sure we ask the system to re-use the provided feature-ID
      liveFeature.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE

      val featureCollection = new DefaultFeatureCollection(sftName, sft)

      featureCollection.add(liveFeature)

      // write the feature to the store
      val res = fs.addFeatures(featureCollection)

      // compose a CQL query that uses a polygon that is disjoint with the feature bounds
      val cqlFilter = CQL.toFilter(s"BBOX($SF_PROPERTY_GEOMETRY, 64.9,68.9,65.1,69.1)")
      val query = new Query(sftName, cqlFilter)

      // Let's read out what we wrote.
      val results = fs.getFeatures(query)
      val features = results.features
      results.getSchema should be equalTo(sft)
      res.length should be equalTo(1)
      features.hasNext should be equalTo(false)
    }
  }
}
