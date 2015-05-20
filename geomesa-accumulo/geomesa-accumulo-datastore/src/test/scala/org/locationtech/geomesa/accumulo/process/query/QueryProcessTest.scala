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

package org.locationtech.geomesa.accumulo.process.query

import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.DataStoreFinder
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.filter.text.cql2.CQL
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeatureStore}
import org.locationtech.geomesa.accumulo.index.Constants
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class QueryProcessTest extends Specification {

  sequential

  val dtgField = org.locationtech.geomesa.accumulo.process.tube.DEFAULT_DTG_FIELD
  val geotimeAttributes = s"*geom:Geometry:srid=4326,$dtgField:Date"

  def createStore: AccumuloDataStore =
  // the specific parameter values should not matter, as we
  // are requesting a mock data store connection to Accumulo
    DataStoreFinder.getDataStore(Map(
      "instanceId"        -> "mycloud",
      "zookeepers"        -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"              -> "myuser",
      "password"          -> "mypassword",
      "auths"             -> "A,B,C",
      "tableName"         -> "testwrite",
      "useMock"           -> "true",
      "featureEncoding"   -> "avro")).asInstanceOf[AccumuloDataStore]

  val sftName = "geomesaQueryTestType"
  val sft = SimpleFeatureTypes.createType(sftName, s"type:String,$geotimeAttributes")
  sft.getUserData()(Constants.SF_PROPERTY_START_TIME) = dtgField

  val ds = createStore

  ds.createSchema(sft)
  val fs = ds.getFeatureSource(sftName).asInstanceOf[AccumuloFeatureStore]

  val featureCollection = new DefaultFeatureCollection(sftName, sft)

  List("a", "b").foreach { name =>
    List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
      val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
      sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
      sf.setAttribute(org.locationtech.geomesa.accumulo.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
      sf.setAttribute("type", name)
      sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      featureCollection.add(sf)
    }
  }

  // write the feature to the store
  val res = fs.addFeatures(featureCollection)


  "GeomesaQuery" should {
    "return things without a filter" in {
      val features = fs.getFeatures()

      val geomesaQuery = new QueryProcess
      val results = geomesaQuery.execute(features, null)

      val f = results.features()
      while (f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") should beOneOf("a", "b")
      }

      results.size mustEqual 8
    }

    "respect a parent filter" in {
      val features = fs.getFeatures(CQL.toFilter("type = 'b'"))

      val geomesaQuery = new QueryProcess
      val results = geomesaQuery.execute(features, null)

      val f = results.features()
      while (f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") mustEqual "b"
      }

      results.size mustEqual 4
    }

    "be able to use its own filter" in {
      val features = fs.getFeatures(CQL.toFilter("type = 'b' OR type = 'a'"))

      val geomesaQuery = new QueryProcess
      val results = geomesaQuery.execute(features, CQL.toFilter("type = 'a'"))

      val f = results.features()
      while (f.hasNext) {
        val sf = f.next
        sf.getAttribute("type") mustEqual "a"
      }

      results.size mustEqual 4
    }

    "properly query geometry" in {
      val features = fs.getFeatures()

      val geomesaQuery = new QueryProcess
      val results = geomesaQuery.execute(features, CQL.toFilter("bbox(geom, 45.0, 45.0, 46.0, 46.0)"))

      var poly = WKTUtils.read("POLYGON((45 45, 46 45, 46 46, 45 46, 45 45))")

      val f = results.features()
      while (f.hasNext) {
        val sf = f.next
        poly.intersects(sf.getDefaultGeometry.asInstanceOf[Geometry]) must beTrue
      }

      results.size mustEqual 4
    }
  }


}
