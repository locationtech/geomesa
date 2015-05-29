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

package org.locationtech.geomesa.accumulo.process.proximity

import com.vividsolutions.jts.geom.Coordinate
import org.geotools.data.DataStoreFinder
import org.geotools.factory.Hints
import org.geotools.feature.DefaultFeatureCollection
import org.geotools.geometry.jts.JTSFactoryFinder
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.{AccumuloDataStore, AccumuloFeatureStore}
import org.locationtech.geomesa.accumulo.index.Constants
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, SimpleFeatureTypes}
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class ProximitySearchProcessTest extends Specification {

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

     val sftName = "geomesaProximityTestType"
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

    val geoFactory = JTSFactoryFinder.getGeometryFactory

    def getPoint(lat: Double, lon: Double, meters: Double) =
      GeometryUtils.farthestPoint(geoFactory.createPoint(new Coordinate(lat, lon)), meters)

   "GeomesaProximityQuery" should {
     "find things close by" in {
       import org.locationtech.geomesa.utils.geotools.Conversions._
       val p1 = getPoint(45, 45, 99)
       WKTUtils.read("POINT(45 45)").bufferMeters(99.1).intersects(p1) must beTrue
       WKTUtils.read("POINT(45 45)").bufferMeters(100).intersects(p1) must beTrue
       WKTUtils.read("POINT(45 45)").bufferMeters(98).intersects(p1) must beFalse
       val p2 = getPoint(46, 46, 99)
       val p3 = getPoint(47, 47, 99)


       val inputFeatures = new DefaultFeatureCollection(sftName, sft)
       List(1,2,3).zip(List(p1, p2, p3)).foreach  { case (i, p) =>
         val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), i.toString)
         sf.setDefaultGeometry(p)
         sf.setAttribute(org.locationtech.geomesa.accumulo.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
         sf.setAttribute("type", "fake")
         sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
         inputFeatures.add(sf)
       }

       val dataFeatures = fs.getFeatures()

       dataFeatures.size should be equalTo 8
       val prox = new ProximitySearchProcess
       prox.execute(inputFeatures, dataFeatures, 50).size should be equalTo 0
       prox.execute(inputFeatures, dataFeatures, 90).size should be equalTo 0
       prox.execute(inputFeatures, dataFeatures, 99.1).size should be equalTo 6
       prox.execute(inputFeatures, dataFeatures, 100).size should be equalTo 6
       prox.execute(inputFeatures, dataFeatures, 101).size should be equalTo 6
     }
   }

  "GeomesaProximityQuery" should {
    "work on non-accumulo feature sources" in {
      import org.locationtech.geomesa.utils.geotools.Conversions._
      val p1 = getPoint(45, 45, 99)
      WKTUtils.read("POINT(45 45)").bufferMeters(99.1).intersects(p1) must beTrue
      WKTUtils.read("POINT(45 45)").bufferMeters(100).intersects(p1) must beTrue
      WKTUtils.read("POINT(45 45)").bufferMeters(98).intersects(p1) must beFalse
      val p2 = getPoint(46, 46, 99)
      val p3 = getPoint(47, 47, 99)


      val inputFeatures = new DefaultFeatureCollection(sftName, sft)
      List(1,2,3).zip(List(p1,p2,p3)).foreach  { case (i, p) =>
        val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), i.toString)
        sf.setDefaultGeometry(p)
        sf.setAttribute(org.locationtech.geomesa.accumulo.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
        sf.setAttribute("type", "fake")
        sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
        inputFeatures.add(sf)
      }

      val nonAccumulo = new DefaultFeatureCollection(sftName, sft)

      List("a", "b").foreach { name =>
        List(1, 2, 3, 4).zip(List(45, 46, 47, 48)).foreach { case (i, lat) =>
          val sf = AvroSimpleFeatureFactory.buildAvroFeature(sft, List(), name + i.toString)
          sf.setDefaultGeometry(WKTUtils.read(f"POINT($lat%d $lat%d)"))
          sf.setAttribute(org.locationtech.geomesa.accumulo.process.tube.DEFAULT_DTG_FIELD, new DateTime("2011-01-01T00:00:00Z", DateTimeZone.UTC).toDate)
          sf.setAttribute("type", name)
          sf.getUserData()(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
          nonAccumulo.add(sf)
        }
      }

      nonAccumulo.size should be equalTo 8
      val prox = new ProximitySearchProcess
      prox.execute(inputFeatures, nonAccumulo, 30).size should be equalTo 0
      prox.execute(inputFeatures, nonAccumulo, 98).size should be equalTo 0
      prox.execute(inputFeatures, nonAccumulo, 99.0001).size should be equalTo 6
      prox.execute(inputFeatures, nonAccumulo, 100).size should be equalTo 6
      prox.execute(inputFeatures, nonAccumulo, 101).size should be equalTo 6
    }
  }

 }
