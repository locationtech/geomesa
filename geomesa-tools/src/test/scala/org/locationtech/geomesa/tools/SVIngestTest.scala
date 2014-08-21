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

package org.locationtech.geomesa.tools

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SVIngestTest extends Specification{

  sequential
  var id = 0

  var csvConfig = new IngestArguments(spec = "id:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326",
    idFields = None, dtField = Option("time"), lonAttribute = Option("lon"), latAttribute = Option("lat"),
    dtFormat = "yyyy-MM-dd", skipHeader = false, featureName = Option("test_type"),
    method = "local", file = "none", format = Some("CSV"))

  var csvWktConfig = new IngestArguments(spec = "id:Double,time:Date,*geom:Geometry", idFields = None,
    dtField = Option("time"), dtFormat = "yyyy-MM-dd", skipHeader = false, featureName = Option("test_type"),
    method = "local", file = "none", format = Some("CSV"))

  def csvNormParams: Map[String, List[String]] = {
    id = id + 1
    Map("idFields" -> List(null),
      "path" -> List(null),
      "sftspec" -> List("id:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326"),
      "dtField" -> List("time"),
      "dtFormat" -> List("yyyy-MM-dd"),
      "lonAttribute" -> List("lon"),
      "latAttribute" -> List("lat"),
      "skipHeader" -> List("false"),
      "doHash" -> List("true"),
      "format" -> List("CSV"),
      "featureName" -> List("test_type"),
      "catalog" -> List(currentCatalog),
      "instanceId" -> List("mycloud"),
      "zookeepers" -> List("zoo1:2181,zoo2:2181,zoo3:2181"),
      "user" -> List("myuser"),
      "password" -> List("mypassword"),
      "useMock" -> List("true"))
  }

  def csvWktParams: Map[String, List[String]] = {
    id = id + 1
    Map("--idFields" -> List(null),
      "--path" -> List(null),
      "--sftspec" -> List("id:Double,time:Date,lon:Double,lat:Double,*geom:Geometry"),
      "--dtField" -> List("time"),
      "--dtFormat" -> List("yyyy-MM-dd"),
      "--skipHeader" -> List("false"),
      "--doHash" -> List("true"),
      "--format" -> List("CSV"),
      "--featureName" -> List("test_type"),
      "--catalog" -> List(currentCatalog),
      "--instanceID" -> List("mycloud"),
      "--zookeepers" -> List("zoo1:2181,zoo2:2181,zoo3:2181"),
      "--user" -> List("myuser"),
      "--password" -> List("mypassword"),
      "--useMock" -> List("true"))
  }

  def currentCatalog = f"SVIngestTestTableUnique$id%d"

  "SVIngest" should {

//    "properly create an AvroSimpleFeature from a comma-delimited string" in {
//      val ingest = new SVIngest(new Args(csvNormParams))
//      val testString = "1325409954,2013-07-17Z,-90.368732,35.3155"
//      val f = ingest.lineToFeature(testString)
//
//      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
//      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
//      f.get.getAttribute(2) must beAnInstanceOf[java.lang.Double]
//      f.get.getAttribute(3) must beAnInstanceOf[java.lang.Double]
//      f.get.getAttribute(4) must beAnInstanceOf[Geometry]
//    }
//
//    "properly create an AvroSimpleFeature from a tab-delimited string" in {
//      val ingest = new SVIngest(new Args(csvNormParams.updated("format", List("TSV"))))
//      val testString = "1325409954\t2013-07-17Z\t-90.368732\t35.3155"
//      val f = ingest.lineToFeature(testString)
//
//      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
//      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
//      f.get.getAttribute(2) must beAnInstanceOf[java.lang.Double]
//      f.get.getAttribute(3) must beAnInstanceOf[java.lang.Double]
//      f.get.getAttribute(4) must beAnInstanceOf[Geometry]
//    }

//    "properly create an AvroSimpleFeature from a comma-delimited string with a Point WKT geometry" in {
//      val ingest = new SVIngest(Config(csvWktParams).getArgs)
//      val testString = "294908082,2013-07-17Z,POINT(-90.161852 32.39271)"
//      val f = ingest.lineToFeature(testString)
//
//      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
//      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
//      f.get.getAttribute(2) must beAnInstanceOf[Geometry]
//    }
//
//    "properly create an AvroSimpleFeature from a tab-delimited string with a Point WKT geometry" in {
//      val ingest = new SVIngest(Config(csvWktParams.updated("--format", "TSV")).getArgs)
//      val testString = "294908082\t2013-07-17Z\tPOINT(-90.161852 32.39271)"
//      val f = ingest.lineToFeature(testString)
//
//      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
//      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
//      f.get.getAttribute(2) must beAnInstanceOf[Geometry]
//    }

//    "properly create an AvroSimpleFeature from a tab-delimited string with" +
//      " a Point WKT geometry and non-standard dtformat" in {
//      val ingest = new SVIngest(csvWktConfig.copy(spec = "id:String:index=False,username:String:index=false," +
//        "userid:String:index=false,text:String:index=false,dtg:Date:index=false,*geom:Point:srid=4326:index=true",
//        format = Option("TSV"), dtFormat = "yyyy/MM/dd :HH:mm:ss:", dtField = Option("dtg")),
//        createConnectionMap)
//      val testString = "0000\tgeomesa user\t823543\tGeoMesa rules!\t2014/08/13 :06:06:06:\tPoint(-78.4 38.0)"
//      val f = ingest.lineToFeature(testString)
//
//      f.get.getAttribute(0) must beAnInstanceOf[java.lang.String]
//      f.get.getAttribute(1) must beAnInstanceOf[java.lang.String]
//      f.get.getAttribute(2) must beAnInstanceOf[java.lang.String]
//      f.get.getAttribute(3) must beAnInstanceOf[java.lang.String]
//      f.get.getAttribute(4) must beAnInstanceOf[java.util.Date]
//      f.get.getAttribute(5) must beAnInstanceOf[Geometry]
//
//    }
//
//
//    "properly create an Interator[Try[AvroSimpleFeature]] from a valid CSV with WKT geometries" in {
//      val path = Tools.getClass.getResource("/test_valid_wkt.csv")
//      val ingest = new SVIngest(csvWktConfig, createConnectionMap)
//      val lines = Source.fromFile(path.getFile).getLines
//      val featureIterator = ingest.linesToFeatures(lines)
//
//      featureIterator.foreach {
//        case Success(ft) =>
//          ft.getAttribute(0) must beAnInstanceOf[java.lang.Double]
//          ft.getAttribute(1) must beAnInstanceOf[java.util.Date]
//          ft.getAttribute(2) must beAnInstanceOf[Geometry]
//        case Failure(ex) => failure
//      }
//      featureIterator must beAnInstanceOf[Iterator[Try[AvroSimpleFeature]]]
//
//    }
//
//    "properly create an Interator[Try[AvroSimpleFeature]] from a valid TSV with WKT geometries" in {
//      val path = Tools.getClass.getResource("/test_valid_wkt.tsv")
//      val ingest = new SVIngest(csvWktConfig.copy(format = Option("TSV")), createConnectionMap)
//      val lines = Source.fromFile(path.getFile).getLines
//      val featureIterator = ingest.linesToFeatures(lines)
//
//      featureIterator.foreach {
//        case Success(ft) =>
//          ft.getAttribute(0) must beAnInstanceOf[java.lang.Double]
//          ft.getAttribute(1) must beAnInstanceOf[java.util.Date]
//          ft.getAttribute(2) must beAnInstanceOf[Geometry]
//        case Failure(ex) => failure
//      }
//      featureIterator must beAnInstanceOf[Iterator[Try[AvroSimpleFeature]]]
//
//    }

  }

}
