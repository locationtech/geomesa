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

import com.vividsolutions.jts.geom.Geometry
import org.junit.runner.RunWith
import org.locationtech.geomesa.feature.AvroSimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import scala.io.Source
import scala.util.{Failure, Success, Try}

@RunWith(classOf[JUnitRunner])
class SVIngestTest extends Specification{

  sequential
  var id = 0
  var csvConfig = new ScoptArguments(spec = "id:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326",
    idFields = None, dtField = "time", lonAttribute = Option("lon"), latAttribute = Option("lat"),
    dtFormat = "yyyy-MM-dd", skipHeader = false, featureName = "test_type",
    method = "local", file = "none", format = "CSV")

  var csvWktConfig = new ScoptArguments(spec = "id:Double,time:Date,*geom:Geometry", idFields = None,
    dtField = "time", dtFormat = "yyyy-MM-dd", skipHeader = false, featureName = "test_type",
    method = "local", file = "none", format = "CSV")

  def currentCatalog = f"SVIngestTestTableUnique$id%d"

  def createConnectionMap: Map[String, String] ={
    id = id + 1
    Map(
      "instanceId"      -> "mycloud",
      "zookeepers"      -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"            -> "myuser",
      "password"        -> "mypassword",
      "tableName"       -> currentCatalog,
      "featureEncoding" -> "avro",
      "useMock"         -> "true")
  }

  "SVIngest" should {

    "properly create an AvroSimpleFeature from a comma-delimited string" in {
      val ingest = new SVIngest(csvConfig, createConnectionMap)
      val testString = "1325409954,2013-07-17Z,-90.368732,35.3155"
      val f = ingest.lineToFeature(testString)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(4) must beAnInstanceOf[Geometry]

    }

    "properly create an AvroSimpleFeature from a tab-delimited string" in {
      val ingest = new SVIngest(csvConfig.copy(format = "TSV"), createConnectionMap)
      val testString = "1325409954\t2013-07-17Z\t-90.368732\t35.3155"
      val f = ingest.lineToFeature(testString)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(4) must beAnInstanceOf[Geometry]

    }

    "properly create an AvroSimpleFeature from a comma-delimited string with a Point WKT geometry" in {
      val ingest = new SVIngest(csvWktConfig, createConnectionMap)
      val testString = "294908082,2013-07-17Z,POINT(-90.161852 32.39271)"
      val f = ingest.lineToFeature(testString)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(2) must beAnInstanceOf[Geometry]

    }

    "properly create an AvroSimpleFeature from a tab-delimited string with a Point WKT geometry" in {
      val ingest = new SVIngest(csvWktConfig.copy(format = "TSV"), createConnectionMap)
      val testString = "294908082\t2013-07-17Z\tPOINT(-90.161852 32.39271)"
      val f = ingest.lineToFeature(testString)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(2) must beAnInstanceOf[Geometry]

    }

    "properly create an AvroSimpleFeature from a tab-delimited string with" +
      " a Point WKT geometry and non-standard dtformat" in {
      val ingest = new SVIngest(csvWktConfig.copy(spec = "id:String:index=False,username:String:index=false," +
        "userid:String:index=false,text:String:index=false,dtg:Date:index=false,*geom:Point:srid=4326:index=true",
        format = "TSV", dtFormat = "yyyy/MM/dd :HH:mm:ss:", dtField = "dtg"),
        createConnectionMap)
      val testString = "0000\tgeomesa user\t823543\tGeoMesa rules!\t2014/08/13 :06:06:06:\tPoint(-78.4 38.0)"
      val f = ingest.lineToFeature(testString)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.String]
      f.get.getAttribute(1) must beAnInstanceOf[java.lang.String]
      f.get.getAttribute(2) must beAnInstanceOf[java.lang.String]
      f.get.getAttribute(3) must beAnInstanceOf[java.lang.String]
      f.get.getAttribute(4) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(5) must beAnInstanceOf[Geometry]

    }

    "properly create an Interator[Try[AvroSimpleFeature]] from a valid CSV" in {
      val path = Tools.getClass.getResource("/test_valid.csv")
      val ingest = new SVIngest(csvConfig, createConnectionMap)
      val lines = Source.fromFile(path.getFile).getLines
      val featureIterator = ingest.linesToFeatures(lines)

      featureIterator.foreach {
        case Success(ft) =>
          ft.getAttribute(0) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(1) must beAnInstanceOf[java.util.Date]
          ft.getAttribute(2) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(3) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(4) must beAnInstanceOf[Geometry]
        case Failure(ex) => failure
      }
      featureIterator must beAnInstanceOf[Iterator[Try[AvroSimpleFeature]]]

    }

    "properly create an Interator[Try[AvroSimpleFeature]] from a valid TSV" in {
      val path = Tools.getClass.getResource("/test_valid.tsv")
      val ingest = new SVIngest(csvConfig.copy(format = "TSV"), createConnectionMap)
      val lines = Source.fromFile(path.getFile).getLines
      val featureIterator = ingest.linesToFeatures(lines)

      featureIterator.foreach {
        case Success(ft) =>
          ft.getAttribute(0) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(1) must beAnInstanceOf[java.util.Date]
          ft.getAttribute(2) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(3) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(4) must beAnInstanceOf[Geometry]
        case Failure(ex) => failure
      }
      featureIterator must beAnInstanceOf[Iterator[Try[AvroSimpleFeature]]]

    }

    "properly create an Interator[Try[AvroSimpleFeature]] from a valid CSV with WKT geometries" in {
      val path = Tools.getClass.getResource("/test_valid_wkt.csv")
      val ingest = new SVIngest(csvWktConfig, createConnectionMap)
      val lines = Source.fromFile(path.getFile).getLines
      val featureIterator = ingest.linesToFeatures(lines)

      featureIterator.foreach {
        case Success(ft) =>
          ft.getAttribute(0) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(1) must beAnInstanceOf[java.util.Date]
          ft.getAttribute(2) must beAnInstanceOf[Geometry]
        case Failure(ex) => failure
      }
      featureIterator must beAnInstanceOf[Iterator[Try[AvroSimpleFeature]]]

    }

    "properly create an Interator[Try[AvroSimpleFeature]] from a valid TSV with WKT geometries" in {
      val path = Tools.getClass.getResource("/test_valid_wkt.tsv")
      val ingest = new SVIngest(csvWktConfig.copy(format = "TSV"), createConnectionMap)
      val lines = Source.fromFile(path.getFile).getLines
      val featureIterator = ingest.linesToFeatures(lines)

      featureIterator.foreach {
        case Success(ft) =>
          ft.getAttribute(0) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(1) must beAnInstanceOf[java.util.Date]
          ft.getAttribute(2) must beAnInstanceOf[Geometry]
        case Failure(ex) => failure
      }
      featureIterator must beAnInstanceOf[Iterator[Try[AvroSimpleFeature]]]

    }

  }

}
