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

import java.io.File
import com.vividsolutions.jts.geom.Geometry
import org.apache.commons.csv.{CSVFormat, CSVParser}
import org.junit.runner.RunWith
import org.locationtech.geomesa.feature.AvroSimpleFeature
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import scala.util.{Failure, Success, Try}

@RunWith(classOf[JUnitRunner])
class SVIngestTest extends Specification{

  sequential
  var id = 0
  var csvConfig = new IngestArguments(spec = "id:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326",
    idFields = None, dtField = Option("time"), lonAttribute = Option("lon"), latAttribute = Option("lat"),
    dtFormat = Option("yyyy-MM-dd"), skipHeader = false, featureName = Option("test_type"),
    method = "local", file = "none", format = Some("CSV"))

  var csvWktConfig = new IngestArguments(spec = "id:Double,time:Date,*geom:Geometry", idFields = None,
    dtField = Option("time"), dtFormat = Option("yyyy-MM-dd"), skipHeader = false, featureName = Option("test_type"),
    method = "local", file = "none", format = Some("CSV"))

  def currentCatalog = f"SVIngestTestTableUnique$id%d"

  def createConnectionMap: Map[String, String] ={
    id = id + 1
    Map(
      "instanceId"      -> "mycloud",
      "zookeepers"      -> "zoo1:2181,zoo2:2181,zoo3:2181",
      "user"            -> "myuser",
      "password"        -> "mypassword",
      "tableName"       -> currentCatalog,
      "useMock"         -> "true")
  }

  "SVIngest" should {

    "properly create an AvroSimpleFeature from a comma-delimited string" in {
      val ingest = new SVIngest(csvConfig, createConnectionMap)
      val testString = "1325409954,2013-07-17,-90.368732,35.3155"
      val csvRecord = CSVParser.parse(testString, CSVFormat.DEFAULT).iterator().next()
      val f = ingest.lineToFeature(csvRecord)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(4) must beAnInstanceOf[Geometry]
    }

    "properly create an AvroSimpleFeature from a tab-delimited string" in {
      val ingest = new SVIngest(csvConfig.copy(format = Option("TSV")), createConnectionMap)
      val testString = "1325409954\t2013-07-17\t-90.368732\t35.3155"
      val tsvRecord = CSVParser.parse(testString, CSVFormat.TDF).iterator().next()
      val f = ingest.lineToFeature(tsvRecord)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(4) must beAnInstanceOf[Geometry]
    }

    "properly create an AvroSimpleFeature from a comma-delimited string with no date time field or format" in {
      val ingest = new SVIngest(csvConfig.copy(dtFormat = None, dtField = None,
        spec = "id:Double,lon:Double,lat:Double,*geom:Point:srid=4326"), createConnectionMap)
      val testString = "1325409954,-90.368732,35.3155"
      val csvRecord = CSVParser.parse(testString, CSVFormat.DEFAULT).iterator().next()
      val f = ingest.lineToFeature(csvRecord)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(3) must beAnInstanceOf[Geometry]
    }

    "properly create an AvroSimpleFeature from a tab-delimited string with no date time field or format" in {
      val ingest = new SVIngest(csvConfig.copy(format = Option("TSV"), dtFormat = None, dtField = None,
        spec = "id:Double,lon:Double,lat:Double,*geom:Point:srid=4326"), createConnectionMap)
      val testString = "1325409954\t-90.368732\t35.3155"
      val tsvRecord = CSVParser.parse(testString, CSVFormat.TDF).iterator().next()
      val f = ingest.lineToFeature(tsvRecord)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(3) must beAnInstanceOf[Geometry]
    }

    "properly create an AvroSimpleFeature from a comma-delimited string with a Point WKT geometry" in {
      val ingest = new SVIngest(csvWktConfig, createConnectionMap)
      val testString = "294908082,2013-07-17,POINT(-90.161852 32.39271)"
      val csvRecord = CSVParser.parse(testString, CSVFormat.DEFAULT).iterator().next()
      val f = ingest.lineToFeature(csvRecord)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(2) must beAnInstanceOf[Geometry]
    }

    "properly create an AvroSimpleFeature from a tab-delimited string with a Point WKT geometry" in {
      val ingest = new SVIngest(csvWktConfig.copy(format = Option("TSV")), createConnectionMap)
      val testString = "294908082\t2013-07-17\tPOINT(-90.161852 32.39271)"
      val tsvRecord = CSVParser.parse(testString, CSVFormat.TDF).iterator().next()
      val f = ingest.lineToFeature(tsvRecord)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(2) must beAnInstanceOf[Geometry]
    }

    "properly create an AvroSimpleFeature from a comma-delimited string with a Polygon WKT geometry" in {
      val ingest = new SVIngest(csvWktConfig, createConnectionMap)
      val testString = "294908082,2013-07-17,\"POLYGON((0 0, 0 10, 10 10, 0 0))\""
      val csvRecord = CSVParser.parse(testString, CSVFormat.DEFAULT).iterator().next()
      val f = ingest.lineToFeature(csvRecord)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(2) must beAnInstanceOf[Geometry]
    }

    "properly create an AvroSimpleFeature from a tab-delimited string with a Polygon WKT geometry" in {
      val ingest = new SVIngest(csvWktConfig.copy(format = Option("TSV")), createConnectionMap)
      val testString = "294908082\t2013-07-17\tPOLYGON((0 0, 0 10, 10 10, 0 0))"
      val tsvRecord = CSVParser.parse(testString, CSVFormat.TDF).iterator().next()
      val f = ingest.lineToFeature(tsvRecord)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(2) must beAnInstanceOf[Geometry]
    }

    "properly create an AvroSimpleFeature from a tab-delimited string with" +
      " a Point WKT geometry and non-standard dtformat" in {
      val ingest = new SVIngest(csvWktConfig.copy(spec = "id:String:index=False,username:String:index=false," +
        "userid:String:index=false,text:String:index=false,dtg:Date:index=false,*geom:Point:srid=4326:index=true",
        format = Option("TSV"), dtFormat = Option("yyyy/MM/dd :HH:mm:ss:"), dtField = Option("dtg")),
        createConnectionMap)
      val testString = "0000\tgeomesa user\t823543\tGeoMesa rules!\t2014/08/13 :06:06:06:\tPoint(-78.4 38.0)"
      val tsvRecord = CSVParser.parse(testString, CSVFormat.TDF).iterator().next()
      val f = ingest.lineToFeature(tsvRecord)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.String]
      f.get.getAttribute(1) must beAnInstanceOf[java.lang.String]
      f.get.getAttribute(2) must beAnInstanceOf[java.lang.String]
      f.get.getAttribute(3) must beAnInstanceOf[java.lang.String]
      f.get.getAttribute(4) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(5) must beAnInstanceOf[Geometry]
    }

    "properly create an Seq[Try[AvroSimpleFeature]] from a valid CSV" in {
      val path = Tools.getClass.getResource("/test_valid.csv")
      val ingest = new SVIngest(csvConfig, createConnectionMap)
      val featureIterator = ingest.linesToFeatures(new File(path.toURI))

      featureIterator.foreach {
        case Success(ft) =>
          ft.getAttribute(0) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(1) must beAnInstanceOf[java.util.Date]
          ft.getAttribute(2) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(3) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(4) must beAnInstanceOf[Geometry]
        case Failure(ex) => failure
      }
      featureIterator must beAnInstanceOf[Seq[Try[AvroSimpleFeature]]]
    }

    "properly create an Seq[Try[AvroSimpleFeature]] from a valid TSV" in {
      val path = Tools.getClass.getResource("/test_valid.tsv")
      val ingest = new SVIngest(csvConfig.copy(format = Option("TSV")), createConnectionMap)
      val featureIterator = ingest.linesToFeatures(new File(path.toURI))

      featureIterator.foreach {
        case Success(ft) =>
          ft.getAttribute(0) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(1) must beAnInstanceOf[java.util.Date]
          ft.getAttribute(2) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(3) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(4) must beAnInstanceOf[Geometry]
        case Failure(ex) => failure
      }
      featureIterator must beAnInstanceOf[Seq[Try[AvroSimpleFeature]]]
    }

    "properly create an Seq[Try[AvroSimpleFeature]] from a valid CSV with WKT geometries" in {
      val path = Tools.getClass.getResource("/test_valid_wkt.csv")
      val ingest = new SVIngest(csvWktConfig, createConnectionMap)
      val featureIterator = ingest.linesToFeatures(new File(path.toURI))

      featureIterator.foreach {
        case Success(ft) =>
          ft.getAttribute(0) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(1) must beAnInstanceOf[java.util.Date]
          ft.getAttribute(2) must beAnInstanceOf[Geometry]
        case Failure(ex) => failure
      }
      featureIterator must beAnInstanceOf[Seq[Try[AvroSimpleFeature]]]
    }

    "properly create an Seq[Try[AvroSimpleFeature]]from a valid TSV with WKT geometries" in {
      val path = Tools.getClass.getResource("/test_valid_wkt.tsv")
      val ingest = new SVIngest(csvWktConfig.copy(format = Option("TSV")), createConnectionMap)
      val featureIterator = ingest.linesToFeatures(new File(path.toURI))

      featureIterator.foreach {
        case Success(ft) =>
          ft.getAttribute(0) must beAnInstanceOf[java.lang.Double]
          ft.getAttribute(1) must beAnInstanceOf[java.util.Date]
          ft.getAttribute(2) must beAnInstanceOf[Geometry]
        case Failure(ex) => failure
      }
      featureIterator must beAnInstanceOf[Seq[Try[AvroSimpleFeature]]]
    }

    "properly write the features from a valid CSV" in {
      val path = Tools.getClass.getResource("/test_valid.csv")
      val ingest = new SVIngest(csvConfig, createConnectionMap)

      ingest.runTestIngest(new File(path.toURI)) must beASuccessfulTry
    }

    "properly write the features from a valid TSV" in {
      val path = Tools.getClass.getResource("/test_valid.tsv")
      val ingest = new SVIngest(csvConfig.copy(format = Option("TSV")), createConnectionMap)

      ingest.runTestIngest(new File(path.toURI)) must beASuccessfulTry
    }

    "properly write the features from a valid CSV with WKT geometries" in {
      val path = Tools.getClass.getResource("/test_valid_wkt.csv")
      val ingest = new SVIngest(csvWktConfig, createConnectionMap)

      ingest.runTestIngest(new File(path.toURI)) must beASuccessfulTry
    }

    "properly write the features from a valid TSV with WKT geometries" in {
      val path = Tools.getClass.getResource("/test_valid_wkt.tsv")
      val ingest = new SVIngest(csvWktConfig.copy(format = Option("TSV")), createConnectionMap)

      ingest.runTestIngest(new File(path.toURI)) must beASuccessfulTry
    }

    "properly write the features from a valid CSV with no date/time" in {
      val path = Tools.getClass.getResource("/test_valid_nd.csv")
      val ingest = new SVIngest(csvConfig.copy(dtFormat = None, dtField = None,
        spec = "id:Double,lon:Double,lat:Double,*geom:Point:srid=4326"), createConnectionMap)

      ingest.runTestIngest(new File(path.toURI)) must beASuccessfulTry
    }

    "properly write the features from a valid TSV with no date/time" in {
      val path = Tools.getClass.getResource("/test_valid_nd.tsv")
      val ingest = new SVIngest(csvConfig.copy(format = Option("TSV"), dtFormat = None, dtField = None,
        spec = "id:Double,lon:Double,lat:Double,*geom:Point:srid=4326"), createConnectionMap)

      ingest.runTestIngest(new File(path.toURI)) must beASuccessfulTry
    }
  }

}
