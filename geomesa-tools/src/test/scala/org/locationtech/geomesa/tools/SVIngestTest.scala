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

import com.twitter.scalding.Args
import com.vividsolutions.jts.geom.Geometry
import org.junit.runner.RunWith
import org.locationtech.geomesa.tools.Utils.IngestParams
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
    Map(
      IngestParams.ID_FIELDS -> List(null),
      IngestParams.FILE_PATH -> List(null),
      IngestParams.SFT_SPEC -> List("id:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326"),
      IngestParams.DT_FIELD -> List("time"),
      IngestParams.DT_FORMAT -> List("yyyy-MM-dd"),
      IngestParams.LON_ATTRIBUTE -> List("lon"),
      IngestParams.LAT_ATTRIBUTE -> List("lat"),
      IngestParams.SKIP_HEADER -> List("false"),
      IngestParams.DO_HASH -> List("true"),
      IngestParams.FORMAT -> List("CSV"),
      IngestParams.FEATURE_NAME -> List("test_type"),
      IngestParams.CATALOG_TABLE -> List(currentCatalog),
      IngestParams.ACCUMULO_INSTANCE -> List("mycloud"),
      IngestParams.ZOOKEEPERS -> List("zoo1:2181,zoo2:2181,zoo3:2181"),
      IngestParams.ACCUMULO_USER -> List("myuser"),
      IngestParams.ACCUMULO_PASSWORD -> List("mypassword"),
      IngestParams.ACCUMULO_MOCK -> List("true"))
  }

  def csvWktParams: Map[String, List[String]] = {
    id = id + 1
    Map(
      IngestParams.ID_FIELDS -> List(null),
      IngestParams.FILE_PATH -> List(null),
      IngestParams.SFT_SPEC -> List("id:Double,time:Date,*geom:Geometry"),
      IngestParams.DT_FIELD -> List("time"),
      IngestParams.DT_FORMAT -> List("yyyy-MM-dd"),
      IngestParams.SKIP_HEADER -> List("false"),
      IngestParams.DO_HASH -> List("true"),
      IngestParams.FORMAT -> List("CSV"),
      IngestParams.FEATURE_NAME -> List("test_type"),
      IngestParams.CATALOG_TABLE -> List(currentCatalog),
      IngestParams.ACCUMULO_INSTANCE -> List("mycloud"),
      IngestParams.ZOOKEEPERS -> List("zoo1:2181,zoo2:2181,zoo3:2181"),
      IngestParams.ACCUMULO_USER -> List("myuser"),
      IngestParams.ACCUMULO_PASSWORD -> List("mypassword"),
      IngestParams.ACCUMULO_MOCK -> List("true"))
  }

  def currentCatalog = f"SVIngestTestTableUnique$id%d"

  "SVIngest" should {

    "properly create an AvroSimpleFeature from a comma-delimited string" in {
      val ingest = new SVIngest(new Args(csvNormParams))
      val testString = "1325409954,2013-07-17Z,-90.368732,35.3155"
      val f = ingest.lineToFeature(testString)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(4) must beAnInstanceOf[Geometry]
    }

    "properly create an AvroSimpleFeature from a tab-delimited string" in {
      val ingest = new SVIngest(new Args(csvNormParams.updated(IngestParams.FORMAT, List("TSV"))))
      val testString = "1325409954\t 2013-07-17 \t-90.368732\t35.3155"
      val f = ingest.lineToFeature(testString)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(4) must beAnInstanceOf[Geometry]
    }

    "properly create an AvroSimpleFeature from a comma-delimited string with a Point WKT geometry" in {
      val ingest = new SVIngest(new Args(csvWktParams))
      val testString = "294908082,2013-07-17Z,POINT(-90.161852 32.39271)"
      val f = ingest.lineToFeature(testString)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(2) must beAnInstanceOf[Geometry]
    }

    "properly create an AvroSimpleFeature from a tab-delimited string with a Point WKT geometry" in {
      val ingest = new SVIngest(new Args(csvWktParams.updated(IngestParams.FORMAT, List("TSV"))))
      val testString = "294908082\t2013-07-17Z\tPOINT(-90.161852 32.39271)"
      val f = ingest.lineToFeature(testString)

      f.get.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.get.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.get.getAttribute(2) must beAnInstanceOf[Geometry]
    }

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
