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
import org.geotools.filter.identity.FeatureIdImpl
import org.junit.runner.RunWith
import org.locationtech.geomesa.feature.AvroSimpleFeature
import org.locationtech.geomesa.tools.Utils.IngestParams
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.io.Source
import scala.util.Try

@RunWith(classOf[JUnitRunner])
class SVIngestTest extends Specification{
  sequential
  var id = 0

  def csvNormParams: Map[String, List[String]] = {
    id = id + 1
    Map(
      IngestParams.ID_FIELDS -> List(null),
      IngestParams.FILE_PATH -> List(null),
      IngestParams.SFT_SPEC -> List("fid:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326"),
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
      IngestParams.ACCUMULO_MOCK -> List("true"),
      IngestParams.IS_TEST_INGEST -> List("true"))
  }

  def csvWktParams: Map[String, List[String]] = {
    id = id + 1
    Map(
      IngestParams.ID_FIELDS -> List(null),
      IngestParams.FILE_PATH -> List(null),
      IngestParams.SFT_SPEC -> List("fid:Double,time:Date,*geom:Geometry"),
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
      IngestParams.ACCUMULO_MOCK -> List("true"),
      IngestParams.IS_TEST_INGEST -> List("true"))
  }

  def currentCatalog = f"SVIngestTestTableUnique$id%d"

  "SVIngest" should {
    "properly add attributes to an AvroSimpleFeature from a comma-delimited string" in {
      val ingest = new SVIngest(new Args(csvNormParams))
      val testString = "1325409954,2013-07-17,-90.368732,35.3155"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      val ingestTry = ingest.ingestDataToFeature(testString, f)

      ingestTry must beASuccessfulTry

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(4) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a comma-delimited string with quote wrapped fields" in {
      val ingest = new SVIngest(new Args(csvNormParams))
      val testString = "\"1325409954\",\"2013-07-17\",\"-90.368732\",\"35.3155\""
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      val ingestTry = ingest.ingestDataToFeature(testString, f)

      ingestTry must beASuccessfulTry

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(4) must beAnInstanceOf[Geometry]
    }

    "properly fail to add attributes to an AvroSimpleFeature from a comma-delimited string with mangled quote wrapped fields" in {
      val ingest = new SVIngest(new Args(csvNormParams))
      val testString = "\"1325409954\",2013-07-17\",\"-90.368732\",\"35.3155"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      val ingestTry = ingest.ingestDataToFeature(testString, f)

      ingestTry must beAFailedTry
    }

    "properly add attributes to an AvroSimpleFeature from a tab-delimited string" in {
      val ingest = new SVIngest(new Args(csvNormParams.updated(IngestParams.FORMAT, List("TSV"))))
      val testString = "1325409954\t2013-07-17\t-90.368732\t35.3155"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      val ingestTry = ingest.ingestDataToFeature(testString, f)

      ingestTry must beASuccessfulTry

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(4) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a tab-delimited string with quote wrapped fields" in {
      val ingest = new SVIngest(new Args(csvNormParams.updated(IngestParams.FORMAT, List("TSV"))))
      val testString = "\"1325409954\"\t\"2013-07-17\"\t\"-90.368732\"\t\"35.3155\""
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      val ingestTry = ingest.ingestDataToFeature(testString, f)

      ingestTry must beASuccessfulTry

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(4) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a comma-delimited string with a unix (milliseconds) datetime field" in {
      val ingest = new SVIngest(new Args(csvNormParams.updated(IngestParams.DT_FIELD, List("time"))
        .updated(IngestParams.DT_FORMAT, List.empty)))
      val testString = "1325409954,1410199543000,-90.368732,35.3155"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      val ingestTry = ingest.ingestDataToFeature(testString, f)

      ingestTry must beASuccessfulTry

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(4) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a comma-delimited string with no date time field or format" in {
      val ingest = new SVIngest(new Args(csvNormParams.updated(IngestParams.DT_FORMAT, List.empty)
        .updated(IngestParams.DT_FIELD, List.empty)))
      val testString = "1325409954,-90.368732,35.3155"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      val ingestTry = ingest.ingestDataToFeature(testString, f)
      ingestTry must beASuccessfulTry

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(3) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a tab-delimited string with no date time field or format" in {
      val ingest = new SVIngest(new Args(csvNormParams.updated(IngestParams.DT_FORMAT, List.empty)
        .updated(IngestParams.DT_FIELD, List.empty)
        .updated(IngestParams.FORMAT, List("TSV"))))
      val testString = "1325409954\t-90.368732\t35.3155"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      val ingestTry = ingest.ingestDataToFeature(testString, f)

      ingestTry must beASuccessfulTry

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(3) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a comma-delimited string with a Point WKT geometry" in {
      val ingest = new SVIngest(new Args(csvWktParams))
      val testString = "294908082,2013-07-17,POINT(-90.161852 32.39271)"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,*geom:Geometry")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      val ingestTry = ingest.ingestDataToFeature(testString, f)

      ingestTry must beASuccessfulTry

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a tab-delimited string with a Point WKT geometry" in {
      val ingest = new SVIngest(new Args(csvWktParams.updated(IngestParams.FORMAT, List("TSV"))))
      val testString = "294908082\t2013-07-17\tPOINT(-90.161852 32.39271)"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,*geom:Geometry")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      val ingestTry = ingest.ingestDataToFeature(testString, f)

      ingestTry must beASuccessfulTry

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a comma-delimited string with a Polygon WKT geometry" in {
      val ingest = new SVIngest(new Args(csvWktParams))
      val testString = "294908082,2013-07-17,\"POLYGON((0 0, 0 10, 10 10, 0 0))\""
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,*geom:Geometry")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      val ingestTry = ingest.ingestDataToFeature(testString, f)

      ingestTry must beASuccessfulTry

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a tab-delimited string with a Polygon WKT geometry" in {
      val ingest = new SVIngest(new Args(csvWktParams.updated(IngestParams.FORMAT, List("TSV"))))
      val testString = "294908082\t2013-07-17\tPOLYGON((0 0, 0 10, 10 10, 0 0))"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,*geom:Geometry")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      val ingestTry = ingest.ingestDataToFeature(testString, f)

      ingestTry must beASuccessfulTry

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a tab-delimited string with" +
      " a Point WKT geometry and non-standard dtformat" in {
      val ingest = new SVIngest(new Args(
        csvNormParams.updated(IngestParams.DT_FORMAT, List("yyyy/MM/dd :HH:mm:ss:")).updated(IngestParams.FORMAT, List("TSV"))
        .updated(IngestParams.SFT_SPEC, List("fid:String,username:String,userid:String,text:String,time:Date,*geom:Point:srid=4326"))))
      val testString = "0000\tgeomesa user\t823543\tGeoMesa rules!\t2014/08/13 :06:06:06:\tPoint(-78.4 38.0)"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:String,username:String,userid:String,text:String,time:Date,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      val ingestTry = ingest.ingestDataToFeature(testString, f)

      ingestTry must beASuccessfulTry

      f.getAttribute(0) must beAnInstanceOf[java.lang.String]
      f.getAttribute(1) must beAnInstanceOf[java.lang.String]
      f.getAttribute(2) must beAnInstanceOf[java.lang.String]
      f.getAttribute(3) must beAnInstanceOf[java.lang.String]
      f.getAttribute(4) must beAnInstanceOf[java.util.Date]
      f.getAttribute(5) must beAnInstanceOf[Geometry]
    }

    "properly write the features from a valid CSV" in {
      val path = Tools.getClass.getResource("/test_valid.csv")
      val ingest = new SVIngest(new Args(csvNormParams))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines) must beASuccessfulTry
    }

    "properly write the features from a valid TSV" in {
      val path = Tools.getClass.getResource("/test_valid.tsv")
      val ingest = new SVIngest(new Args(csvNormParams.updated(IngestParams.FORMAT, List("TSV"))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines) must beASuccessfulTry
    }

    "properly write the features from a valid CSV containing WKT geometries" in {
      val path = Tools.getClass.getResource("/test_valid_wkt.csv")
      val ingest = new SVIngest(new Args(csvWktParams))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines) must beASuccessfulTry
    }

    "properly write the features from a valid TSV containing WKT geometries" in {
      val path = Tools.getClass.getResource("/test_valid_wkt.tsv")
      val ingest = new SVIngest(new Args(csvWktParams.updated(IngestParams.FORMAT, List("TSV"))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines) must beASuccessfulTry
    }

    "properly write the features from a valid CSV with no date/time" in {
      val path = Tools.getClass.getResource("/test_valid_nd.csv")
      val ingest = new SVIngest(new Args(csvNormParams.updated(IngestParams.DT_FORMAT, List.empty)
        .updated(IngestParams.DT_FIELD, List.empty)
        .updated(IngestParams.SFT_SPEC, List("fid:Double,lon:Double,lat:Double,*geom:Point:srid=4326"))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines) must beASuccessfulTry
    }

    "properly write the features from a valid TSV with no date/time" in {
      val path = Tools.getClass.getResource("/test_valid_nd.tsv")
      val ingest = new SVIngest(new Args(csvNormParams.updated(IngestParams.DT_FORMAT, List.empty)
        .updated(IngestParams.DT_FIELD, List.empty).updated(IngestParams.FORMAT, List("TSV"))
        .updated(IngestParams.SFT_SPEC, List("fid:Double,lon:Double,lat:Double,*geom:Point:srid=4326"))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines) must beASuccessfulTry
    }

    "properly create subset of column list" in {
      val ingest = new SVIngest(new Args(csvNormParams))
      val inputColsList1 = "0,1,3,7-9,12,13"
      val inputColsList2 = "0,3,1,3,1,7-9,12,13"
      val checkList = List(0,1,3,7,8,9,12,13)

      val resultList1 = ingest.ColsParser.build(inputColsList1)
      resultList1 mustEqual checkList

      val resultList2 = ingest.ColsParser.build(inputColsList2)
      resultList2 mustEqual checkList
    }

    "properly fail in creating subset of column list" in {
      val ingest = new SVIngest(new Args(csvNormParams))
      Try(ingest.ColsParser.build("1,3,-7,12,13")) must beFailedTry
      Try(ingest.ColsParser.build("1,3,9-7,12,13")) must beFailedTry
      Try(ingest.ColsParser.build("1,3,-7-9,12,13")) must beFailedTry
    }

    "properly write a subset of features from a valid CSV" in {
      val path = Tools.getClass.getResource("/test_valid.csv")
      val ingest = new SVIngest(new Args(csvNormParams.updated(IngestParams.SFT_SPEC,
        List("time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")) ++ Map(IngestParams.COLS -> List("1-3"))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines) must beASuccessfulTry
    }

    "properly write a subset of features from a valid TSV" in {
      val path = Tools.getClass.getResource("/test_valid.tsv")
      val ingest = new SVIngest(new Args(csvNormParams.updated(IngestParams.SFT_SPEC,
        List("time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")).updated(IngestParams.FORMAT, List("TSV"))
        ++ Map(IngestParams.COLS -> List("1-3"))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines) must beASuccessfulTry
    }

    "properly write the features from a valid CSV containing WKT geometries" in {
      val path = Tools.getClass.getResource("/test_valid_wkt.csv")
      val ingest = new SVIngest(new Args(csvWktParams.updated(IngestParams.SFT_SPEC,
        List("time:Date,*geom:Point:srid=4326")) ++ Map(IngestParams.COLS -> List("1-2"))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines) must beASuccessfulTry
    }

    "properly write the features from a valid TSV containing WKT geometries" in {
      val path = Tools.getClass.getResource("/test_valid_wkt.tsv")
      val ingest = new SVIngest(new Args(csvWktParams.updated(IngestParams.SFT_SPEC,
        List("time:Date,*geom:Point:srid=4326")).updated(IngestParams.FORMAT, List("TSV"))
        ++ Map(IngestParams.COLS -> List("1-2"))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines) must beASuccessfulTry
    }
  }
}
