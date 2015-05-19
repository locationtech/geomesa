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

import java.util.{Date, UUID}

import com.twitter.scalding.{Args, Mode}
import com.vividsolutions.jts.geom.Geometry
import org.geotools.data.DataStoreFinder
import org.geotools.filter.identity.FeatureIdImpl
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.params
import org.locationtech.geomesa.accumulo.util.SftBuilder
import org.locationtech.geomesa.features.avro.AvroSimpleFeature
import org.locationtech.geomesa.tools.Utils.IngestParams
import org.locationtech.geomesa.tools.ingest.{ColsParser, ScaldingDelimitedIngestJob}
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source
import scala.util.Try

@RunWith(classOf[JUnitRunner])
class ScaldingDelimitedIngestJobTest extends Specification{
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
      IngestParams.IS_TEST_INGEST -> List("true"),
      IngestParams.LIST_DELIMITER -> List(","),
      IngestParams.MAP_DELIMITERS -> List(",", ";"))
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
      IngestParams.IS_TEST_INGEST -> List("true"),
      IngestParams.LIST_DELIMITER -> List(","),
      IngestParams.MAP_DELIMITERS -> List(",", ";"))
  }

  def currentCatalog = f"DelimitedIngestTestTableUnique$id%d"

  def paramMap = Map[String, String](
    params.instanceIdParam.getName -> "mycloud",
    params.zookeepersParam.getName -> "zoo1:2181,zoo2:2181,zoo3:2181",
    params.userParam.getName       -> "myser",
    params.passwordParam.getName   -> "mypassword",
    params.tableNameParam.getName  -> currentCatalog,
    params.mockParam.getName       -> "true")

  def ds = DataStoreFinder.getDataStore(paramMap).asInstanceOf[AccumuloDataStore]
  
  "DelimitedIngest" should {
    "properly add attributes to an AvroSimpleFeature from a comma-delimited string" in {
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),  new Args(csvNormParams)))
      val testString = "1325409954,2013-07-17,-90.368732,35.3155"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      ingest.ingestDataToFeature(testString, f)

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(4) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a comma-delimited string with quote wrapped fields" in {
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)), new Args(csvNormParams)))
      val testString = "\"1325409954\",\"2013-07-17\",\"-90.368732\",\"35.3155\""
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      ingest.ingestDataToFeature(testString, f)

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(4) must beAnInstanceOf[Geometry]
    }

    "properly fail to add attributes to an AvroSimpleFeature from a comma-delimited string with mangled quote wrapped fields" in {
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),  new Args(csvNormParams)))
      val testString = "\"1325409954\",2013-07-17\",\"-90.368732\",\"35.3155"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      ingest.ingestDataToFeature(testString, f) must throwA[Exception]
    }

    "properly add attributes to an AvroSimpleFeature from a tab-delimited string" in {
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),  new Args(csvNormParams.updated(IngestParams.FORMAT, List("TSV")))))
      val testString = "1325409954\t2013-07-17\t-90.368732\t35.3155"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      ingest.ingestDataToFeature(testString, f)

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(4) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a tab-delimited string with quote wrapped fields" in {
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams.updated(IngestParams.FORMAT, List("TSV")))))
      val testString = "\"1325409954\"\t\"2013-07-17\"\t\"-90.368732\"\t\"35.3155\""
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      ingest.ingestDataToFeature(testString, f)

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(4) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a comma-delimited string with a unix (milliseconds) datetime field" in {
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams.updated(IngestParams.DT_FIELD, List("time")).updated(IngestParams.DT_FORMAT, List.empty))))
      val testString = "1325409954,1410199543000,-90.368732,35.3155"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      ingest.ingestDataToFeature(testString, f)

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(3) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(4) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a comma-delimited string with no date time field or format" in {
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams.updated(IngestParams.DT_FORMAT, List.empty).updated(IngestParams.DT_FIELD, List.empty))))
      val testString = "1325409954,-90.368732,35.3155"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      ingest.ingestDataToFeature(testString, f)

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(3) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a tab-delimited string with no date time field or format" in {
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams.updated(IngestParams.DT_FORMAT, List.empty)
        .updated(IngestParams.DT_FIELD, List.empty)
        .updated(IngestParams.FORMAT, List("TSV")))))
      val testString = "1325409954\t-90.368732\t35.3155"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,lon:Double,lat:Double,*geom:Point:srid=4326")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      ingest.ingestDataToFeature(testString, f)

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(2) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(3) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a comma-delimited string with a Point WKT geometry" in {
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)), new Args(csvWktParams)))
      val testString = "294908082,2013-07-17,POINT(-90.161852 32.39271)"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,*geom:Geometry")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      ingest.ingestDataToFeature(testString, f)

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a tab-delimited string with a Point WKT geometry" in {
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)), new Args(csvWktParams.updated(IngestParams.FORMAT, List("TSV")))))
      val testString = "294908082\t2013-07-17\tPOINT(-90.161852 32.39271)"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,*geom:Geometry")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      Try(ingest.ingestDataToFeature(testString, f)) must beASuccessfulTry

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a comma-delimited string with a Polygon WKT geometry" in {
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),  new Args(csvWktParams)))
      val testString = "294908082,2013-07-17,\"POLYGON((0 0, 0 10, 10 10, 0 0))\""
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,*geom:Geometry")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      ingest.ingestDataToFeature(testString, f)

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a tab-delimited string with a Polygon WKT geometry" in {
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)), new Args(csvWktParams.updated(IngestParams.FORMAT, List("TSV")))))
      val testString = "294908082\t2013-07-17\tPOLYGON((0 0, 0 10, 10 10, 0 0))"
      val sft = SimpleFeatureTypes.createType("test_type", "fid:Double,time:Date,*geom:Geometry")
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      ingest.ingestDataToFeature(testString, f)

      f.getAttribute(0) must beAnInstanceOf[java.lang.Double]
      f.getAttribute(1) must beAnInstanceOf[java.util.Date]
      f.getAttribute(2) must beAnInstanceOf[Geometry]
    }

    "properly add attributes to an AvroSimpleFeature from a tab-delimited string with" +
      " a Point WKT geometry and non-standard dtformat" in {
      val spec = new SftBuilder()
        .stringType("fid")
        .stringType("username")
        .stringType("userid")
        .stringType("text")
        .date("time", default = true)
        .point("geom", default = true)
        .getSpec
      val params = (csvNormParams - IngestParams.LAT_ATTRIBUTE - IngestParams.LON_ATTRIBUTE)
        .updated(IngestParams.DT_FORMAT, List("yyyy/MM/dd :HH:mm:ss:"))
        .updated(IngestParams.FORMAT, List("TSV"))
        .updated(IngestParams.SFT_SPEC, List(spec))

      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),  new Args(params)))
      val testString = "0000\tgeomesa user\t823543\tGeoMesa rules!\t2014/08/13 :06:06:06:\tPOINT(-78.4 38.0)"
      val sft = SimpleFeatureTypes.createType("test_type", spec)
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      ingest.ingestDataToFeature(testString, f)

      f.getAttribute(0) must beAnInstanceOf[java.lang.String]
      f.getAttribute(1) must beAnInstanceOf[java.lang.String]
      f.getAttribute(2) must beAnInstanceOf[java.lang.String]
      f.getAttribute(3) must beAnInstanceOf[java.lang.String]
      f.getAttribute(4) must beAnInstanceOf[java.util.Date]
      f.getAttribute(5) must beAnInstanceOf[Geometry]
    }

    "throw an exception when lon/lat field names are provided in args but aren't in sft" in {
      val spec = "fid:String,username:String,userid:String,text:String,time:Date,*geom:Point:srid=4326"
      val params = csvNormParams
        .updated(IngestParams.DT_FORMAT, List("yyyy/MM/dd :HH:mm:ss:"))
        .updated(IngestParams.FORMAT, List("TSV"))
        .updated(IngestParams.SFT_SPEC, List(spec))

      csvNormParams.keys must contain(IngestParams.LAT_ATTRIBUTE)
      csvNormParams.keys must contain(IngestParams.LON_ATTRIBUTE)

      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),  new Args(params)))
      val testString = "0000\tgeomesa user\t823543\tGeoMesa rules!\t2014/08/13 :06:06:06:\tPoint(-78.4 38.0)"
      val sft = SimpleFeatureTypes.createType("test_type", spec)
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      ingest.ingestDataToFeature(testString, f) must throwAn[IllegalArgumentException]
    }

    "drop lon/lat from ingest to create a point geom only" in {
      val spec = "vec:List[Int],time:Date,*geom:Point:srid=4326"
      val params = csvNormParams
        .updated(IngestParams.DT_FORMAT, List("yyyy/MM/dd HH:mm:ss"))
        .updated(IngestParams.FORMAT, List("csv"))
        .updated(IngestParams.SFT_SPEC, List(spec))
        .updated(IngestParams.LON_ATTRIBUTE, List("1"))
        .updated(IngestParams.LAT_ATTRIBUTE, List("2"))
        .updated(IngestParams.COLS, List("0,3"))

      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),  new Args(params)))
      val testString = List(
        "9,8,7",
        "-78.4",
        "38.0",
        "2014/08/13 06:06:06").map("\"" + _ + "\"").mkString(",")
      val sft = SimpleFeatureTypes.createType("test_type", spec)
      val f = new AvroSimpleFeature(new FeatureIdImpl("test_type"), sft)
      ingest.ingestDataToFeature(testString, f)

      type JList[T] = java.util.List[T]
      f.get[JList[Integer]](0).toList mustEqual List(9, 8, 7)
      f.point.getX mustEqual -78.4
      f.point.getY mustEqual 38.0

    }

    "properly write the features from a valid CSV" in {
      val path = Runner.getClass.getResource("/test_valid.csv")
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams)))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines()) must beASuccessfulTry
    }

    "properly write the features from a valid TSV" in {
      val path = Runner.getClass.getResource("/test_valid.tsv")
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams.updated(IngestParams.FORMAT, List("TSV")))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines()) must beASuccessfulTry
    }

    "properly write the features from a valid CSV containing WKT geometries" in {
      val path = Runner.getClass.getResource("/test_valid_wkt.csv")
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvWktParams)))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines()) must beASuccessfulTry
    }

    "properly write the features from a valid TSV containing WKT geometries" in {
      val path = Runner.getClass.getResource("/test_valid_wkt.tsv")
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvWktParams.updated(IngestParams.FORMAT, List("TSV")))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines()) must beASuccessfulTry
    }

    "properly write the features from a valid CSV with no date/time" in {
      val path = Runner.getClass.getResource("/test_valid_nd.csv")
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams.updated(IngestParams.DT_FORMAT, List.empty)
        .updated(IngestParams.DT_FIELD, List.empty)
        .updated(IngestParams.SFT_SPEC, List("fid:Double,lon:Double,lat:Double,*geom:Point:srid=4326")))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines()) must beASuccessfulTry
    }

    "properly write the features from a valid TSV with no date/time" in {
      val path = Runner.getClass.getResource("/test_valid_nd.tsv")
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams.updated(IngestParams.DT_FORMAT, List.empty)
        .updated(IngestParams.DT_FIELD, List.empty).updated(IngestParams.FORMAT, List("TSV"))
        .updated(IngestParams.SFT_SPEC, List("fid:Double,lon:Double,lat:Double,*geom:Point:srid=4326")))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines()) must beASuccessfulTry
    }

    "properly create subset of column list" in {
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams)))
      val inputColsList1 = "0,1,3,7-9,12,13"
      val inputColsList2 = "0,3,1,3,1,7-9,12,13"
      val checkList = List(0,1,3,7,8,9,12,13)

      val resultList1 = ColsParser.build(inputColsList1)
      resultList1 mustEqual checkList

      val resultList2 = ColsParser.build(inputColsList2)
      resultList2 mustEqual checkList
    }

    "properly fail in creating subset of column list" in {
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams)))
      Try(ColsParser.build("1,3,-7,12,13")) must beFailedTry
      Try(ColsParser.build("1,3,9-7,12,13")) must beFailedTry
      Try(ColsParser.build("1,3,-7-9,12,13")) must beFailedTry
    }

    "properly write a subset of features from a valid CSV" in {
      val path = Runner.getClass.getResource("/test_valid.csv")
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams.updated(IngestParams.SFT_SPEC,
        List("time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")) ++ Map(IngestParams.COLS -> List("1-3")))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines()) must beASuccessfulTry
    }

    "properly write a subset of features from a valid TSV" in {
      val path = Runner.getClass.getResource("/test_valid.tsv")
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams.updated(IngestParams.SFT_SPEC,
        List("time:Date,lon:Double,lat:Double,*geom:Point:srid=4326")).updated(IngestParams.FORMAT, List("TSV"))
        ++ Map(IngestParams.COLS -> List("1-3")))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines()) must beASuccessfulTry
    }

    "properly write the features from a valid CSV containing WKT geometries" in {
      val path = Runner.getClass.getResource("/test_valid_wkt.csv")
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvWktParams.updated(IngestParams.SFT_SPEC,
        List("time:Date,*geom:Geometry:srid=4326")) ++ Map(IngestParams.COLS -> List("1-2")))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines()) must beASuccessfulTry
    }

    "properly write the features from a valid TSV containing WKT geometries" in {
      val path = Runner.getClass.getResource("/test_valid_wkt.tsv")
      val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
          new Args(csvWktParams.updated(IngestParams.SFT_SPEC,
        List("time:Date,*geom:Geometry:srid=4326")).updated(IngestParams.FORMAT, List("TSV"))
        ++ Map(IngestParams.COLS -> List("1-2")))))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines()) must beASuccessfulTry
    }

    "ingest using lat/lon to set default geom without lat/lon attributes in sft" in {
      def doTest(testFileName: String, format: String, featureName: String) = {
        val path = Runner.getClass.getResource(testFileName)
        val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
          new Args(csvNormParams
          .updated(IngestParams.SFT_SPEC, List("i:Integer,*geom:Point:srid=4326"))
          .updated(IngestParams.FORMAT, List(format))
          .updated(IngestParams.FEATURE_NAME, List(featureName))
          .updated(IngestParams.LON_ATTRIBUTE, List("3"))
          .updated(IngestParams.LAT_ATTRIBUTE, List("4"))
          .updated(IngestParams.COLS, List("0")))))

        ingest.runTestIngest(Source.fromFile(path.toURI).getLines()) must beASuccessfulTry

        val geoms = ds.getFeatureSource(featureName).getFeatures.features().map { sf =>
          sf.get[Integer]("i") -> (sf.point.getX, sf.point.getY)
        }.toMap

        (1 to 6).zip(Seq(
          (-90.368732, 35.3155),
          (-70.970585, 42.36211),
          (-97.599004, 30.50901),
          (-89.901051, 38.56421),
          (-117.051022, 32.61654),
          (-118.027531, 34.07623)))
        .map { case (idx, xy) => geoms(idx) mustEqual xy }
      }
      doTest("/test_list.csv", "csv", "csvFeature")
      doTest("/test_list.tsv", "tsv", "tsvFeature")
      success
    }

    "write a list and handle spaces, empty lists, and single element lists...and a pipe for list delimiter!" in {
      def doTest(format: String, delim: String, quote: String, featureName: String) = {
        val spec = "name:String,foobar:List[String],*geom:Point:srid=4326"
        val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
          new Args(
          csvWktParams
            .updated(IngestParams.SFT_SPEC, List(spec))
            .updated(IngestParams.FORMAT, List(format))
            .updated(IngestParams.FEATURE_NAME, List(featureName))
            .updated(IngestParams.LIST_DELIMITER, List("|")))))
        val sft = SimpleFeatureTypes.createType(featureName, spec)
        val testStrings = List(
          List("1","a|b|c","POINT(1 1)"),
          List("2","a| b| c","POINT(1 1)"),
          List("3","a","POINT(1 1)"),
          List("4","","POINT(1 1)"),
          List("5","a| b| c","POINT(1 1)")
        ).map { lst => lst.map(quote + _ + quote).mkString(delim)}

        val features = testStrings.map { s =>
          val sf = new AvroSimpleFeature(new FeatureIdImpl(featureName), sft)
          ingest.ingestDataToFeature(s, sf)
          sf
        }

        ingest.runTestIngest(testStrings.toIterator) must beASuccessfulTry
        forall(features) { _.getAttribute("foobar") must beAnInstanceOf[java.util.List[Integer]] }

        val values = ds.getFeatureSource(featureName).getFeatures.features().map { sf =>
          // must convert list to scala list for equality tests below
          sf.get[String]("name") -> sf.get[java.util.List[Integer]]("foobar").toList
        }.toMap

        values("1") mustEqual List("a", "b", "c")
        values("2") mustEqual List("a", "b", "c")
        values("3") mustEqual List("a")
        values("4") mustEqual List()
        values("5") mustEqual List("a", "b", "c")
      }

      doTest("csv", ",", "\"", "csvtestfeature")
      doTest("tsv", "\t", "", "tsvtestfeature")
      success
    }

    "ingest a list as from a file" in {
      def doTest(testFileName: String, format: String, featureName: String) = {
        val path = Runner.getClass.getResource(testFileName)
        val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
          new Args(csvNormParams.updated(IngestParams.SFT_SPEC,
          List("i:Integer,numbers:List[Integer],time:Date,lon:Double,lat:Double,*geom:Point:srid=4326"))
          .updated(IngestParams.FORMAT, List(format))
          .updated(IngestParams.FEATURE_NAME, List(featureName)))))

        ingest.runTestIngest(Source.fromFile(path.toURI).getLines()) must beASuccessfulTry

        val values = ds.getFeatureSource(featureName).getFeatures.features().map { sf =>
          // must convert list to scala list for equality tests below
          sf.get[Integer]("i") -> sf.get[java.util.List[Integer]]("numbers").toList
        }.toMap

        values(1) mustEqual List(1, 2, 3, 4)
        values(2) mustEqual List(5, 6, 7, 8)
        values(3) mustEqual List(9, 10)
        values(4) mustEqual List(11)
        values(5) mustEqual List()
        values(6) mustEqual List(111, 222)
      }
      doTest("/test_list.csv", "csv", "csvMapFeature")
      doTest("/test_list.tsv", "tsv", "tsvMapFeature")
      success
    }

    "ingest a map from a file" in {
      def doTest(testFileName: String, format: String, featureName: String) = {
        val path = Runner.getClass.getResource(testFileName)
        val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
          new Args(csvNormParams.updated(IngestParams.SFT_SPEC,
          List("i:Integer,numbers:Map[Integer,String],time:Date,lon:Double,lat:Double,*geom:Point:srid=4326"))
          .updated(IngestParams.FORMAT, List(format))
          .updated(IngestParams.FEATURE_NAME, List(featureName)))))

        ingest.runTestIngest(Source.fromFile(path.toURI).getLines()) must beASuccessfulTry

        val values = ds.getFeatureSource(featureName).getFeatures.features().map { sf =>
          // must convert list to scala list for equality tests below
          sf.get[Integer]("i") -> sf.get[java.util.Map[Integer, String]]("numbers").toMap
        }.toMap

        values.size mustEqual 6

        values(1) mustEqual Map(1 -> "a", 2 -> "b", 3 -> "c", 4 -> "d")
        values(2) mustEqual Map(5 -> "e", 6 -> "f", 7 -> "g", 8 -> "h")
        values(3) mustEqual Map(9 -> "i", 10 -> "j")
        values(4) mustEqual Map(11 -> "k")
        values(5) mustEqual Map()
        values(6) mustEqual Map(111 -> "y", 222 -> "zz")
      }
      doTest("/test_map.csv", "csv", "csvMapFeature2")
      doTest("/test_map.tsv", "tsv", "tsvMapFeature2")
      success
    }

    "ingest a map with custom delimiters from a file" in {
      def doTest(testFileName: String, format: String, featureName: String) = {
        val path = Runner.getClass.getResource(testFileName)
        val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
          new Args(csvNormParams.updated(IngestParams.SFT_SPEC,
          List("i:Integer,numbers:Map[Integer,String],time:Date,lon:Double,lat:Double,*geom:Point:srid=4326"))
          .updated(IngestParams.FORMAT, List(format))
          .updated(IngestParams.FEATURE_NAME, List(featureName))
          .updated(IngestParams.MAP_DELIMITERS, List(":", "_")))))

        ingest.runTestIngest(Source.fromFile(path.toURI).getLines()) must beASuccessfulTry

        val values = ds.getFeatureSource(featureName).getFeatures.features().map { sf =>
          // must convert list to scala list for equality tests below
          sf.get[Integer]("i") -> sf.get[java.util.Map[Integer, String]]("numbers").toMap
        }.toMap

        values.size mustEqual 6

        values(1) mustEqual Map(1 -> "a", 2 -> "b", 3 -> "c", 4 -> "d")
        values(2) mustEqual Map(5 -> "e", 6 -> "f", 7 -> "g", 8 -> "h")
        values(3) mustEqual Map(9 -> "i", 10 -> "j")
        values(4) mustEqual Map(11 -> "k")
        values(5) mustEqual Map()
        values(6) mustEqual Map(111 -> "y", 222 -> "zz")
      }
      doTest("/test_map_custom_delimiters.csv", "csv", "csvMapFeature3")
      success
    }

    "handle all primitives from csv" in {

      def doTest(format: String, delim: String, quote: String, featureName: String) = {
        val spec = List(
          "name:String",
          "i:List[Integer]",
          "l:List[Long]",
          "f:List[Float]",
          "d:List[Double]",
          "s:List[String]",
          "b:List[Boolean]",
          "u:List[UUID]",
          "dt:List[Date]",
          "*geom:Point:srid=4326").mkString(",")
        val ingest = new ScaldingDelimitedIngestJob(Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
          new Args(
          csvWktParams.updated(IngestParams.SFT_SPEC,
            List(spec))
            .updated(IngestParams.FORMAT, List(format))
            .updated(IngestParams.FEATURE_NAME, List(featureName)))))
        val sft = SimpleFeatureTypes.createType(featureName, spec)
        val testString = List(
          "somename",
          "1,2,3,4",
          "1,2,3,4",
          "1.0, 2.0, 3.0",
          "1.0, 2.0, 3.0",
          "a,b,c",
          "true, false, true",
          "12345678-1234-1234-1234-123456789012,00000000-0000-0000-0000-000000000000",
          "2014-01-01,2014-01-02, 2014-01-03",
          "POINT(1 2)").map(quote + _ + quote).mkString(delim)

        val f1 = new AvroSimpleFeature(new FeatureIdImpl(featureName), sft)
        ingest.ingestDataToFeature(testString, f1)

        type JList[T] = java.util.List[T]
        f1.get[String]("name")             mustEqual "somename"
        f1.get[JList[Integer]]("i").toList mustEqual List[Int](1, 2, 3, 4)
        f1.get[JList[Long]]("l").toList    mustEqual List[Long](1, 2, 3, 4)
        f1.get[JList[Float]]("f").toList   mustEqual List[Float](1.0f, 2.0f, 3.0f)
        f1.get[JList[Double]]("d").toList  mustEqual List[Double](1.0d, 2.0d, 3.0d)
        f1.get[JList[String]]("s").toList  mustEqual List[String]("a", "b", "c")
        f1.get[JList[Boolean]]("b").toList mustEqual List[Boolean](true, false, true)
        f1.get[JList[UUID]]("u").toList    mustEqual
          List("12345678-1234-1234-1234-123456789012",
            "00000000-0000-0000-0000-000000000000").map(UUID.fromString)
        f1.get[JList[Date]]("dt").toList   mustEqual
          List("2014-01-01", "2014-01-02", "2014-01-03").map { dt => new DateTime(dt).toDate}

        // FUN with Generics!!!! ... lists of dates as lists of uuids ? yay type erasure + jvm + scala?
        val foo = f1.get[JList[UUID]]("dt").toList
        val bar = List("2014-01-01", "2014-01-02", "2014-01-03")
          .map { dt => new DateTime(dt).withZone(DateTimeZone.UTC).toDate}.toList
        val baz = f1.get[JList[Date]]("dt").toList
        foo mustEqual bar
        bar mustEqual baz
        foo(0) must throwA[ClassCastException]
        baz(0) must not(throwA[ClassCastException])
      }

      doTest("csv", ",", "\"", "csvtestfeature")
      doTest("tsv", "\t", "", "tsvtestfeature")
      success
    }
  }
}
