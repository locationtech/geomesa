/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools

import java.util.{Date, UUID}

import com.twitter.scalding.{Args, Mode}
import com.typesafe.config.ConfigFactory
import com.vividsolutions.jts.geom.Geometry
import org.apache.commons.io.IOUtils
import org.geotools.data.DataStoreFinder
import org.geotools.filter.identity.FeatureIdImpl
import org.joda.time.{DateTime, DateTimeZone}
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.params
import org.locationtech.geomesa.features.avro.AvroSimpleFeature
import org.locationtech.geomesa.tools.Utils.IngestParams
import org.locationtech.geomesa.tools.ingest.ScaldingConverterIngestJob
import org.locationtech.geomesa.utils.geotools.Conversions._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.io.Source
import scala.util.Try

@RunWith(classOf[JUnitRunner])
class ScaldingConverterIngestJobTest extends Specification{
  sequential

  var id = 0

  def csvNormParams: Map[String, List[String]] = {
    id = id + 1
    Map(
      IngestParams.FILE_PATH -> List(null),
      IngestParams.SFT_SPEC -> List("fid:Double,time:Date,lon:Double,lat:Double,*geom:Point:srid=4326"),
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
      IngestParams.FILE_PATH -> List(null),
      IngestParams.SFT_SPEC -> List("fid:Double,time:Date,*geom:Geometry"),
      IngestParams.FEATURE_NAME -> List("test_type"),
      IngestParams.CATALOG_TABLE -> List(currentCatalog),
      IngestParams.ACCUMULO_INSTANCE -> List("mycloud"),
      IngestParams.ZOOKEEPERS -> List("zoo1:2181,zoo2:2181,zoo3:2181"),
      IngestParams.ACCUMULO_USER -> List("myuser"),
      IngestParams.ACCUMULO_PASSWORD -> List("mypassword"),
      IngestParams.ACCUMULO_MOCK -> List("true"),
      IngestParams.IS_TEST_INGEST -> List("true"))
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
    "properly write the features from a valid CSV" in {
      val path = Runner.getClass.getResource("/test_valid.csv")
      val conf = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("test_valid.csv.convert"))
      val args = Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams.updated(IngestParams.CONVERTER_CONFIG, List(conf))))
      val ingest = new ScaldingConverterIngestJob(args)
      ds.createSchema(SimpleFeatureTypes.createType(args(IngestParams.FEATURE_NAME), args(IngestParams.SFT_SPEC) ))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines())
      ingest.counter.getLineCount mustEqual 6
      ingest.counter.getFailure mustEqual 0
      ingest.counter.getSuccess mustEqual 6
      success
    }

    "properly write the features from a valid TSV" in {
      val path = Runner.getClass.getResource("/test_valid.tsv")
      val conf = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("test_valid.tsv.convert"))
      val args = Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams.updated(IngestParams.CONVERTER_CONFIG, List(conf))))
      val ingest = new ScaldingConverterIngestJob(args)
      ds.createSchema(SimpleFeatureTypes.createType(args(IngestParams.FEATURE_NAME), args(IngestParams.SFT_SPEC) ))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines())
      ingest.counter.getLineCount mustEqual 6
      ingest.counter.getFailure mustEqual 0
      ingest.counter.getSuccess mustEqual 6
      success
    }

    "properly write the features from a valid CSV containing WKT geometries" in {
      val path = Runner.getClass.getResource("/test_valid_wkt.csv")
      val conf = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("test_valid_wkt.csv.convert"))
      val args = Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams.updated(IngestParams.CONVERTER_CONFIG, List(conf))))
      val ingest = new ScaldingConverterIngestJob(args)
      ds.createSchema(SimpleFeatureTypes.createType(args(IngestParams.FEATURE_NAME), args(IngestParams.SFT_SPEC) ))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines())
      ingest.counter.getLineCount mustEqual 5
      ingest.counter.getFailure mustEqual 0
      ingest.counter.getSuccess mustEqual 5
      success
    }

    "properly write the features from a valid TSV containing WKT geometries" in {
      val path = Runner.getClass.getResource("/test_valid_wkt.tsv")
      val conf = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("test_valid_wkt.tsv.convert"))
      val args = Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams.updated(IngestParams.CONVERTER_CONFIG, List(conf))))
      val ingest = new ScaldingConverterIngestJob(args)
      ds.createSchema(SimpleFeatureTypes.createType(args(IngestParams.FEATURE_NAME), args(IngestParams.SFT_SPEC) ))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines())
      ingest.counter.getLineCount mustEqual 5
      ingest.counter.getFailure mustEqual 0
      ingest.counter.getSuccess mustEqual 5
      success
    }

    "properly write the features from a valid CSV with no date/time" in {
      val path = Runner.getClass.getResource("/test_valid_nd.csv")
      val conf = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("test_valid_nd.csv.convert"))
      val args = Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams
          .updated(IngestParams.SFT_SPEC, List("fid:Double,lon:Double,lat:Double,*geom:Point:srid=4326"))
          .updated(IngestParams.CONVERTER_CONFIG, List(conf))))
      val ingest = new ScaldingConverterIngestJob(args)
      ds.createSchema(SimpleFeatureTypes.createType(args(IngestParams.FEATURE_NAME), args(IngestParams.SFT_SPEC) ))

      ingest.runTestIngest(Source.fromFile(path.toURI).getLines())
      ingest.counter.getLineCount mustEqual 6
      ingest.counter.getFailure mustEqual 0
      ingest.counter.getSuccess mustEqual 6
      success
    }

    "properly write the features from a valid TSV with no date/time" in {
      val path = Runner.getClass.getResource("/test_valid_nd.tsv")
      val conf = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream("test_valid_nd.tsv.convert"))
      val args = Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
        new Args(csvNormParams
          .updated(IngestParams.SFT_SPEC, List("fid:Double,lon:Double,lat:Double,*geom:Point:srid=4326"))
          .updated(IngestParams.CONVERTER_CONFIG, List(conf))))
      val ingest = new ScaldingConverterIngestJob(args)
      ds.createSchema(SimpleFeatureTypes.createType(args(IngestParams.FEATURE_NAME), args(IngestParams.SFT_SPEC) ))


      ingest.runTestIngest(Source.fromFile(path.toURI).getLines())
      ingest.counter.getLineCount mustEqual 6
      ingest.counter.getFailure mustEqual 0
      ingest.counter.getSuccess mustEqual 6
      success
    }

    "ingest a list with or without custom delimiters" in {
      def doTest(testFileName: String, format: String, featureName: String) = {
        val path = Runner.getClass.getResource("/" +testFileName)
        val conf = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream(s"$testFileName.convert"))

        val args = Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
          new Args(csvNormParams.updated(IngestParams.SFT_SPEC,
          List("i:Integer,numbers:List[Integer],time:Date,lon:Double,lat:Double,*geom:Point:srid=4326"))
          .updated(IngestParams.FEATURE_NAME, List(featureName))
          .updated(IngestParams.CONVERTER_CONFIG, List(conf))))
        val ingest = new ScaldingConverterIngestJob(args)
        ds.createSchema(SimpleFeatureTypes.createType(args(IngestParams.FEATURE_NAME), args(IngestParams.SFT_SPEC) ))
        ingest.runTestIngest(Source.fromFile(path.toURI).getLines())

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
      doTest("test_list_custom_delim.csv", "csv", "csvMapFeature")
      doTest("test_list.tsv", "tsv", "tsvMapFeature")
      success
    }

    "ingest a map with or without custom delimiters" in {
      def doTest(testFileName: String, format: String, featureName: String) = {
        val path = Runner.getClass.getResource("/" + testFileName)
        val conf = IOUtils.toString(this.getClass.getClassLoader.getResourceAsStream(s"$testFileName.convert"))

        val args = Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
          new Args(csvNormParams.updated(IngestParams.SFT_SPEC,
            List("i:Integer,numbers:Map[Integer,String],time:Date,lon:Double,lat:Double,*geom:Point:srid=4326"))
            .updated(IngestParams.FEATURE_NAME, List(featureName))
            .updated(IngestParams.CONVERTER_CONFIG, List(conf))))
        ds.createSchema(SimpleFeatureTypes.createType(args(IngestParams.FEATURE_NAME), args(IngestParams.SFT_SPEC) ))
        val ingest = new ScaldingConverterIngestJob(args)

        ingest.runTestIngest(Source.fromFile(path.toURI).getLines())

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
      doTest("test_map.csv", "csv", "csvMapFeature2")
      doTest("test_map_custom_delim.tsv", "tsv", "tsvMapFeature2")
      success
    }
    "handle all primitives from csv" in {

      def doTest(format: String, delim: String, featureName: String) = {
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

        val conf =
          """converter = {
            |   type         = "delimited-text",
            |   format       = "FORMAT",
            |   id-field     = "toString($i)",
            |   fields = [
            |     { name = "name", transform = "$1" },
            |     { name = "i",    transform = "parseList('int', $2)"},
            |     { name = "l",    transform = "parseList('long', $3)"},
            |     { name = "f",    transform = "parseList('float', $4)"},
            |     { name = "d",    transform = "parseList('double', $5)"},
            |     { name = "s",    transform = "parseList('string', $6)"},
            |     { name = "b",    transform = "parseList('boolean', $7)"},
            |     { name = "u",    transform = "parseList('uuid', $8)"},
            |     { name = "dt",   transform = "parseList('date', $9)"},
            |     { name = "geom", transform = "point($10)" }
            |   ]
            | }
            |""".stripMargin.replaceAllLiterally("FORMAT", format)

        val args = Mode.putMode(com.twitter.scalding.Test((s) => Some(mutable.Buffer.empty)),
          new Args(
            csvWktParams.updated(IngestParams.SFT_SPEC,
              List(spec))
              .updated(IngestParams.FEATURE_NAME, List(featureName))
              .updated(IngestParams.CONVERTER_CONFIG, List(conf))))
        ds.createSchema(SimpleFeatureTypes.createType(args(IngestParams.FEATURE_NAME), args(IngestParams.SFT_SPEC) ))
        val ingest = new ScaldingConverterIngestJob(args)
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
          "POINT(1 2)").map("\"" + _ + "\"").mkString(delim)

        ingest.runTestIngest(Seq(testString).iterator)

        val f1 = ds.getFeatureSource(featureName).getFeatures.features().next()

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

      doTest("csv", ",",  "csvtestfeature")
      doTest("tsv", "\t", "tsvtestfeature")
      success
    }
  }
}
