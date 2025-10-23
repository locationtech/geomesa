/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.convert.parquet

import com.typesafe.config.ConfigFactory
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import java.io.File
import java.util.{Date, UUID}

@RunWith(classOf[JUnitRunner])
class ParquetConverterTest extends Specification {

  import scala.collection.JavaConverters._

  "ParquetConverter" should {
    "parse a parquet file" in {
      val conf = ConfigFactory.parseString(
        """
          | {
          |   type         = "parquet",
          |   id-field     = "avroPath($0, '/__fid__')",
          |   fields = [
          |     { name = "fid",      transform = "avroPath($0, '/fid')" },
          |     { name = "name",     transform = "avroPath($0, '/name')" },
          |     { name = "age",      transform = "avroPath($0, '/age')" },
          |     { name = "lastseen", transform = "avroPath($0, '/lastseen')" },
          |     { name = "geom",     transform = "parquetPoint($0, '/geom')" },
          |   ]
          | }
        """.stripMargin)

      val sft = SimpleFeatureTypes.createType("test", "fid:Int,name:String,age:Int,lastseen:Date,*geom:Point:srid=4326")

      val file = getClass.getClassLoader.getResource("example.parquet")
      val path = new File(file.toURI).getAbsolutePath

      val res = WithClose(SimpleFeatureConverter(sft, conf)) { converter =>
        val ec = converter.createEvaluationContext(EvaluationContext.inputFileParam(path))
        WithClose(converter.process(file.openStream(), ec))(_.toList)
      }

      res must haveLength(3)
      res.map(_.getID) mustEqual Seq("23623", "26236", "3233")
      res.map(_.getAttribute("fid")) mustEqual Seq(23623, 26236, 3233)
      res.map(_.getAttribute("name")) mustEqual Seq("Harry", "Hermione", "Severus")
      res.map(_.getAttribute("age")) mustEqual Seq(20, 25, 30)
      res.map(_.getAttribute("lastseen")) mustEqual Seq("2015-05-06", "2015-06-07", "2015-10-23").map(FastConverter.convert(_, classOf[Date]))
      res.map(_.getAttribute("geom")) mustEqual Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)").map(FastConverter.convert(_, classOf[Point]))
    }

    "infer a converter from a simple geomesa parquet file" >> {
      val file = getClass.getClassLoader.getResource("example.parquet")
      val path = new File(file.toURI).getAbsolutePath

      val factory = new ParquetConverterFactory()
      val inferred = WithClose(file.openStream())(factory.infer(_, None, EvaluationContext.inputFileParam(path)))
      inferred must beASuccessfulTry

      val (sft, config) = inferred.get

      sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual Seq("fid", "name", "age", "lastseen", "geom")
      sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[java.lang.Integer], classOf[String], classOf[java.lang.Integer], classOf[Date], classOf[Point])

      val res = WithClose(SimpleFeatureConverter(sft, config)) { converter =>
        val ec = converter.createEvaluationContext(EvaluationContext.inputFileParam(path))
        WithClose(converter.process(file.openStream(), ec))(_.toList)
      }

      res must haveLength(3)
      res.map(_.getID) mustEqual Seq("23623", "26236", "3233")
      res.map(_.getAttribute("fid")) mustEqual Seq(23623, 26236, 3233)
      res.map(_.getAttribute("name")) mustEqual Seq("Harry", "Hermione", "Severus")
      res.map(_.getAttribute("age")) mustEqual Seq(20, 25, 30)
      res.map(_.getAttribute("lastseen")) mustEqual Seq("2015-05-06", "2015-06-07", "2015-10-23").map(FastConverter.convert(_, classOf[Date]))
      res.map(_.getAttribute("geom")) mustEqual Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)").map(FastConverter.convert(_, classOf[Point]))
    }

    "infer a converter from a complex geomesa parquet file, for all supported encodings" >> {
      val factory = new ParquetConverterFactory()
      val files =
        Seq("geomesav1-test.parquet", "geoparquet-native-test.parquet", "geoparquet-wkb-test.parquet")
          .map(getClass.getClassLoader.getResource)
      val attributes =
        Seq("dtg", "name", "age", "time", "height", "weight", "bool", "uuid", "bytes", "list", "map", "line",
            "multipt", "poly", "multiline", "multipoly", "geom", "pt")
      foreach(files) { file =>
        val path = new File(file.toURI).getAbsolutePath

        val inferred = WithClose(file.openStream())(factory.infer(_, None, EvaluationContext.inputFileParam(path)))
        inferred must beASuccessfulTry

        val (sft, config) = inferred.get

        sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual attributes
        sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[Date], classOf[String], classOf[java.lang.Integer], classOf[java.lang.Long], classOf[java.lang.Float],
            classOf[java.lang.Double], classOf[java.lang.Boolean], classOf[UUID], classOf[Array[Byte]],
            classOf[java.util.List[String]],classOf[java.util.Map[String,Long]], classOf[LineString], classOf[MultiPoint],
            classOf[Polygon], classOf[MultiLineString], classOf[MultiPolygon], classOf[Geometry], classOf[Point])

        val res = WithClose(SimpleFeatureConverter(sft, config)) { converter =>
          val ec = converter.createEvaluationContext(EvaluationContext.inputFileParam(path))
          WithClose(converter.process(file.openStream(), ec))(_.toList)
        }

        // copied from GenerateParquetFiles, which we used to create the test data
        val expected = Seq.tabulate(10) { i =>
          val sf = new ScalaSimpleFeature(sft, i.toString)
          sf.setAttribute("dtg", f"2014-01-${i + 1}%02dT00:00:01.000Z")
          sf.setAttribute("name", s"name$i")
          sf.setAttribute("age", s"$i")
          sf.setAttribute("time", s"$i")
          sf.setAttribute("height", s"$i")
          sf.setAttribute("weight", s"$i")
          sf.setAttribute("bool", Boolean.box(i < 5))
          sf.setAttribute("uuid", UUID.fromString(s"00000000-0000-0000-0000-00000000000$i"))
          sf.setAttribute("bytes", Array.tabulate[Byte](i)(i => i.toByte))
          sf.setAttribute("list", Seq.tabulate[String](i)(i => i.toString))
          sf.setAttribute("map", (0 until i).map(i => i.toString -> Long.box(i)).toMap)
          sf.setAttribute("line", s"LINESTRING(0 $i, 2 $i, 8 ${10 - i})")
          sf.setAttribute("multipt", s"MULTIPOINT(0 $i, 2 3)")
          sf.setAttribute("poly",
            if (i == 5) {
              // polygon example with holes from wikipedia
              "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30))"
            } else {
              s"POLYGON((40 3$i, 42 3$i, 42 2$i, 40 2$i, 40 3$i))"
            }
          )
          sf.setAttribute("multiline", s"MULTILINESTRING((0 2, 2 $i, 8 6),(0 $i, 2 $i, 8 ${10 - i}))")
          sf.setAttribute("multipoly", s"MULTIPOLYGON(((-1 0, 0 $i, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6), (-1 5, 2 5, 2 2, -1 2, -1 5)))")
          sf.setAttribute("geom", sf.getAttribute(Seq("line", "multipt", "poly", "multiline", "multipoly").drop(i % 5).head))
          sf.setAttribute("pt", s"POINT(4$i 5$i)")
          sf
        }

        res must haveLength(10)

        def comparableBytes(f: SimpleFeature): String = ByteArrays.toHex(f.getAttribute("bytes").asInstanceOf[Array[Byte]])

        foreach(attributes) {
          case "bytes" => res.map(comparableBytes) mustEqual expected.map(comparableBytes)
          case a => res.map(_.getAttribute(a)) mustEqual expected.map(_.getAttribute(a))
        }
      }
    }

    "infer a converter from a non-geomesa parquet file" >> {
      // corresponds to our example.json file, converted to parquet through spark sql
      val file = getClass.getClassLoader.getResource("infer.parquet")
      val path = new File(file.toURI).getAbsolutePath

      val factory = new ParquetConverterFactory()
      val inferred = WithClose(file.openStream())(factory.infer(_, None, EvaluationContext.inputFileParam(path)))
      inferred must beASuccessfulTry

      val (sft, config) = inferred.get

      SimpleFeatureTypes.encodeType(sft) mustEqual
          "color:String,id_0:Long,lat:Double,lon:Double,number:Long,physical_height:String,physical_weight:Double,*geom:Point:srid=4326"

      val converter = factory.apply(sft, config)
      converter must beSome

      val ec = converter.get.createEvaluationContext(EvaluationContext.inputFileParam(path))
      val features = WithClose(converter.get.process(file.openStream(), ec))(_.toList)
      converter.get.close()
      features must haveLength(3)
      features(0).getAttributes.asScala mustEqual Seq("red", 1L, 0d, 0d, 123L, "5'11", 127.5d, WKTUtils.read("POINT (0 0)"))
      features(1).getAttributes.asScala mustEqual Seq("blue", 2L, 1d, 1d, 456L, "5'11", 150d, WKTUtils.read("POINT (1 1)"))
      features(2).getAttributes.asScala mustEqual Seq("green", 3L, 4.4d, 3.3d, 789L, "6'2", 200.4d, WKTUtils.read("POINT (3.3 4.4)"))
    }

    "infer a converter from a non-geomesa GeoParquet file" >> {
      // this file was downloaded from https://emotional.byteroad.net/catalogue and
      // then trimmed using org.locationtech.geomesa.convert.parquet.TrimParquetFile
      val file = getClass.getClassLoader.getResource("hex350_grid_cardio_1920-10.parquet")
      val path = new File(file.toURI).getAbsolutePath

      val factory = new ParquetConverterFactory()
      val inferred = WithClose(file.openStream())(factory.infer(_, None, EvaluationContext.inputFileParam(path)))
      inferred must beASuccessfulTry

      val (sft, config) = inferred.get

      SimpleFeatureTypes.encodeType(sft) mustEqual
        "fid:Integer,Col_ID:Integer,Row_ID:Integer,Hex_ID:Integer,Centroid_X:Double,Centroid_Y:Double,area:Double," +
          "wprev_mean:Double,wprev_majority:Double,*geometry:MultiPolygon:srid=4326"

      val converter = factory.apply(sft, config)
      converter must beSome

      val ec = converter.get.createEvaluationContext(EvaluationContext.inputFileParam(path))
      val features = WithClose(converter.get.process(file.openStream(), ec))(_.toList)
      converter.get.close()
      features must haveLength(10)
      foreach(features)(_.getAttribute("geometry") must beAnInstanceOf[MultiPolygon])
      features.head.getAttributes.asScala mustEqual Seq(
        1,
        1120,
        1202,
        11201202,
        521148.5154,
        174050.7975,
        106088.115,
        0.8044027913184393,
        0.8047540783882141,
        WKTUtils.read("MULTIPOLYGON (((-0.260765354541958 51.45253141333989, -0.259371809771638 51.450937050803674, " +
          "-0.256465188455566 51.45089385920242, -0.254951912350778 51.452445025302715, -0.256345346487153 51.45403943897036, " +
          "-0.259252167374285 51.45408263540652, -0.260765354541958 51.45253141333989)))"),
      )
    }

    "infer a converter from a non-geomesa GeoParquet file" >> {
      // this file was downloaded from s3://overturemaps-us-west-2/release/2025-04-23.0/theme=base/type=infrastructure/
      // (see https://docs.overturemaps.org/release/latest/) and then trimmed using org.locationtech.geomesa.convert.parquet.TrimParquetFile
      val file = getClass.getClassLoader.getResource("part-00000-189f03e5-dde4-4576-bc83-4f1956a98011-c000-10.zstd.parquet")
      val path = new File(file.toURI).getAbsolutePath

      val factory = new ParquetConverterFactory()
      val inferred = WithClose(file.openStream())(factory.infer(_, None, EvaluationContext.inputFileParam(path)))
      inferred must beASuccessfulTry

      val (sft, config) = inferred.get

      SimpleFeatureTypes.encodeType(sft) mustEqual
        "id_0:String,*geometry:Geometry:srid=4326,version:Integer,sources:String:json=true,level:Integer,subtype:String," +
          "class:String,height:Double,surface:String,names_primary:String,names_common:Map[String,String]," +
          "names_rules:String:json=true,source_tags:Map[String,String],wikidata:String"

      val converter = factory.apply(sft, config)
      converter must beSome

      val ec = converter.get.createEvaluationContext(EvaluationContext.inputFileParam(path))
      val features = WithClose(converter.get.process(file.openStream(), ec))(_.toList)
      converter.get.close()
      features must haveLength(10)

      features.head.getAttributes.asScala mustEqual Seq(
        "08bbb364e428dfff0001bf556bd767b7",
        WKTUtils.read("LINESTRING (-176.6378811 -44.0466765, -176.6378513 -44.0466907)"),
        0,
        """[{"property":"","dataset":"OpenStreetMap","record_id":"w58443657@5","update_time":"2022-07-05T05:35:04.000Z"}]""",
        1,
        "bridge",
        "bridge",
        null,
        "concrete",
        null,
        null,
        null,
        java.util.Map.of("surface", "concrete", "highway", "tertiary", "bridge", "yes"),
        null,
      )
      features(5).getAttributes.asScala mustEqual Seq(
        "08bce9e59e212fff0001c31e8c83b392",
        WKTUtils.read("POLYGON ((-68.692393 -38.581357, -68.6930275 -38.5816525, -68.6932579 -38.5817623, -68.6934995 -38.5818808, " +
          "-68.6942836 -38.5822612, -68.6943095 -38.5822864, -68.6943127 -38.5823167, -68.6942933 -38.5823394, -68.6942611 -38.5823495, " +
          "-68.6942223 -38.582347, -68.6934104 -38.582004, -68.6932513 -38.5819403, -68.6930727 -38.5818583, -68.6875213 -38.5792975, " +
          "-68.6873743 -38.579228, -68.6872329 -38.5791518, -68.6865382 -38.5787749, -68.686909 -38.5788495, -68.6871056 -38.5789066, " +
          "-68.6872629 -38.5789681, -68.6904368 -38.5804461, -68.692393 -38.581357))"),
        0,
        """[{"property":"","dataset":"OpenStreetMap","record_id":"r13636394@2","update_time":"2022-01-12T08:25:42.000Z"}]""",
        null,
        "water",
        "dam",
        16.0,
        null,
        "Dique Marí Menuco",
        java.util.Map.of("en", "Dique Marí Menuco"),
        """[{"variant":"alternate","value":"Complejo hidroeléctrico Cerros Colorados"}]""",
        java.util.Map.of("operator", "Orazul Energy Cerros Colorados", "waterway", "dam"),
        "Q1056184",
      )
    }

    "read visibilities with an inferred geomesa parquet file" >> {
      val file = getClass.getClassLoader.getResource("example-with-vis.parquet")
      val path = new File(file.toURI).getAbsolutePath

      val factory = new ParquetConverterFactory()
      val inferred = WithClose(file.openStream())(factory.infer(_, None, EvaluationContext.inputFileParam(path)))
      inferred must beASuccessfulTry

      val (sft, config) = inferred.get

      sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual Seq("fid", "name", "age", "lastseen", "geom")
      sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
        Seq(classOf[java.lang.Integer], classOf[String], classOf[java.lang.Integer], classOf[Date], classOf[Point])

      val res = WithClose(SimpleFeatureConverter(sft, config)) { converter =>
        val ec = converter.createEvaluationContext(EvaluationContext.inputFileParam(path))
        WithClose(converter.process(file.openStream(), ec))(_.toList)
      }

      res must haveLength(3)
      res.map(_.getID) mustEqual Seq("23623", "26236", "3233")
      res.map(_.getAttribute("fid")) mustEqual Seq(23623, 26236, 3233)
      res.map(_.getAttribute("name")) mustEqual Seq("Harry", "Hermione", "Severus")
      res.map(_.getAttribute("age")) mustEqual Seq(20, 25, 30)
      res.map(_.getAttribute("lastseen")) mustEqual Seq("2015-05-06", "2015-06-07", "2015-10-23").map(FastConverter.convert(_, classOf[Date]))
      res.map(_.getAttribute("geom")) mustEqual Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)").map(FastConverter.convert(_, classOf[Point]))
      res.map(_.getUserData.get("geomesa.feature.visibility")) mustEqual Seq("user", "user", "user&admin")
    }

    "handle all attribute types" >> {
      val file = getClass.getClassLoader.getResource("infer-complex.parquet")
      val path = new File(file.toURI).getAbsolutePath

      val factory = new ParquetConverterFactory()
      val inferred = WithClose(file.openStream())(factory.infer(_, None, EvaluationContext.inputFileParam(path)))
      inferred must beASuccessfulTry

      val (sft, config) = inferred.get

      sft.getAttributeDescriptors.asScala.map(_.getLocalName) mustEqual
          Seq("name", "age", "time", "height", "weight", "bool", "uuid", "bytes", "list", "map",
            "line", "mpt", "poly", "mline", "mpoly", "dtg", "geom")
      sft.getAttributeDescriptors.asScala.map(_.getType.getBinding) mustEqual
          Seq(classOf[String], classOf[Integer], classOf[java.lang.Long], classOf[java.lang.Float],
            classOf[java.lang.Double], classOf[java.lang.Boolean], classOf[UUID], classOf[Array[Byte]],
            classOf[java.util.List[Integer]], classOf[java.util.Map[String, Long]],
            classOf[LineString], classOf[MultiPoint], classOf[Polygon], classOf[MultiLineString],
            classOf[MultiPolygon], classOf[Date], classOf[Point])

      val res = WithClose(SimpleFeatureConverter(sft, config)) { converter =>
        val ec = converter.createEvaluationContext(EvaluationContext.inputFileParam(path))
        WithClose(converter.process(file.openStream(), ec))(_.toList)
      }

      res must haveLength(10)

      // features used to write the parquet file
      val expected = Seq.tabulate(10) { i =>
        val sf = new ScalaSimpleFeature(sft, i.toString)
        sf.getUserData.put(Hints.USE_PROVIDED_FID, java.lang.Boolean.TRUE)
        sf.setAttribute("name", s"name$i")
        sf.setAttribute("age", s"$i")
        sf.setAttribute("time", s"$i")
        sf.setAttribute("height", s"$i")
        sf.setAttribute("weight", s"$i")
        sf.setAttribute("bool", Boolean.box(i < 5))
        sf.setAttribute("uuid", UUID.fromString(s"00000000-0000-0000-0000-00000000000$i"))
        sf.setAttribute("bytes", Array.tabulate[Byte](i)(i => i.toByte))
        sf.setAttribute("list", Seq.tabulate[Integer](i)(i => Int.box(i)))
        sf.setAttribute("map", (0 until i).map(i => i.toString -> Long.box(i)).toMap)
        sf.setAttribute("line", s"LINESTRING(0 $i, 2 $i, 8 ${10 - i})")
        sf.setAttribute("mpt", s"MULTIPOINT(0 $i, 2 3)")
        sf.setAttribute("poly",
          if (i == 5) {
            // multipolygon example from wikipedia
            "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30))"
          } else {
            s"POLYGON((40 3$i, 42 3$i, 42 2$i, 40 2$i, 40 3$i))"
          }
        )
        sf.setAttribute("mline", s"MULTILINESTRING((0 2, 2 $i, 8 6),(0 $i, 2 $i, 8 ${10 - i}))")
        sf.setAttribute("mpoly", s"MULTIPOLYGON(((-1 0, 0 $i, 1 0, 0 -1, -1 0)), ((-2 6, 1 6, 1 3, -2 3, -2 6)), ((-1 5, 2 5, 2 2, -1 2, -1 5)))")
        sf.setAttribute("dtg", f"2014-01-${i + 1}%02dT00:00:01.000Z")
        sf.setAttribute("geom", s"POINT(4$i 5$i)")
        sf
      }

      // compare bytes first, as they aren't 'equals'
      res.map(_.getAttribute("bytes").asInstanceOf[Array[Byte]].mkString(",")) mustEqual
          expected.map(_.getAttribute("bytes").asInstanceOf[Array[Byte]].mkString(","))

      // now clear the bytes and compare the rest of the attributes
      (res ++ expected).foreach(_.setAttribute("bytes", null))

      res mustEqual expected
    }
  }
}
