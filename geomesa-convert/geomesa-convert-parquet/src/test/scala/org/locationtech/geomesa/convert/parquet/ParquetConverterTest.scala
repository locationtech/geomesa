/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.parquet

import com.typesafe.config.ConfigFactory
import org.geotools.util.factory.Hints
import org.junit.runner.RunWith
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert2.SimpleFeatureConverter
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
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

  sequential

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

    "infer a converter from a geomesa parquet file" >> {
      val file = getClass.getClassLoader.getResource("example.parquet")
      val path = new File(file.toURI).getAbsolutePath

      val factory = new ParquetConverterFactory()
      val inferred = factory.infer(file.openStream(), None, EvaluationContext.inputFileParam(path))
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

    "infer a converter from a non-geomesa parquet file" >> {
      // corresponds to our example.json file, converted to parquet through spark sql
      val file = getClass.getClassLoader.getResource("infer.parquet")
      val path = new File(file.toURI).getAbsolutePath

      val factory = new ParquetConverterFactory()
      val inferred = factory.infer(file.openStream(), None, EvaluationContext.inputFileParam(path))
      inferred must beASuccessfulTry

      val (sft, config) = inferred.get

      SimpleFeatureTypes.encodeType(sft) mustEqual
          "color:String,id_0:Long,lat:Double,lon:Double,number:Long,height:String,weight:Double,*geom:Point:srid=4326"

      val converter = factory.apply(sft, config)
      converter must beSome

      val ec = converter.get.createEvaluationContext(EvaluationContext.inputFileParam(path))
      val features = converter.get.process(file.openStream(), ec).toList
      converter.get.close()
      features must haveLength(3)
      features(0).getAttributes.asScala mustEqual Seq("red", 1L, 0d, 0d, 123L, "5'11", 127.5d, WKTUtils.read("POINT (0 0)"))
      features(1).getAttributes.asScala mustEqual Seq("blue", 2L, 1d, 1d, 456L, "5'11", 150d, WKTUtils.read("POINT (1 1)"))
      features(2).getAttributes.asScala mustEqual Seq("green", 3L, 4.4d, 3.3d, 789L, "6'2", 200.4d, WKTUtils.read("POINT (3.3 4.4)"))
    }

    "handle all attribute types" >> {
      val file = getClass.getClassLoader.getResource("infer-complex.parquet")
      val path = new File(file.toURI).getAbsolutePath

      val factory = new ParquetConverterFactory()
      val inferred = factory.infer(file.openStream(), None, EvaluationContext.inputFileParam(path))
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
