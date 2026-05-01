/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.parquet.io

import org.apache.parquet.conf.PlainParquetConfiguration
import org.geotools.api.feature.simple.SimpleFeature
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.fs.storage.core.fs.LocalObjectStore
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.jts.geom._
import org.specs2.mutable.SpecificationWithJUnit

import java.util.{Date, UUID}

class SimpleFeatureReadSupportTest extends SpecificationWithJUnit {

  import scala.collection.JavaConverters._

  "SimpleFeatureReadSupport" should {
    "read old geomesa files" in {
      val sft = SimpleFeatureTypes.createType("example", "fid:Integer,name:String,age:Integer,lastseen:Date,*geom:Point:srid=4326")
      val conf = new PlainParquetConfiguration()
      SimpleFeatureParquetSchema.setSft(conf, sft)

      val file = getClass.getClassLoader.getResource("example.parquet").toURI

      WithClose(ParquetFileSystemReader.builder(LocalObjectStore, file).withConf(conf).build()) { reader =>
        val features = Iterator.continually(reader.read()).takeWhile(_ != null).toList
        features must haveLength(3)
        features.map(_.getID) mustEqual Seq("23623", "26236", "3233")
        features.map(_.getAttribute("fid")) mustEqual Seq(23623, 26236, 3233)
        features.map(_.getAttribute("name")) mustEqual Seq("Harry", "Hermione", "Severus")
        features.map(_.getAttribute("age")) mustEqual Seq(20, 25, 30)
        features.map(_.getAttribute("lastseen")) mustEqual Seq("2015-05-06", "2015-06-07", "2015-10-23").map(FastConverter.convert(_, classOf[Date]))
        features.map(_.getAttribute("geom")) mustEqual Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)").map(FastConverter.convert(_, classOf[Point]))
      }
    }

    "read old geomesa files with vis" in {
      val sft = SimpleFeatureTypes.createType("example", "fid:Integer,name:String,age:Integer,lastseen:Date,*geom:Point:srid=4326")
      val conf = new PlainParquetConfiguration()
      SimpleFeatureParquetSchema.setSft(conf, sft)

      val file = getClass.getClassLoader.getResource("example-with-vis.parquet")

      WithClose(ParquetFileSystemReader.builder(LocalObjectStore, file.toURI).withConf(conf).build()) { reader =>
        val features = Iterator.continually(reader.read()).takeWhile(_ != null).toList
        features must haveLength(3)
        features.map(_.getID) mustEqual Seq("23623", "26236", "3233")
        features.map(_.getAttribute("fid")) mustEqual Seq(23623, 26236, 3233)
        features.map(_.getAttribute("name")) mustEqual Seq("Harry", "Hermione", "Severus")
        features.map(_.getAttribute("age")) mustEqual Seq(20, 25, 30)
        features.map(_.getAttribute("lastseen")) mustEqual Seq("2015-05-06", "2015-06-07", "2015-10-23").map(FastConverter.convert(_, classOf[Date]))
        features.map(_.getAttribute("geom")) mustEqual Seq("POINT (-100.2365 23)", "POINT (40.232 -53.2356)", "POINT (3 -62.23)").map(FastConverter.convert(_, classOf[Point]))
        features.map(_.getUserData.get("geomesa.feature.visibility")) mustEqual Seq("user", "user", "user&admin")
      }
    }

    "read old geomesa parquet files, for all supported encodings" >> {
      val sft =
        SimpleFeatureTypes.createType("example",
          "dtg:Date,name:String,age:Integer,time:Long,height:Float,weight:Double,bool:Boolean,uuid:UUID,bytes:Bytes," +
            "list:List[String],map:Map[String,Long],line:LineString,multipt:MultiPoint,poly:Polygon," +
            "multiline:MultiLineString,multipoly:MultiPolygon,geom:Geometry,pt:Point")

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

      def comparableBytes(f: SimpleFeature): String = ByteArrays.toHex(f.getAttribute("bytes").asInstanceOf[Array[Byte]])

      val conf = new PlainParquetConfiguration()
      SimpleFeatureParquetSchema.setSft(conf, sft)

      val files =
        Seq("geomesav1-test.parquet", "geoparquet-native-test.parquet", "geoparquet-wkb-test.parquet")
          .map(getClass.getClassLoader.getResource)
      foreach(files) { file =>
        WithClose(ParquetFileSystemReader.builder(LocalObjectStore, file.toURI).withConf(conf).build()) { reader =>
          val features = Iterator.continually(reader.read()).takeWhile(_ != null).toList
          features must haveLength(10)
          foreach(sft.getAttributeDescriptors.asScala.map(_.getLocalName)) {
            case "bytes" => features.map(comparableBytes) mustEqual expected.map(comparableBytes)
            case a => features.map(_.getAttribute(a)) mustEqual expected.map(_.getAttribute(a))
          }
        }

      }
    }
  }
}
