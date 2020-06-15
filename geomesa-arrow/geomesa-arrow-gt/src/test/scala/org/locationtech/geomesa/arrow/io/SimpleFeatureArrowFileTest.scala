/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.arrow.io

import java.io.{File, FileInputStream, FileOutputStream}
import java.nio.file.Files
import java.util.concurrent.atomic.AtomicInteger

import org.apache.arrow.vector.ipc.message.IpcOption
import org.geotools.filter.text.ecql.ECQL
import org.junit.runner.RunWith
import org.locationtech.geomesa.arrow.vector.ArrowDictionary
import org.locationtech.geomesa.arrow.vector.SimpleFeatureVector.SimpleFeatureEncoding
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.io.WithClose
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.locationtech.jts.geom.{Geometry, LineString, Point, Polygon}
import org.specs2.matcher.MatchResult
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SimpleFeatureArrowFileTest extends Specification {

  val fileCount = new AtomicInteger(0)

  val sft = SimpleFeatureTypes.createImmutableType("test", "name:String,foo:String,age:Int,dtg:Date,*geom:Point:srid=4326")
  val lineSft = SimpleFeatureTypes.createType("test", "name:String,team:String,age:Int,weight:Int,dtg:Date,*geom:LineString:srid=4326")
  val geomSft = SimpleFeatureTypes.createType("test", "name:String,team:String,age:Int,weight:Int,dtg:Date,*geom:Geometry:srid=4326")

  val features0 = (0 until 10).map { i =>
    ScalaSimpleFeature.create(sft, s"0$i", s"name0$i", s"foo${i % 2}", s"${i % 5}", s"2017-03-15T00:0$i:00.000Z", s"POINT (4$i 5$i)")
  }
  val features1 = (10 until 20).map { i =>
    ScalaSimpleFeature.create(sft, s"$i", s"name$i", s"foo${i % 3}", s"${i % 5}", s"2017-03-15T00:$i:00.000Z", s"POINT (4${i -10} 5${i -10})")
  }
  val lineFeatures = (0 until 10).map { i =>
    val name = s"name${i % 2}"
    val team = s"team$i"
    val age = i % 5
    val weight = Option(i % 3).filter(_ != 0).map(Int.box).orNull
    val geom = s"LINESTRING(40 6$i, 40.1 6$i, 40.2 6$i, 40.3 6$i)"
    ScalaSimpleFeature.create(lineSft, s"$i", name, team, age, weight, s"2017-02-03T00:0$i:01.000Z", geom)
  }
  val points: Seq[Geometry] = Seq("POINT(0 0)", "POINT(1 1)", "POINT(2 2)", "POINT(3 3)", "POINT(4 4)").map(WKTUtils.read)
  val polys = points.map(_.buffer(1))
  val geoms = (points ++ polys).toArray
  val geomFeatures = (0 until 10).map { i =>
    val name = s"name${i % 2}"
    val team = s"team$i"
    val age = i % 5
    val weight = Option(i % 3).filter(_ != 0).map(Int.box).orNull
    ScalaSimpleFeature.create(geomSft, s"$i", name, team, age, weight, s"2017-02-03T00:0$i:01.000Z", geoms(i))
  }

  val ipcOpts = new IpcOption() // TODO test legacy opts

  // note: we clone the features before comparing them as they aren't valid once 'next' is called again,
  // and specs doesn't have methods for comparing iterators

  "SimpleFeatureArrowFiles" should {
    "write and read just a schema" >> {
      withTestFile("empty") { file =>
        WithClose(SimpleFeatureArrowFileWriter(new FileOutputStream(file), sft, Map.empty, SimpleFeatureEncoding.Max, ipcOpts, None))(_.flush())
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          reader.sft mustEqual sft
          reader.features().toSeq must beEmpty
        }
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file))) { reader =>
          reader.sft mustEqual sft
          reader.features().toSeq must beEmpty
        }
      }
    }
    "write and read and filter values" >> {
      withTestFile("simple") { file =>
        WithClose(SimpleFeatureArrowFileWriter(new FileOutputStream(file), sft, Map.empty, SimpleFeatureEncoding.Max, ipcOpts, None)) { writer =>
          features0.foreach(writer.add)
          writer.flush()
          features1.foreach(writer.add)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features(ECQL.toFilter("foo = 'foo1'"))) { f =>
            f.map(ScalaSimpleFeature.copy).toSeq mustEqual
                Seq(features0(1), features0(3), features0(5), features0(7), features0(9), features1(0), features1(3), features1(6), features1(9))
          }
        }
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1)
          WithClose(reader.features(ECQL.toFilter("foo = 'foo1'")))(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual
              Seq(features0(1), features0(3), features0(5), features0(7), features0(9), features1(0), features1(3), features1(6), features1(9)))
        }
      }
    }
    "optimize queries for sorted files" >> {
      withTestFile("sorted") { file =>
        WithClose(SimpleFeatureArrowFileWriter(new FileOutputStream(file), sft, Map.empty, SimpleFeatureEncoding.Max, ipcOpts, Some(("dtg", false)))) { writer =>
          features0.foreach(writer.add)
          writer.flush()
          features1.foreach(writer.add)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features(ECQL.toFilter("dtg > '2017-03-15T00:12:01.000Z'"))) { f =>
            f.map(ScalaSimpleFeature.copy).toSeq mustEqual features1.drop(3)
          }
        }
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file))) { reader =>
          WithClose(reader.features(ECQL.toFilter("dtg > '2017-03-15T00:12:01.000Z'"))) { f =>
            f.map(ScalaSimpleFeature.copy).toSeq mustEqual features1.drop(3)
          }
          // test second invocation
          WithClose(reader.features(ECQL.toFilter("dtg > '2017-03-15T00:12:01.000Z'"))) { f =>
            f.map(ScalaSimpleFeature.copy).toSeq mustEqual features1.drop(3)
          }
        }
      }
    }
    "write and read multiple logical files in one" >> {
      withTestFile("multi-files") { file =>
        WithClose(SimpleFeatureArrowFileWriter(new FileOutputStream(file), sft, Map.empty, SimpleFeatureEncoding.Max, ipcOpts, None)) { writer =>
          features0.foreach(writer.add)
        }
        WithClose(SimpleFeatureArrowFileWriter(new FileOutputStream(file, true), sft, Map.empty, SimpleFeatureEncoding.Max, ipcOpts, None)) { writer =>
          features1.foreach(writer.add)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features(ECQL.toFilter("foo = 'foo1'"))) { f =>
            f.map(ScalaSimpleFeature.copy).toSeq mustEqual
                Seq(features0(1), features0(3), features0(5), features0(7), features0(9), features1(0), features1(3), features1(6), features1(9))
          }
        }
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1)
          WithClose(reader.features(ECQL.toFilter("foo = 'foo1'")))(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual
              Seq(features0(1), features0(3), features0(5), features0(7), features0(9), features1(0), features1(3), features1(6), features1(9))
          )
        }
      }
    }
    "write and read dictionary encoded values" >> {
      val dictionaries = Map("foo:String" -> ArrowDictionary.create(0, Array("foo0", "foo1", "foo2")))
      withTestFile("dictionary") { file =>
        WithClose(SimpleFeatureArrowFileWriter(new FileOutputStream(file), sft, dictionaries, SimpleFeatureEncoding.Max, ipcOpts, None)) { writer =>
          features0.foreach(writer.add)
          writer.flush()
          features1.foreach(writer.add)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1)
        }
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1)
        }
      }
    }
    "write and read dictionary encoded ints" >> {
      val dictionaries = Map("age" -> ArrowDictionary.create(0, Array(0, 1, 2, 3, 4, 5).map(Int.box)))
      withTestFile("dictionary-int") { file =>
        WithClose(SimpleFeatureArrowFileWriter(new FileOutputStream(file), sft, dictionaries, SimpleFeatureEncoding.Max, ipcOpts, None)) { writer =>
          features0.foreach(writer.add)
          writer.flush()
          features1.foreach(writer.add)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1)
        }
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual features0 ++ features1)
        }
      }
    }
    "write and read dictionary encoded values with defaults" >> {
      val dictionaries = Map("foo" -> ArrowDictionary.create(0, Array("foo0", "foo1")))
      withTestFile("dictionary-defaults") { file =>
        WithClose(SimpleFeatureArrowFileWriter(new FileOutputStream(file), sft, dictionaries, SimpleFeatureEncoding.Max, ipcOpts, None)) { writer =>
          features0.foreach(writer.add)
          writer.flush()
          features1.foreach(writer.add)
        }
        val expected = features0 ++ features1.map {
          case f if f.getAttribute("foo") != "foo2" => f
          case f =>
            val attributes = f.getAttributes.toArray
            attributes.update(1, "[other]")
            ScalaSimpleFeature.create(sft, f.getID, attributes: _*)
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual expected)
        }
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file))) { reader =>
          WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toSeq mustEqual expected)
        }
      }
    }
    "write and read linestrings" >> {
      withTestFile("lines") { file =>
        val encoding = SimpleFeatureEncoding.min(includeFids = true)
        WithClose(SimpleFeatureArrowFileWriter(new FileOutputStream(file), lineSft, Map.empty, encoding, ipcOpts, None)) { writer =>
          lineFeatures.foreach(writer.add)
        }
        def testReader(reader: SimpleFeatureArrowFileReader): MatchResult[Any] = {
          val read = WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toList)
          read.map(_.getID) mustEqual lineFeatures.map(_.getID)
          forall(0 until lineSft.getAttributeCount - 1) { i =>
            read.map(_.getAttribute(i)) mustEqual lineFeatures.map(_.getAttribute(i))
          }
          forall(read.map(_.getDefaultGeometry()).zip(lineFeatures.map(_.getDefaultGeometry))) {
            case (r: LineString, f: LineString) =>
              // because of our limited precision in arrow queries, points don't exactly match up
              r.getNumPoints mustEqual f.getNumPoints
              foreach(0 until r.getNumPoints) { n =>
                r.getCoordinateN(n).x must beCloseTo(f.getCoordinateN(n).x, 0.001)
                r.getCoordinateN(n).y must beCloseTo(f.getCoordinateN(n).y, 0.001)
              }
          }
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file)))(testReader)
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file)))(testReader)
      }
    }

    "write and read geometries" >> {
      withTestFile("geometries") { file =>
        val encoding = SimpleFeatureEncoding.min(includeFids = true)
        WithClose(SimpleFeatureArrowFileWriter(new FileOutputStream(file), geomSft, Map.empty, encoding, ipcOpts, None)) {
          writer =>
            geomFeatures.foreach(writer.add)
        }
        def testReader(reader: SimpleFeatureArrowFileReader): MatchResult[Any] = {
          val read = WithClose(reader.features())(f => f.map(ScalaSimpleFeature.copy).toList)
          read.map(_.getID) mustEqual geomFeatures.map(_.getID)
          forall(0 until geomSft.getAttributeCount - 1) { i =>
            read.map(_.getAttribute(i)) mustEqual geomFeatures.map(_.getAttribute(i))
          }
          forall(read.map(_.getDefaultGeometry()).zip(geomFeatures.map(_.getDefaultGeometry))) {
            case (r: Point, f: Point) =>
              r.getX must beCloseTo(f.getX, delta=0.001)
              r.getY must beCloseTo(f.getY, delta=0.001)
            case (r: Polygon, f: Polygon) =>
              // because of our limited precision in arrow queries, points don't exactly match up
              r.getNumPoints mustEqual f.getNumPoints
              foreach(0 until r.getNumPoints) { n =>
                r.getExteriorRing.getCoordinateN(n).x must beCloseTo(r.getExteriorRing.getCoordinateN(n).x, delta=0.001)
                r.getExteriorRing.getCoordinateN(n).y must beCloseTo(r.getExteriorRing.getCoordinateN(n).y, delta=0.001)
              }
            case (a, b) => 
              ko
          }
        }
        WithClose(SimpleFeatureArrowFileReader.streaming(() => new FileInputStream(file)))(testReader)
        WithClose(SimpleFeatureArrowFileReader.caching(new FileInputStream(file)))(testReader)
      }
    }
  }

  def withTestFile[T](name: String)(fn: (File) => T): T = {
    val file = Files.createTempFile(s"gm-arrow-file-test-${fileCount.getAndIncrement()}-", "arrow").toFile
    try { fn(file) } finally {
      // note: uncomment to re-create test data for arrow datastore
      // val copy = new File(s"geomesa-arrow/geomesa-arrow-datastore/src/test/resources/data/$name.arrow")
      // org.apache.commons.io.FileUtils.copyFile(file, copy)
      if (!file.delete()) {
        file.deleteOnExit()
      }
    }
  }
}
