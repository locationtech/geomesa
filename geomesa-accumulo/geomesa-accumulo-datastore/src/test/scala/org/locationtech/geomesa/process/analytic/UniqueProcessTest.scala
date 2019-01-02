/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import java.text.SimpleDateFormat
import java.util.TimeZone

import org.geotools.factory.CommonFactoryFinder
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.cql2.CQL
import org.geotools.util.NullProgressListener
import org.junit.runner.RunWith
import org.locationtech.geomesa.accumulo.TestWithDataStore
import org.locationtech.geomesa.features.avro.AvroSimpleFeatureFactory
import org.locationtech.geomesa.process.analytic.{AttributeVisitor, UniqueProcess}
import org.locationtech.geomesa.utils.collection.SelfClosingIterator
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class UniqueProcessTest extends Specification with TestWithDataStore {

  sequential

  override val spec = "name:String:index=join,weight:Double:index=join,ml:List[String],dtg:Date,*geom:Point:srid=4326"

  import java.{util => jl}
  def toJavaList(s: Seq[String]): java.util.List[String] = s.asJava

  addFeatures({
    val geom = WKTUtils.read("POINT(45.0 49.0)")

    val builder = new SimpleFeatureBuilder(sft, new AvroSimpleFeatureFactory)
    val dtFormat = new SimpleDateFormat("yyyyMMdd HH:mm:SS")
    dtFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

    Seq(
      Seq("alice",    20,   toJavaList(Seq()),                          dtFormat.parse("20120101 12:00:00"), geom),
      Seq("alice",    25,   null.asInstanceOf[jl.List[String]],         dtFormat.parse("20120101 12:00:00"), geom),
      Seq("bill",     21,   toJavaList(Seq("foo", "bar")),              dtFormat.parse("20130101 12:00:00"), geom),
      Seq("bill",     22,   toJavaList(Seq("foo")),                     dtFormat.parse("20130101 12:00:00"), geom),
      Seq("bill",     23,   toJavaList(Seq("foo")),                     dtFormat.parse("20130101 12:00:00"), geom),
      Seq("bob",      30,   toJavaList(Seq("foo")),                     dtFormat.parse("20140101 12:00:00"), geom),
      Seq("charles",  40,   toJavaList(Seq("foo")),                     dtFormat.parse("20140101 12:30:00"), geom),
      Seq("charles",  null, toJavaList(Seq("foo")),                     dtFormat.parse("20140101 12:30:00"), geom)
    ).map { case name :: weight :: l :: dtg :: geom :: Nil =>
      val feature = builder.buildFeature(s"$name$weight")
      feature.setDefaultGeometry(geom)
      feature.setAttribute("name", name)
      feature.setAttribute("weight", weight)
      feature.setAttribute("ml", l)
      feature.setAttribute("dtg", dtg)
      feature.setAttribute("geom", geom)
      feature
    }
  })

  val pl = new NullProgressListener()

  "UniqueProcess" should {

    "return things without a filter" in {
      val features = fs.getFeatures()

      val process = new UniqueProcess
      val results = process.execute(features, "name", null, null, null, null, pl)

      val names = SelfClosingIterator(results.features()).map(_.getAttribute("value")).toList
      names must contain(exactly[Any]("alice", "bill", "bob", "charles"))
    }

    "respect a parent filter" in {
      val features = fs.getFeatures(CQL.toFilter("name LIKE 'b%'"))

      val process = new UniqueProcess
      val results = process.execute(features, "name", null, null, null, null, pl)

      val names = SelfClosingIterator(results.features()).map(_.getAttribute("value")).toList
      names must contain(exactly[Any]("bill", "bob"))
    }

    "be able to use its own filter" in {
      val features = fs.getFeatures()

      val process = new UniqueProcess
      val results = process.execute(features, "name", CQL.toFilter("name LIKE 'b%'"), null, null, null, pl)

      val names = SelfClosingIterator(results.features()).map(_.getAttribute("value")).toList
      names must contain(exactly[Any]("bill", "bob"))
    }

    "combine parent and own filter" in {
      val features = fs.getFeatures(CQL.toFilter("name LIKE 'b%'"))

      val process = new UniqueProcess
      val results = process.execute(features, "name", CQL.toFilter("weight > 25"), null, null, null, pl)

      val names = SelfClosingIterator(results.features()).map(_.getAttribute("value")).toList
      names must contain(exactly[Any]("bob"))
    }

    "default to no histogram" in {
      val features = fs.getFeatures()

      val process = new UniqueProcess
      val results = process.execute(features, "name", null, null, null, null, pl)

      val uniques = SelfClosingIterator(results.features()).toList
      val names = uniques.map(_.getAttribute("value"))
      names must contain(exactly[Any]("alice", "bill", "bob", "charles"))

      val counts = uniques.flatMap(f => Option(f.getAttribute("count")))
      counts must beEmpty
    }

    "include histogram if requested" in {
      val features = fs.getFeatures()

      val process = new UniqueProcess
      val results = process.execute(features, "name", null, true, null, null, pl)

      val uniques = SelfClosingIterator(results.features()).toList
      val names = uniques.map(_.getAttribute("value"))
      names should contain(exactly[Any]("alice", "bill", "bob", "charles"))

      val counts = uniques.map(_.getAttribute("count"))
      counts should contain(exactly[Any](1L, 2L, 2L, 3L))

      val alice = uniques.find(_.getAttribute("value") == "alice").map(_.getAttribute("count"))
      alice must beSome(2)

      val bill = uniques.find(_.getAttribute("value") == "bill").map(_.getAttribute("count"))
      bill must beSome(3)

      val charles = uniques.find(_.getAttribute("value") == "charles").map(_.getAttribute("count"))
      charles must beSome(2)
    }

    "sort by value" in {
      val features = fs.getFeatures()

      val process = new UniqueProcess
      val results = process.execute(features, "name", null, true, "DESC", null, pl)

      val uniques = SelfClosingIterator(results.features()).toList
      val names = uniques.map(_.getAttribute("value"))
      names must haveLength(4)
      names(0) mustEqual("charles")
      names(1) mustEqual("bob")
      names(2) mustEqual("bill")
      names(3) mustEqual("alice")

      val counts = uniques.map(_.getAttribute("count"))
      counts should contain(exactly[Any](1L, 2L, 2L, 3L))

      val alice = uniques.find(_.getAttribute("value") == "alice").map(_.getAttribute("count"))
      alice must beSome(2)

      val bill = uniques.find(_.getAttribute("value") == "bill").map(_.getAttribute("count"))
      bill must beSome(3)

      val charles = uniques.find(_.getAttribute("value") == "charles").map(_.getAttribute("count"))
      charles must beSome(2)
    }

    "sort by histogram" in {
      val features = fs.getFeatures()

      val process = new UniqueProcess
      val results = process.execute(features, "name", null, true, "DESC", true, pl)

      val uniques = SelfClosingIterator(results.features()).toList
      val names = uniques.map(_.getAttribute("value"))
      names must haveLength(4)
      names(0) mustEqual("bill")
      names(1) mustEqual("alice")
      names(2) mustEqual("charles")
      names(3) mustEqual("bob")

      val counts = uniques.map(_.getAttribute("count"))
      counts should contain(exactly[Any](1L, 2L, 2L, 3L))

      val alice = uniques.find(_.getAttribute("value") == "alice").map(_.getAttribute("count"))
      alice must beSome(2)

      val bill = uniques.find(_.getAttribute("value") == "bill").map(_.getAttribute("count"))
      bill must beSome(3)

      val charles = uniques.find(_.getAttribute("value") == "charles").map(_.getAttribute("count"))
      charles must beSome(2)
    }

    "deal with multi-valued properties correctly" >> {
      val features = fs.getFeatures()
      val proc = new UniqueProcess
      val results = proc.execute(features, "ml", null, true, "DESC", false, pl)
      val uniques = SelfClosingIterator(results.features()).toList
      val values = uniques.map(_.getAttribute("value"))
      "contain 'foo' and 'bar'" >> { values must containTheSameElementsAs(Seq("foo", "bar")) }
      "'foo' must have count 6" >> { uniques.find(_.getAttribute("value") == "foo").map(_.getAttribute("count")) must beSome(6) }
      "'bar' must have count 1" >> { uniques.find(_.getAttribute("value") == "bar").map(_.getAttribute("count")) must beSome(1) }
    }
  }
}
