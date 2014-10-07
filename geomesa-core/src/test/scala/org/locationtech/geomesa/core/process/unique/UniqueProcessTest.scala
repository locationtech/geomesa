/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.process.unique

import java.text.SimpleDateFormat
import java.util.TimeZone

import org.geotools.factory.{CommonFactoryFinder, Hints}
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.geotools.filter.text.cql2.CQL
import org.geotools.util.NullProgressListener
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.TestWithDataStore
import org.locationtech.geomesa.core.util.SelfClosingIterator
import org.locationtech.geomesa.feature.AvroSimpleFeatureFactory
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.filter.Filter
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConverters._

@RunWith(classOf[JUnitRunner])
class UniqueProcessTest extends Specification with TestWithDataStore {

  override val spec = "name:String:index=true,weight:Double:index=true,dtg:Date,*geom:Geometry:srid=4326"
  override val dtgField = "dtg"

  override def getTestFeatures() = {
    val geom = WKTUtils.read("POINT(45.0 49.0)")

    val builder = new SimpleFeatureBuilder(sft, new AvroSimpleFeatureFactory)
    val dtFormat = new SimpleDateFormat("yyyyMMdd HH:mm:SS")
    dtFormat.setTimeZone(TimeZone.getTimeZone("UTC"))

    Seq(
      Seq("alice",    20,   dtFormat.parse("20120101 12:00:00"), geom),
      Seq("alice",    25,   dtFormat.parse("20120101 12:00:00"), geom),
      Seq("bill",     21,   dtFormat.parse("20130101 12:00:00"), geom),
      Seq("bill",     22,   dtFormat.parse("20130101 12:00:00"), geom),
      Seq("bill",     23,   dtFormat.parse("20130101 12:00:00"), geom),
      Seq("bob",      30,   dtFormat.parse("20140101 12:00:00"), geom),
      Seq("charles",  40,   dtFormat.parse("20140101 12:30:00"), geom),
      Seq("charles",  null, dtFormat.parse("20140101 12:30:00"), geom)
    ).map { case name :: weight :: dtg :: geom :: Nil =>
      val feature = builder.buildFeature(null)
      feature.setDefaultGeometry(geom)
      feature.setAttribute("name", name)
      feature.setAttribute("weight", weight)
      feature.setAttribute("dtg", dtg)
      feature.setAttribute("geom", geom)
      // make sure we ask the system to re-use the provided feature-ID
      feature.getUserData().asScala(Hints.USE_PROVIDED_FID) = java.lang.Boolean.TRUE
      feature
    }
  }

  populateFeatures

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
      val names = uniques.map(_.getAttribute("value")).toList
      names should contain(exactly[Any]("alice", "bill", "bob", "charles"))

      val counts = uniques.map(_.getAttribute("count")).toList
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
      val names = uniques.map(_.getAttribute("value")).toList
      names(0) mustEqual("charles")
      names(1) mustEqual("bob")
      names(2) mustEqual("bill")
      names(3) mustEqual("alice")

      val counts = uniques.map(_.getAttribute("count")).toList
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
      val names = uniques.map(_.getAttribute("value")).toList
      names(0) mustEqual("bill")
      names(1) mustEqual("alice")
      names(2) mustEqual("charles")
      names(3) mustEqual("bob")

      val counts = uniques.map(_.getAttribute("count")).toList
      counts should contain(exactly[Any](1L, 2L, 2L, 3L))

      val alice = uniques.find(_.getAttribute("value") == "alice").map(_.getAttribute("count"))
      alice must beSome(2)

      val bill = uniques.find(_.getAttribute("value") == "bill").map(_.getAttribute("count"))
      bill must beSome(3)

      val charles = uniques.find(_.getAttribute("value") == "charles").map(_.getAttribute("count"))
      charles must beSome(2)
    }
  }

  "AttributeVisitor" should {

    "combine filters appropriately" >> {

      val ff  = CommonFactoryFinder.getFilterFactory2
      val f1 = ff.equals(ff.property("test"), ff.literal("a"))

      "ignore Filter.INCLUDE" >> {
        val f2 = Filter.INCLUDE
        val combined = AttributeVisitor.combineFilters(f1, f2)
        combined mustEqual(f1)

        val combined2 = AttributeVisitor.combineFilters(f2, f1)
        combined2 mustEqual(f1)
      }

      "simplify same filters" >> {
        val f2 = ff.equals(ff.property("test"), ff.literal("a"))
        val combined = AttributeVisitor.combineFilters(f1, f2)
        combined mustEqual(f1)

        val combined2 = AttributeVisitor.combineFilters(f2, f1)
        combined2 mustEqual(f1)
      }

      "AND different filters" >> {
        val f2 = ff.equals(ff.property("test2"), ff.literal("a"))
        val desired = ff.and(f1, f2)
        val combined = AttributeVisitor.combineFilters(f1, f2)
        combined mustEqual(desired)

        val combined2 = AttributeVisitor.combineFilters(f2, f1)
        combined2 mustEqual(desired)
      }
    }
  }
}
