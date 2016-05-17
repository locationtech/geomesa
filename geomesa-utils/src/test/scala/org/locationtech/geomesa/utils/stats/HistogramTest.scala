/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.util.parsing.json.JSON

@RunWith(classOf[JUnitRunner])
class HistogramTest extends Specification with StatTestHelper {

  def newStat[T](attribute: String, observe: Boolean = true): Histogram[T] = {
    val stat = Stat(sft, s"Histogram($attribute)")
    if (observe) {
      features.foreach { stat.observe }
    }
    stat.asInstanceOf[Histogram[T]]
  }

  "Histogram stat" should {

    "work with strings" >> {
      "be empty initiallly" >> {
        val stat = newStat[String]("strAttr", observe = false)
        stat.attribute mustEqual stringIndex
        stat.histogram must beEmpty
        stat.isEmpty must beTrue
      }

      "observe correct values" >> {
        val stat = newStat[String]("strAttr")
        stat.histogram must haveSize(100)
        forall(0 until 100)(i => stat.histogram(f"abc$i%03d") mustEqual 1L)
      }

      "unobserve correct values" >> {
        val stat = newStat[String]("strAttr")
        stat.histogram must haveSize(100)
        forall(0 until 100)(i => stat.histogram(f"abc$i%03d") mustEqual 1L)
        features.take(10).foreach(stat.unobserve)
        stat.histogram must haveSize(90)
        forall(0 until 10)(i => stat.histogram(f"abc$i%03d") mustEqual 0L)
        forall(10 until 100)(i => stat.histogram(f"abc$i%03d") mustEqual 1L)
      }

      "serialize to json" >> {
        val stat = newStat[String]("strAttr")
        JSON.parseFull(stat.toJson) must beSome(stat.histogram)
      }

      "serialize empty to json" >> {
        val stat = newStat[String]("strAttr", observe = false)
        stat.toJson mustEqual "{ }"
      }

      "serialize and deserialize" >> {
        val stat = newStat[String]("strAttr")
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stat" >> {
        val stat = newStat[String]("strAttr", observe = false)
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual stat.toJson
      }

      "deserialize as immutable value" >> {
        val stat = newStat[String]("strAttr")
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed, immutable = true)
        unpacked.toJson mustEqual stat.toJson

        unpacked.clear must throwAn[Exception]
        unpacked.+=(stat) must throwAn[Exception]
        unpacked.observe(features.head) must throwAn[Exception]
        unpacked.unobserve(features.head) must throwAn[Exception]
      }

      "combine two states" >> {
        val stat = newStat[String]("strAttr")
        val stat2 = newStat[String]("strAttr", observe = false)

        features2.foreach { stat2.observe }

        stat2.histogram must haveSize(100)
        forall(100 until 200)(i => stat2.histogram(f"abc$i%03d") mustEqual 1L)

        stat += stat2

        stat.histogram must haveSize(200)
        forall(0 until 200)(i => stat.histogram(f"abc$i%03d") mustEqual 1L)
        stat2.histogram must haveSize(100)
        forall(100 until 200)(i => stat2.histogram(f"abc$i%03d") mustEqual 1L)
      }

      "clear" >> {
        val stat = newStat[String]("strAttr")
        stat.isEmpty must beFalse

        stat.clear()

        stat.histogram must beEmpty
        stat.isEmpty must beTrue
      }
    }

    "work with ints" >> {
      "be empty initiallly" >> {
        val stat = newStat[java.lang.Integer]("intAttr", observe = false)
        stat.attribute mustEqual intIndex
        stat.histogram must beEmpty
        stat.isEmpty must beTrue
      }

      "observe correct values" >> {
        val stat = newStat[java.lang.Integer]("intAttr")
        forall(0 until 100)(i => stat.histogram(i) mustEqual 1)
      }

      "serialize to json" >> {
        val stat = newStat[java.lang.Integer]("intAttr")
        val s = Stat.stringifier(classOf[java.lang.Integer])
        val expected = stat.histogram.map { case (k, v) => (s(k), v)}
        JSON.parseFull(stat.toJson) must beSome(expected)
      }

      "serialize empty to json" >> {
        val stat = newStat[java.lang.Integer]("intAttr", observe = false)
        stat.toJson mustEqual "{ }"
      }

      "serialize and deserialize" >> {
        val stat = newStat[java.lang.Integer]("intAttr")
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stat" >> {
        val stat = newStat[java.lang.Integer]("intAttr", observe = false)
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual stat.toJson
      }

      "combine two states" >> {
        val stat = newStat[java.lang.Integer]("intAttr")
        val stat2 = newStat[java.lang.Integer]("intAttr", observe = false)

        features2.foreach { stat2.observe }

        stat2.histogram must haveSize(100)
        forall(100 until 200)(i => stat2.histogram(i) mustEqual 1L)

        stat += stat2

        stat.histogram must haveSize(200)
        forall(0 until 200)(i => stat.histogram(i) mustEqual 1L)
        stat2.histogram must haveSize(100)
        forall(100 until 200)(i => stat2.histogram(i) mustEqual 1L)
      }

      "clear" >> {
        val stat = newStat[java.lang.Integer]("intAttr")
        stat.isEmpty must beFalse

        stat.clear()

        stat.histogram must beEmpty
        stat.isEmpty must beTrue
      }
    }

    "work with longs" >> {
      "be empty initiallly" >> {
        val stat = newStat[java.lang.Long]("longAttr", observe = false)
        stat.attribute mustEqual longIndex
        stat.histogram must beEmpty
        stat.isEmpty must beTrue
      }

      "observe correct values" >> {
        val stat = newStat[java.lang.Long]("longAttr")
        forall(0 until 100)(i => stat.histogram(i.toLong) mustEqual 1)
      }

      "serialize to json" >> {
        val stat = newStat[java.lang.Long]("longAttr")
        val s = Stat.stringifier(classOf[java.lang.Long])
        val expected = stat.histogram.map { case (k, v) => (s(k), v)}
        JSON.parseFull(stat.toJson) must beSome(expected)
      }

      "serialize empty to json" >> {
        val stat = newStat[java.lang.Long]("longAttr", observe = false)
        stat.toJson mustEqual "{ }"
      }

      "serialize and deserialize" >> {
        val stat = newStat[java.lang.Long]("longAttr")
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stat" >> {
        val stat = newStat[java.lang.Long]("longAttr", observe = false)
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual stat.toJson
      }

      "combine two states" >> {
        val stat = newStat[java.lang.Long]("longAttr")
        val stat2 = newStat[java.lang.Long]("longAttr", observe = false)

        features2.foreach { stat2.observe }

        stat2.histogram must haveSize(100)
        forall(100 until 200)(i => stat2.histogram(i.toLong) mustEqual 1L)

        stat += stat2

        stat.histogram must haveSize(200)
        forall(0 until 200)(i => stat.histogram(i.toLong) mustEqual 1L)
        stat2.histogram must haveSize(100)
        forall(100 until 200)(i => stat2.histogram(i.toLong) mustEqual 1L)
      }

      "clear" >> {
        val stat = newStat[java.lang.Long]("longAttr")
        stat.isEmpty must beFalse

        stat.clear()

        stat.histogram must beEmpty
        stat.isEmpty must beTrue
      }
    }

    "work with floats" >> {
      "be empty initiallly" >> {
        val stat = newStat[java.lang.Float]("floatAttr", observe = false)
        stat.attribute mustEqual floatIndex
        stat.histogram must beEmpty
        stat.isEmpty must beTrue
      }

      "observe correct values" >> {
        val stat = newStat[java.lang.Float]("floatAttr")
        forall(0 until 100)(i => stat.histogram(i.toFloat) mustEqual 1)
      }

      "serialize to json" >> {
        val stat = newStat[java.lang.Float]("floatAttr")
        val s = Stat.stringifier(classOf[java.lang.Float])
        val expected = stat.histogram.map { case (k, v) => (s(k), v)}
        JSON.parseFull(stat.toJson) must beSome(expected)
      }

      "serialize empty to json" >> {
        val stat = newStat[java.lang.Float]("floatAttr", observe = false)
        stat.toJson mustEqual "{ }"
      }

      "serialize and deserialize" >> {
        val stat = newStat[java.lang.Float]("floatAttr")
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stat" >> {
        val stat = newStat[java.lang.Float]("floatAttr", observe = false)
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual stat.toJson
      }

      "combine two states" >> {
        val stat = newStat[java.lang.Float]("floatAttr")
        val stat2 = newStat[java.lang.Float]("floatAttr", observe = false)

        features2.foreach { stat2.observe }

        stat2.histogram must haveSize(100)
        forall(100 until 200)(i => stat2.histogram(i.toFloat) mustEqual 1L)

        stat += stat2

        stat.histogram must haveSize(200)
        forall(0 until 200)(i => stat.histogram(i.toFloat) mustEqual 1L)
        stat2.histogram must haveSize(100)
        forall(100 until 200)(i => stat2.histogram(i.toFloat) mustEqual 1L)
      }

      "clear" >> {
        val stat = newStat[java.lang.Float]("floatAttr")
        stat.isEmpty must beFalse

        stat.clear()

        stat.histogram must beEmpty
        stat.isEmpty must beTrue
      }
    }

    "work with doubles" >> {
      "be empty initiallly" >> {
        val stat = newStat[java.lang.Double]("doubleAttr", observe = false)
        stat.attribute mustEqual doubleIndex
        stat.histogram must beEmpty
        stat.isEmpty must beTrue
      }

      "observe correct values" >> {
        val stat = newStat[java.lang.Double]("doubleAttr")
        forall(0 until 100)(i => stat.histogram(i.toDouble) mustEqual 1)
      }

      "serialize to json" >> {
        val stat = newStat[java.lang.Double]("doubleAttr")
        val s = Stat.stringifier(classOf[java.lang.Double])
        val expected = stat.histogram.map { case (k, v) => (s(k), v)}
        JSON.parseFull(stat.toJson) must beSome(expected)
      }

      "serialize empty to json" >> {
        val stat = newStat[java.lang.Double]("doubleAttr", observe = false)
        stat.toJson mustEqual "{ }"
      }

      "serialize and deserialize" >> {
        val stat = newStat[java.lang.Double]("doubleAttr")
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stat" >> {
        val stat = newStat[java.lang.Double]("doubleAttr", observe = false)
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual stat.toJson
      }

      "combine two states" >> {
        val stat = newStat[java.lang.Double]("doubleAttr")
        val stat2 = newStat[java.lang.Double]("doubleAttr", observe = false)

        features2.foreach { stat2.observe }

        stat2.histogram must haveSize(100)
        forall(100 until 200)(i => stat2.histogram(i.toDouble) mustEqual 1L)

        stat += stat2

        stat.histogram must haveSize(200)
        forall(0 until 200)(i => stat.histogram(i.toDouble) mustEqual 1L)
        stat2.histogram must haveSize(100)
        forall(100 until 200)(i => stat2.histogram(i.toDouble) mustEqual 1L)
      }

      "clear" >> {
        val stat = newStat[java.lang.Double]("doubleAttr")
        stat.isEmpty must beFalse

        stat.clear()

        stat.histogram must beEmpty
        stat.isEmpty must beTrue
      }
    }

    "work with dates" >> {
      "be empty initiallly" >> {
        val stat = newStat[Date]("dtg", observe = false)
        stat.attribute mustEqual dateIndex
        stat.histogram must beEmpty
        stat.isEmpty must beTrue
      }

      "observe correct values" >> {
        val stat = newStat[Date]("dtg")
        val dates = (0 until 24).map(i => GeoToolsDateFormat.parseDateTime(f"2012-01-01T$i%02d:00:00.000Z").toDate)
        forall(dates.take(4))(d => stat.histogram(d) mustEqual 5)
        forall(dates.drop(4))(d => stat.histogram(d) mustEqual 4)
      }

      "serialize to json" >> {
        val stat = newStat[Date]("dtg")
        val s = Stat.stringifier(classOf[Date])
        val expected = stat.histogram.map { case (k, v) => (s(k), v)}
        JSON.parseFull(stat.toJson) must beSome(expected)
      }

      "serialize empty to json" >> {
        val stat = newStat[Date]("dtg", observe = false)
        stat.toJson mustEqual "{ }"
      }

      "serialize and deserialize" >> {
        val stat = newStat[Date]("dtg")
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stat" >> {
        val stat = newStat[Date]("dtg", observe = false)
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual stat.toJson
      }

      "combine two states" >> {
        val stat = newStat[Date]("dtg")
        val stat2 = newStat[Date]("dtg", observe = false)

        features2.foreach { stat2.observe }

        val dates = (0 until 24).map(i => GeoToolsDateFormat.parseDateTime(f"2012-01-01T$i%02d:00:00.000Z").toDate)
        val dates2 = (0 until 24).map(i => GeoToolsDateFormat.parseDateTime(f"2012-01-02T$i%02d:00:00.000Z").toDate)

        stat2.histogram must haveSize(24)
        forall(dates2.slice(4, 8))(d => stat2.histogram(d) mustEqual 5)
        forall(dates2.take(4) ++ dates2.drop(8))(d => stat2.histogram(d) mustEqual 4)

        stat += stat2

        stat.histogram must haveSize(48)
        forall(dates.take(4) ++ dates2.slice(4, 8))(d => stat.histogram(d) mustEqual 5)
        forall(dates.drop(4) ++ dates2.take(4) ++ dates2.drop(8))(d => stat.histogram(d) mustEqual 4)
        stat2.histogram must haveSize(24)
        forall(dates2.slice(4, 8))(d => stat2.histogram(d) mustEqual 5)
        forall(dates2.take(4) ++ dates2.drop(8))(d => stat2.histogram(d) mustEqual 4)
      }

      "clear" >> {
        val stat = newStat[Date]("dtg")
        stat.isEmpty must beFalse

        stat.clear()

        stat.histogram must beEmpty
        stat.isEmpty must beTrue
      }
    }

    "work with geometries" >> {
      "be empty initiallly" >> {
        val stat = newStat[Geometry]("geom", observe = false)
        stat.attribute mustEqual geomIndex
        stat.histogram must beEmpty
        stat.isEmpty must beTrue
      }

      "observe correct values" >> {
        val stat = newStat[Geometry]("geom")
        forall(0 until 100)(i => stat.histogram(WKTUtils.read(s"POINT(-$i ${i / 2})")) mustEqual 1)
      }

      "serialize to json" >> {
        val stat = newStat[Geometry]("geom")
        val s = Stat.stringifier(classOf[Geometry])
        val expected = stat.histogram.map { case (k, v) => (s(k), v)}
        JSON.parseFull(stat.toJson) must beSome(expected)
      }

      "serialize empty to json" >> {
        val stat = newStat[Geometry]("geom", observe = false)
        stat.toJson mustEqual "{ }"
      }

      "serialize and deserialize" >> {
        val stat = newStat[Geometry]("geom")
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stat" >> {
        val stat = newStat[Geometry]("geom", observe = false)
        val packed = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual stat.toJson
      }

      "combine two states" >> {
        val stat = newStat[Geometry]("geom")
        val stat2 = newStat[Geometry]("geom", observe = false)

        features2.foreach { stat2.observe }

        stat2.histogram must haveSize(100)
        forall(100 until 200)(i => stat2.histogram(WKTUtils.read(s"POINT(${i -20} ${i / 2 - 20})")) mustEqual 1)

        stat += stat2

        stat.histogram must haveSize(200)
        forall(0 until 100)(i => stat.histogram(WKTUtils.read(s"POINT(-$i ${i / 2})")) mustEqual 1)
        forall(100 until 200)(i => stat.histogram(WKTUtils.read(s"POINT(${i -20} ${i / 2 - 20})")) mustEqual 1)

        stat2.histogram must haveSize(100)
        forall(100 until 200)(i => stat2.histogram(WKTUtils.read(s"POINT(${i -20} ${i / 2 - 20})")) mustEqual 1)
      }

      "clear" >> {
        val stat = newStat[Geometry]("geom")
        stat.isEmpty must beFalse

        stat.clear()

        stat.histogram must beEmpty
        stat.isEmpty must beTrue
      }
    }
  }
}
