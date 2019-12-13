/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import org.locationtech.jts.geom.Geometry
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class EnumerationStatTest extends Specification with StatTestHelper {

  def newStat[T](attribute: String, observe: Boolean = true): EnumerationStat[T] = {
    val stat = Stat(sft, s"Enumeration($attribute)")
    if (observe) {
      features.foreach { stat.observe }
    }
    stat.asInstanceOf[EnumerationStat[T]]
  }

  "Histogram stat" should {

    "work with strings" >> {
      "be empty initiallly" >> {
        val stat = newStat[String]("strAttr", observe = false)
        stat.property mustEqual "strAttr"
        stat.enumeration must beEmpty
        stat.isEmpty must beTrue
      }

      "observe correct values" >> {
        val stat = newStat[String]("strAttr")
        stat.enumeration must haveSize(100)
        forall(0 until 100)(i => stat.enumeration(f"abc$i%03d") mustEqual 1L)
      }

      "unobserve correct values" >> {
        val stat = newStat[String]("strAttr")
        stat.enumeration must haveSize(100)
        forall(0 until 100)(i => stat.enumeration(f"abc$i%03d") mustEqual 1L)
        features.take(10).foreach(stat.unobserve)
        stat.enumeration must haveSize(90)
        forall(0 until 10)(i => stat.enumeration(f"abc$i%03d") mustEqual 0L)
        forall(10 until 100)(i => stat.enumeration(f"abc$i%03d") mustEqual 1L)
      }

      "serialize to json" >> {
        val stat = newStat[String]("strAttr")
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[EnumerationStat[String]]
        unpacked.asInstanceOf[EnumerationStat[String]].property mustEqual stat.property
        unpacked.asInstanceOf[EnumerationStat[String]].enumeration mustEqual stat.enumeration
        unpacked.asInstanceOf[EnumerationStat[String]].size mustEqual stat.size
        unpacked.asInstanceOf[EnumerationStat[String]].toJson mustEqual stat.toJson
      }

      "serialize empty to json" >> {
        val stat = newStat[String]("strAttr", observe = false)
        stat.toJson must beEqualTo("{ }").ignoreSpace
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

        stat2.enumeration must haveSize(100)
        forall(100 until 200)(i => stat2.enumeration(f"abc$i%03d") mustEqual 1L)

        stat += stat2

        stat.enumeration must haveSize(200)
        forall(0 until 200)(i => stat.enumeration(f"abc$i%03d") mustEqual 1L)
        stat2.enumeration must haveSize(100)
        forall(100 until 200)(i => stat2.enumeration(f"abc$i%03d") mustEqual 1L)
      }

      "clear" >> {
        val stat = newStat[String]("strAttr")
        stat.isEmpty must beFalse

        stat.clear()

        stat.enumeration must beEmpty
        stat.isEmpty must beTrue
      }
    }

    "work with ints" >> {
      "be empty initiallly" >> {
        val stat = newStat[java.lang.Integer]("intAttr", observe = false)
        stat.property mustEqual "intAttr"
        stat.enumeration must beEmpty
        stat.isEmpty must beTrue
      }

      "observe correct values" >> {
        val stat = newStat[java.lang.Integer]("intAttr")
        forall(0 until 100)(i => stat.enumeration(i) mustEqual 1)
      }

      "serialize to json" >> {
        val stat = newStat[java.lang.Integer]("intAttr")
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[EnumerationStat[java.lang.Integer]]
        unpacked.asInstanceOf[EnumerationStat[java.lang.Integer]].property mustEqual stat.property
        unpacked.asInstanceOf[EnumerationStat[java.lang.Integer]].enumeration mustEqual stat.enumeration
        unpacked.asInstanceOf[EnumerationStat[java.lang.Integer]].size mustEqual stat.size
        unpacked.asInstanceOf[EnumerationStat[java.lang.Integer]].toJson mustEqual stat.toJson
      }

      "serialize empty to json" >> {
        val stat = newStat[java.lang.Integer]("intAttr", observe = false)
        stat.toJson must beEqualTo("{ }").ignoreSpace
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

      "combine two stats" >> {
        val stat = newStat[java.lang.Integer]("intAttr")
        val stat2 = newStat[java.lang.Integer]("intAttr", observe = false)

        features2.foreach { stat2.observe }

        stat2.enumeration must haveSize(100)
        forall(100 until 200)(i => stat2.enumeration(i) mustEqual 1L)

        stat += stat2

        stat.enumeration must haveSize(200)
        forall(0 until 200)(i => stat.enumeration(i) mustEqual 1L)
        stat2.enumeration must haveSize(100)
        forall(100 until 200)(i => stat2.enumeration(i) mustEqual 1L)
      }

      "clear" >> {
        val stat = newStat[java.lang.Integer]("intAttr")
        stat.isEmpty must beFalse

        stat.clear()

        stat.enumeration must beEmpty
        stat.isEmpty must beTrue
      }
    }

    "work with longs" >> {
      "be empty initiallly" >> {
        val stat = newStat[java.lang.Long]("longAttr", observe = false)
        stat.property mustEqual "longAttr"
        stat.enumeration must beEmpty
        stat.isEmpty must beTrue
      }

      "observe correct values" >> {
        val stat = newStat[java.lang.Long]("longAttr")
        forall(0 until 100)(i => stat.enumeration(i.toLong) mustEqual 1)
      }

      "serialize to json" >> {
        val stat = newStat[java.lang.Long]("longAttr")
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[EnumerationStat[java.lang.Long]]
        unpacked.asInstanceOf[EnumerationStat[java.lang.Long]].property mustEqual stat.property
        unpacked.asInstanceOf[EnumerationStat[java.lang.Long]].enumeration mustEqual stat.enumeration
        unpacked.asInstanceOf[EnumerationStat[java.lang.Long]].size mustEqual stat.size
        unpacked.asInstanceOf[EnumerationStat[java.lang.Long]].toJson mustEqual stat.toJson
      }

      "serialize empty to json" >> {
        val stat = newStat[java.lang.Long]("longAttr", observe = false)
        stat.toJson must beEqualTo("{ }").ignoreSpace
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

        stat2.enumeration must haveSize(100)
        forall(100 until 200)(i => stat2.enumeration(i.toLong) mustEqual 1L)

        stat += stat2

        stat.enumeration must haveSize(200)
        forall(0 until 200)(i => stat.enumeration(i.toLong) mustEqual 1L)
        stat2.enumeration must haveSize(100)
        forall(100 until 200)(i => stat2.enumeration(i.toLong) mustEqual 1L)
      }

      "clear" >> {
        val stat = newStat[java.lang.Long]("longAttr")
        stat.isEmpty must beFalse

        stat.clear()

        stat.enumeration must beEmpty
        stat.isEmpty must beTrue
      }
    }

    "work with floats" >> {
      "be empty initiallly" >> {
        val stat = newStat[java.lang.Float]("floatAttr", observe = false)
        stat.property mustEqual "floatAttr"
        stat.enumeration must beEmpty
        stat.isEmpty must beTrue
      }

      "observe correct values" >> {
        val stat = newStat[java.lang.Float]("floatAttr")
        forall(0 until 100)(i => stat.enumeration(i.toFloat) mustEqual 1)
      }

      "serialize to json" >> {
        val stat = newStat[java.lang.Float]("floatAttr")
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[EnumerationStat[java.lang.Float]]
        unpacked.asInstanceOf[EnumerationStat[java.lang.Float]].property mustEqual stat.property
        unpacked.asInstanceOf[EnumerationStat[java.lang.Float]].enumeration mustEqual stat.enumeration
        unpacked.asInstanceOf[EnumerationStat[java.lang.Float]].size mustEqual stat.size
        unpacked.asInstanceOf[EnumerationStat[java.lang.Float]].toJson mustEqual stat.toJson
      }

      "serialize empty to json" >> {
        val stat = newStat[java.lang.Float]("floatAttr", observe = false)
        stat.toJson must beEqualTo("{ }").ignoreSpace
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

        stat2.enumeration must haveSize(100)
        forall(100 until 200)(i => stat2.enumeration(i.toFloat) mustEqual 1L)

        stat += stat2

        stat.enumeration must haveSize(200)
        forall(0 until 200)(i => stat.enumeration(i.toFloat) mustEqual 1L)
        stat2.enumeration must haveSize(100)
        forall(100 until 200)(i => stat2.enumeration(i.toFloat) mustEqual 1L)
      }

      "clear" >> {
        val stat = newStat[java.lang.Float]("floatAttr")
        stat.isEmpty must beFalse

        stat.clear()

        stat.enumeration must beEmpty
        stat.isEmpty must beTrue
      }
    }

    "work with doubles" >> {
      "be empty initiallly" >> {
        val stat = newStat[java.lang.Double]("doubleAttr", observe = false)
        stat.property mustEqual "doubleAttr"
        stat.enumeration must beEmpty
        stat.isEmpty must beTrue
      }

      "observe correct values" >> {
        val stat = newStat[java.lang.Double]("doubleAttr")
        forall(0 until 100)(i => stat.enumeration(i.toDouble) mustEqual 1)
      }

      "serialize to json" >> {
        val stat = newStat[java.lang.Double]("doubleAttr")
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[EnumerationStat[java.lang.Double]]
        unpacked.asInstanceOf[EnumerationStat[java.lang.Double]].property mustEqual stat.property
        unpacked.asInstanceOf[EnumerationStat[java.lang.Double]].enumeration mustEqual stat.enumeration
        unpacked.asInstanceOf[EnumerationStat[java.lang.Double]].size mustEqual stat.size
        unpacked.asInstanceOf[EnumerationStat[java.lang.Double]].toJson mustEqual stat.toJson
      }

      "serialize empty to json" >> {
        val stat = newStat[java.lang.Double]("doubleAttr", observe = false)
        stat.toJson must beEqualTo("{ }").ignoreSpace
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

        stat2.enumeration must haveSize(100)
        forall(100 until 200)(i => stat2.enumeration(i.toDouble) mustEqual 1L)

        stat += stat2

        stat.enumeration must haveSize(200)
        forall(0 until 200)(i => stat.enumeration(i.toDouble) mustEqual 1L)
        stat2.enumeration must haveSize(100)
        forall(100 until 200)(i => stat2.enumeration(i.toDouble) mustEqual 1L)
      }

      "clear" >> {
        val stat = newStat[java.lang.Double]("doubleAttr")
        stat.isEmpty must beFalse

        stat.clear()

        stat.enumeration must beEmpty
        stat.isEmpty must beTrue
      }
    }

    "work with dates" >> {
      "be empty initiallly" >> {
        val stat = newStat[Date]("dtg", observe = false)
        stat.property mustEqual "dtg"
        stat.enumeration must beEmpty
        stat.isEmpty must beTrue
      }

      "observe correct values" >> {
        val stat = newStat[Date]("dtg")
        val dates = (0 until 24).map(i => java.util.Date.from(java.time.LocalDateTime.parse(f"2012-01-01T$i%02d:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC)))
        forall(dates.take(4))(d => stat.enumeration(d) mustEqual 5)
        forall(dates.drop(4))(d => stat.enumeration(d) mustEqual 4)
      }

      "serialize to json" >> {
        val stat = newStat[Date]("dtg")
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[EnumerationStat[Date]]
        unpacked.asInstanceOf[EnumerationStat[Date]].property mustEqual stat.property
        unpacked.asInstanceOf[EnumerationStat[Date]].enumeration mustEqual stat.enumeration
        unpacked.asInstanceOf[EnumerationStat[Date]].size mustEqual stat.size
        unpacked.asInstanceOf[EnumerationStat[Date]].toJson mustEqual stat.toJson
      }

      "serialize empty to json" >> {
        val stat = newStat[Date]("dtg", observe = false)
        stat.toJson must beEqualTo("{ }").ignoreSpace
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

        val dates = (0 until 24).map(i => java.util.Date.from(java.time.LocalDateTime.parse(f"2012-01-01T$i%02d:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC)))
        val dates2 = (0 until 24).map(i => java.util.Date.from(java.time.LocalDateTime.parse(f"2012-01-02T$i%02d:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC)))

        stat2.enumeration must haveSize(24)
        forall(dates2.slice(4, 8))(d => stat2.enumeration(d) mustEqual 5)
        forall(dates2.take(4) ++ dates2.drop(8))(d => stat2.enumeration(d) mustEqual 4)

        stat += stat2

        stat.enumeration must haveSize(48)
        forall(dates.take(4) ++ dates2.slice(4, 8))(d => stat.enumeration(d) mustEqual 5)
        forall(dates.drop(4) ++ dates2.take(4) ++ dates2.drop(8))(d => stat.enumeration(d) mustEqual 4)
        stat2.enumeration must haveSize(24)
        forall(dates2.slice(4, 8))(d => stat2.enumeration(d) mustEqual 5)
        forall(dates2.take(4) ++ dates2.drop(8))(d => stat2.enumeration(d) mustEqual 4)
      }

      "clear" >> {
        val stat = newStat[Date]("dtg")
        stat.isEmpty must beFalse

        stat.clear()

        stat.enumeration must beEmpty
        stat.isEmpty must beTrue
      }
    }

    "work with geometries" >> {
      "be empty initiallly" >> {
        val stat = newStat[Geometry]("geom", observe = false)
        stat.property mustEqual "geom"
        stat.enumeration must beEmpty
        stat.isEmpty must beTrue
      }

      "observe correct values" >> {
        val stat = newStat[Geometry]("geom")
        forall(0 until 100)(i => stat.enumeration(WKTUtils.read(s"POINT(-$i ${i / 2})")) mustEqual 1)
      }

      "serialize to json" >> {
        val stat = newStat[Geometry]("geom")
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[EnumerationStat[Geometry]]
        unpacked.asInstanceOf[EnumerationStat[Geometry]].property mustEqual stat.property
        unpacked.asInstanceOf[EnumerationStat[Geometry]].enumeration mustEqual stat.enumeration
        unpacked.asInstanceOf[EnumerationStat[Geometry]].size mustEqual stat.size
        unpacked.asInstanceOf[EnumerationStat[Geometry]].toJson mustEqual stat.toJson
      }

      "serialize empty to json" >> {
        val stat = newStat[Geometry]("geom", observe = false)
        stat.toJson must beEqualTo("{ }").ignoreSpace
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

        stat2.enumeration must haveSize(100)
        forall(100 until 200)(i => stat2.enumeration(WKTUtils.read(s"POINT(${i -20} ${i / 2 - 20})")) mustEqual 1)

        stat += stat2

        stat.enumeration must haveSize(200)
        forall(0 until 100)(i => stat.enumeration(WKTUtils.read(s"POINT(-$i ${i / 2})")) mustEqual 1)
        forall(100 until 200)(i => stat.enumeration(WKTUtils.read(s"POINT(${i -20} ${i / 2 - 20})")) mustEqual 1)

        stat2.enumeration must haveSize(100)
        forall(100 until 200)(i => stat2.enumeration(WKTUtils.read(s"POINT(${i -20} ${i / 2 - 20})")) mustEqual 1)
      }

      "clear" >> {
        val stat = newStat[Geometry]("geom")
        stat.isEmpty must beFalse

        stat.clear()

        stat.enumeration must beEmpty
        stat.isEmpty must beTrue
      }
    }
  }
}
