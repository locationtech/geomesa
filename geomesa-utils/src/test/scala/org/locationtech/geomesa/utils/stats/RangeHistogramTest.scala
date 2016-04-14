/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util.Date

import com.vividsolutions.jts.geom.Geometry
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RangeHistogramTest extends Specification with StatTestHelper {

  def createStat[T](attribute: String, bins: Int, min: String, max: String, observe: Boolean): RangeHistogram[T] = {
    val s = Stat(sft, s"RangeHistogram($attribute,$bins,'$min','$max')")
    if (observe) {
      features.foreach { s.observe }
    }
    s.asInstanceOf[RangeHistogram[T]]
  }

  def stringStat(bins: Int, min: String, max: String, observe: Boolean = true) =
    createStat[String]("strAttr", bins, min, max, observe)

  def intStat(bins: Int, min: Int, max: Int, observe: Boolean = true) =
    createStat[Integer]("intAttr", bins, min.toString, max.toString, observe)

  def longStat(bins: Int, min: Long, max: Long, observe: Boolean = true) =
    createStat[jLong]("longAttr", bins, min.toString, max.toString, observe)

  def floatStat(bins: Int, min: Float, max: Float, observe: Boolean = true) =
    createStat[jFloat]("floatAttr", bins, min.toString, max.toString, observe)

  def doubleStat(bins: Int, min: Double, max: Double, observe: Boolean = true) =
    createStat[jDouble]("doubleAttr", bins, min.toString, max.toString, observe)

  def dateStat(bins: Int, min: String, max: String, observe: Boolean = true) =
    createStat[Date]("dtg", bins, min, max, observe)

  def geomStat(bins: Int, min: String, max: String, observe: Boolean = true) =
    createStat[Geometry]("geom", bins, min, max, observe)

  def toDate(string: String) = GeoToolsDateFormat.parseDateTime(string).toDate
  def toGeom(string: String) = WKTUtils.read(string)

  "RangeHistogram stat" should {

    "work with strings" >> {
      "be empty initially" >> {
        val stat = stringStat(20, "abc000", "abc200", observe = false)
        stat.isEmpty must beTrue
        stat.length mustEqual 20
        stat.endpoints mustEqual ("abc000", "abc200")
        forall(0 until 20)(stat.count(_) mustEqual 0)
      }

      "correctly bin values"  >> {
        val stat = stringStat(20, "abc000", "abc200")
        stat.isEmpty must beFalse
        stat.length mustEqual 20
        stat.count(0) mustEqual 40
        stat.count(1) mustEqual 38
        stat.count(2) mustEqual 22
        forall(3 until 20)(stat.count(_) mustEqual 0)
      }

      "serialize and deserialize" >> {
        val stat = stringStat(20, "abc000", "abc200")
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[RangeHistogram[String]]
        unpacked.asInstanceOf[RangeHistogram[String]].length mustEqual stat.length
        unpacked.asInstanceOf[RangeHistogram[String]].attribute mustEqual stat.attribute
        unpacked.asInstanceOf[RangeHistogram[String]].toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = stringStat(20, "abc000", "abc200", observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[RangeHistogram[String]]
        unpacked.asInstanceOf[RangeHistogram[String]].length mustEqual stat.length
        unpacked.asInstanceOf[RangeHistogram[String]].attribute mustEqual stat.attribute
        unpacked.asInstanceOf[RangeHistogram[String]].toJson mustEqual stat.toJson
      }

      "combine two RangeHistograms" >> {
        val stat = stringStat(20, "abc000", "abc200")
        val stat2 = stringStat(20, "abc000", "abc200", observe = false)

        features2.foreach { stat2.observe }

        stat2.length mustEqual 20
        stat2.count(10) mustEqual 40
        stat2.count(11) mustEqual 38
        stat2.count(12) mustEqual 22
        forall((0 until 10) ++ (13 until 20))(stat2.count(_) mustEqual 0)

        stat += stat2

        stat.length mustEqual 20
        stat.count(0) mustEqual 40
        stat.count(1) mustEqual 38
        stat.count(2) mustEqual 22
        stat.count(10) mustEqual 40
        stat.count(11) mustEqual 38
        stat.count(12) mustEqual 22
        forall((3 until 10) ++ (13 until 20))(stat.count(_) mustEqual 0)

        stat2.length mustEqual 20
        stat2.count(10) mustEqual 40
        stat2.count(11) mustEqual 38
        stat2.count(12) mustEqual 22
        forall((0 until 10) ++ (13 until 20))(stat2.count(_) mustEqual 0)
      }

      "clear" >> {
        val stat = stringStat(20, "abc000", "abc200")
        stat.clear()

        stat.isEmpty must beTrue
        stat.length mustEqual 20
        forall(0 until 20)(stat.count(_) mustEqual 0)
      }
    }

    "work with integers" >> {
      "be empty initially" >> {
        val stat = intStat(20, 0, 199, observe = false)
        stat.isEmpty must beTrue
        stat.length mustEqual 20
        stat.endpoints mustEqual (0, 199)
        forall(0 until 20)(stat.count(_) mustEqual 0)
      }

      "correctly bin values"  >> {
        val stat = intStat(20, 0, 199)
        stat.isEmpty must beFalse
        stat.length mustEqual 20
        forall(0 until 10)(stat.count(_) mustEqual 10)
        forall(10 until 20)(stat.count(_) mustEqual 0)
      }

      "serialize and deserialize" >> {
        val stat = intStat(20, 0, 199)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[RangeHistogram[Integer]]
        unpacked.asInstanceOf[RangeHistogram[Integer]].length mustEqual stat.length
        unpacked.asInstanceOf[RangeHistogram[Integer]].attribute mustEqual stat.attribute
        unpacked.toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = intStat(20, 0, 199, observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[RangeHistogram[Integer]]
        unpacked.asInstanceOf[RangeHistogram[Integer]].length mustEqual stat.length
        unpacked.asInstanceOf[RangeHistogram[Integer]].attribute mustEqual stat.attribute
        unpacked.toJson mustEqual stat.toJson
      }

      "combine two RangeHistograms" >> {
        val stat = intStat(20, 0, 199)
        val stat2 = intStat(20, 0, 199, observe = false)

        features2.foreach { stat2.observe }

        stat2.length mustEqual 20
        forall(0 until 10)(stat2.count(_) mustEqual 0)
        forall(10 until 20)(stat2.count(_) mustEqual 10)

        stat += stat2

        stat.length mustEqual 20
        forall(0 until 20)(stat.count(_) mustEqual 10)
        stat2.length mustEqual 20
        forall(0 until 10)(stat2.count(_) mustEqual 0)
        forall(10 until 20)(stat2.count(_) mustEqual 10)
      }

      "combine two RangeHistograms with different bounds" >> {
        val stat = intStat(20, 0, 99)
        val stat2 = intStat(20, 100, 199, observe = false)

        features2.foreach { stat2.observe }

        stat.length mustEqual 20
        forall(0 until 20)(stat.count(_) mustEqual 5)

        stat2.length mustEqual 20
        forall(0 until 20)(stat2.count(_) mustEqual 5)

        stat += stat2

        stat.length mustEqual 20
        stat.endpoints mustEqual (0, 199)
        forall(0 until 20)(stat.count(_) mustEqual 10)
      }

      "combine two RangeHistograms with different lengths" >> {
        val stat = intStat(20, 0, 199)
        val stat2 = intStat(10, 0, 199, observe = false)

        features2.foreach { stat2.observe }

        stat.length mustEqual 20
        forall(0 until 10)(stat.count(_) mustEqual 10)
        forall(10 until 20)(stat.count(_) mustEqual 0)

        stat2.length mustEqual 10
        forall(0 until 5)(stat2.count(_) mustEqual 0)
        forall(5 until 10)(stat2.count(_) mustEqual 20)

        stat += stat2

        stat.length mustEqual 20
        stat.endpoints mustEqual (0, 199)
        forall(0 until 20)(stat.count(_) mustEqual 10)
      }

      "combine two RangeHistograms with different bounds and lengths" >> {
        val stat = intStat(20, 0, 99)
        val stat2 = intStat(10, 50, 149, observe = false)

        features2.foreach { stat2.observe }

        stat.length mustEqual 20
        forall(0 until 20)(stat.count(_) mustEqual 5)

        stat2.length mustEqual 10
        forall(0 until 5)(stat2.count(_) mustEqual 0)
        forall(5 until 10)(stat2.count(_) mustEqual 10)
        Seq(stat, stat2).flatMap(s => (0 until s.length).map(s.count)).sum mustEqual 150

        stat += stat2

        stat.length mustEqual 20
        stat.endpoints mustEqual (0, 149)
        forall(Seq(0, 2, 4, 6, 8, 10, 12))(stat.count(_) mustEqual 7)
        forall(Seq(1, 3, 5, 7, 9, 11, 13))(stat.count(_) mustEqual 8)
        forall(Seq(14, 17, 18))(stat.count(_) mustEqual 10)
        forall(Seq(15, 16, 19))(stat.count(_) mustEqual 5)
        (0 until stat.length).map(stat.count).sum mustEqual 150
      }

      "clear" >> {
        val stat = intStat(20, 0, 199)
        stat.clear()

        stat.isEmpty must beTrue
        stat.length mustEqual 20
        forall(0 until 20)(stat.count(_) mustEqual 0)
      }
    }

    "work with longs" >> {
      "be empty initially" >> {
        val stat = longStat(7, 90, 110, observe = false)
        stat.isEmpty must beTrue
        stat.length mustEqual 7
        stat.endpoints mustEqual (90, 110)
        forall(0 until 7)(stat.count(_) mustEqual 0)
      }

      "correctly bin values" >> {
        val stat = longStat(7, 90, 110)

        stat.isEmpty must beFalse
        stat.length mustEqual 7
        stat.endpoints mustEqual (90, 110)
        forall(0 until 3)(stat.count(_) mustEqual 3)
        stat.count(3) mustEqual 1
        forall(4 until 7)(stat.count(_) mustEqual 0)
      }

      "serialize and deserialize" >> {
        val stat = longStat(7, 90, 110)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[RangeHistogram[jLong]]
        unpacked.asInstanceOf[RangeHistogram[jLong]].length mustEqual stat.length
        unpacked.asInstanceOf[RangeHistogram[jLong]].attribute mustEqual stat.attribute
        unpacked.toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = longStat(7, 90, 110, observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[RangeHistogram[jLong]]
        unpacked.asInstanceOf[RangeHistogram[jLong]].length mustEqual stat.length
        unpacked.asInstanceOf[RangeHistogram[jLong]].attribute mustEqual stat.attribute
        unpacked.toJson mustEqual stat.toJson
      }

      "combine two RangeHistograms" >> {
        val stat = longStat(7, 90, 110)
        val stat2 = longStat(7, 90, 110, observe = false)

        features2.foreach { stat2.observe }

        stat2.length mustEqual 7
        forall(0 until 3)(stat2.count(_) mustEqual 0)
        stat2.count(3) mustEqual 2
        forall(4 until 7)(stat2.count(_) mustEqual 3)

        stat += stat2

        stat.length mustEqual 7
        forall(0 until 7)(stat.count(_) mustEqual 3)
        stat2.length mustEqual 7
        forall(0 until 3)(stat2.count(_) mustEqual 0)
        stat2.count(3) mustEqual 2
        forall(4 until 7)(stat2.count(_) mustEqual 3)
      }

      "clear" >> {
        val stat = longStat(7, 90, 110)
        stat.clear()

        stat.isEmpty must beTrue
        stat.length mustEqual 7
        forall(0 until 7)(stat.count(_) mustEqual 0)
      }
    }

    "work with floats" >> {
      "be empty initially" >> {
        val stat = floatStat(7, 90, 110, observe = false)

        stat.isEmpty must beTrue
        stat.length mustEqual 7
        stat.endpoints mustEqual (90f, 110f)
        forall(0 until 7)(stat.count(_) mustEqual 0)
      }

      "correctly bin values" >> {
        val stat = floatStat(7, 90, 110)

        stat.isEmpty must beFalse
        stat.length mustEqual 7
        stat.endpoints mustEqual (90f, 110f)
        forall(0 until 3)(stat.count(_) mustEqual 3)
        stat.count(3) mustEqual 1
        forall(4 until 7)(stat.count(_) mustEqual 0)
      }

      "serialize and deserialize" >> {
        val stat = floatStat(7, 90, 110)

        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[RangeHistogram[jFloat]]
        unpacked.asInstanceOf[RangeHistogram[jFloat]].length mustEqual stat.length
        unpacked.asInstanceOf[RangeHistogram[jFloat]].attribute mustEqual stat.attribute
        unpacked.toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = floatStat(7, 90, 110, observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[RangeHistogram[jFloat]]
        unpacked.asInstanceOf[RangeHistogram[jFloat]].length mustEqual stat.length
        unpacked.asInstanceOf[RangeHistogram[jFloat]].attribute mustEqual stat.attribute
        unpacked.toJson mustEqual stat.toJson
      }

      "combine two RangeHistograms" >> {
        val stat = floatStat(7, 90, 110)
        val stat2 = floatStat(7, 90, 110, observe = false)

        features2.foreach { stat2.observe }

        stat2.length mustEqual 7
        forall(0 until 3)(stat2.count(_) mustEqual 0)
        stat2.count(3) mustEqual 2
        forall(4 until 7)(stat2.count(_) mustEqual 3)

        stat += stat2

        stat.length mustEqual 7
        forall(0 until 7)(stat.count(_) mustEqual 3)
        stat2.length mustEqual 7
        forall(0 until 3)(stat2.count(_) mustEqual 0)
        stat2.count(3) mustEqual 2
        forall(4 until 7)(stat2.count(_) mustEqual 3)
      }

      "clear" >> {
        val stat = floatStat(7, 90, 110)
        stat.clear()

        stat.isEmpty must beTrue
        stat.length mustEqual 7
        forall(0 until 7)(stat.count(_) mustEqual 0)
      }
    }

    "work with doubles" >> {
      "be empty initially" >> {
        val stat = doubleStat(7, 90, 110, observe = false)

        stat.isEmpty must beTrue
        stat.length mustEqual 7
        stat.endpoints mustEqual (90.0, 110.0)
        forall(0 until 7)(stat.count(_) mustEqual 0)
      }

      "correctly bin values" >> {
        val stat = doubleStat(7, 90, 110)

        stat.isEmpty must beFalse
        stat.length mustEqual 7
        stat.endpoints mustEqual (90.0, 110.0)
        forall(0 until 3)(stat.count(_) mustEqual 3)
        stat.count(3) mustEqual 1
        forall(4 until 7)(stat.count(_) mustEqual 0)
      }

      "serialize and deserialize" >> {
        val stat = doubleStat(7, 90, 110)

        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[RangeHistogram[jDouble]]
        unpacked.asInstanceOf[RangeHistogram[jDouble]].length mustEqual stat.length
        unpacked.asInstanceOf[RangeHistogram[jDouble]].attribute mustEqual stat.attribute
        unpacked.toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = doubleStat(7, 90, 110, observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[RangeHistogram[jDouble]]
        unpacked.asInstanceOf[RangeHistogram[jDouble]].length mustEqual stat.length
        unpacked.asInstanceOf[RangeHistogram[jDouble]].attribute mustEqual stat.attribute
        unpacked.toJson mustEqual stat.toJson
      }

      "combine two RangeHistograms" >> {
        val stat = doubleStat(7, 90, 110)
        val stat2 = doubleStat(7, 90, 110, observe = false)

        features2.foreach { stat2.observe }

        stat2.length mustEqual 7
        forall(0 until 3)(stat2.count(_) mustEqual 0)
        stat2.count(3) mustEqual 2
        forall(4 until 7)(stat2.count(_) mustEqual 3)

        stat += stat2

        stat.length mustEqual 7
        forall(0 until 7)(stat.count(_) mustEqual 3)
        stat2.length mustEqual 7
        forall(0 until 3)(stat2.count(_) mustEqual 0)
        stat2.count(3) mustEqual 2
        forall(4 until 7)(stat2.count(_) mustEqual 3)
      }

      "clear" >> {
        val stat = doubleStat(7, 90, 110)
        stat.clear()

        stat.isEmpty must beTrue
        stat.length mustEqual 7
        forall(0 until 7)(stat.count(_) mustEqual 0)
      }
    }

    "work with dates" >> {
      "be empty initially" >> {
        val stat = dateStat(24, "2012-01-01T00:00:00.000Z", "2012-01-03T00:00:00.000Z", observe = false)

        stat.isEmpty must beTrue
        stat.length mustEqual 24
        stat.endpoints mustEqual (toDate("2012-01-01T00:00:00.000Z"), toDate("2012-01-03T00:00:00.000Z"))
        forall(0 until 24)(stat.count(_) mustEqual 0)
      }

      "correctly bin values" >> {
        val stat = dateStat(24, "2012-01-01T00:00:00.000Z", "2012-01-03T00:00:00.000Z")

        stat.isEmpty must beFalse
        stat.length mustEqual 24
        stat.endpoints mustEqual (toDate("2012-01-01T00:00:00.000Z"), toDate("2012-01-03T00:00:00.000Z"))
        forall(0 until 2)(stat.count(_) mustEqual 10)
        forall(2 until 12)(stat.count(_) mustEqual 8)
        forall(12 until 24)(stat.count(_) mustEqual 0)
      }

      "serialize and deserialize" >> {
        val stat = dateStat(24, "2012-01-01T00:00:00.000Z", "2012-01-03T00:00:00.000Z")

        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[RangeHistogram[Date]]
        unpacked.asInstanceOf[RangeHistogram[Date]].length mustEqual stat.length
        unpacked.asInstanceOf[RangeHistogram[Date]].attribute mustEqual stat.attribute
        unpacked.toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = dateStat(24, "2012-01-01T00:00:00.000Z", "2012-01-03T00:00:00.000Z", observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[RangeHistogram[jDouble]]
        unpacked.asInstanceOf[RangeHistogram[jDouble]].length mustEqual stat.length
        unpacked.asInstanceOf[RangeHistogram[jDouble]].attribute mustEqual stat.attribute
        unpacked.toJson mustEqual stat.toJson
      }

      "combine two RangeHistograms" >> {
        val stat = dateStat(24, "2012-01-01T00:00:00.000Z", "2012-01-03T00:00:00.000Z")
        val stat2 = dateStat(24, "2012-01-01T00:00:00.000Z", "2012-01-03T00:00:00.000Z", observe = false)

        features2.foreach { stat2.observe }

        stat2.length mustEqual 24
        forall(0 until 12)(stat2.count(_) mustEqual 0)
        forall((12 until 14) ++ (16 until 24))(stat2.count(_) mustEqual 8)
        forall(15 until 16)(stat2.count(_) mustEqual 10)

        stat += stat2

        stat.length mustEqual 24
        forall((0 until 2) ++ (15 until 16))(stat.count(_) mustEqual 10)
        forall((2 until 14) ++ (16 until 24))(stat.count(_) mustEqual 8)

        stat2.length mustEqual 24
        forall(0 until 12)(stat2.count(_) mustEqual 0)
        forall((12 until 14) ++ (16 until 24))(stat2.count(_) mustEqual 8)
        forall(15 until 16)(stat2.count(_) mustEqual 10)
      }

      "clear" >> {
        val stat = dateStat(24, "2012-01-01T00:00:00.000Z", "2012-01-03T00:00:00.000Z")
        stat.clear()

        stat.isEmpty must beTrue
        stat.length mustEqual 24
        forall(0 until 24)(stat.count(_) mustEqual 0)
      }
    }

    "work with geometries" >> {
      "be empty initially" >> {
        val stat = geomStat(32, "POINT(-180 -90)", "POINT(180 90)", observe = false)

        stat.isEmpty must beTrue
        stat.length mustEqual 32
        stat.endpoints mustEqual (toGeom("POINT(-180 -90)"), toGeom("POINT(180 90)"))
        forall(0 until 32)(stat.count(_) mustEqual 0)
      }

      "correctly bin values" >> {
        val stat = geomStat(32, "POINT(-180 -90)", "POINT(180 90)")

        stat.isEmpty must beFalse
        stat.length mustEqual 32
        stat.endpoints mustEqual (toGeom("POINT(-180 -90)"), toGeom("POINT(180 90)"))
        stat.count(11) mustEqual 9
        stat.count(12) mustEqual 44
        stat.count(13) mustEqual 45
        stat.count(14) mustEqual 1
        stat.count(24) mustEqual 1
        forall((0 until 11) ++ (15 until 24) ++ (25 until 32))(stat.count(_) mustEqual 0)
      }

      "serialize and deserialize" >> {
        val stat = geomStat(32, "POINT(-180 -90)", "POINT(180 90)")

        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[RangeHistogram[jDouble]]
        unpacked.asInstanceOf[RangeHistogram[jDouble]].length mustEqual stat.length
        unpacked.asInstanceOf[RangeHistogram[jDouble]].attribute mustEqual stat.attribute
        unpacked.toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = geomStat(32, "POINT(-180 -90)", "POINT(180 90)", observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[RangeHistogram[jDouble]]
        unpacked.asInstanceOf[RangeHistogram[jDouble]].length mustEqual stat.length
        unpacked.asInstanceOf[RangeHistogram[jDouble]].attribute mustEqual stat.attribute
        unpacked.toJson mustEqual stat.toJson
      }

      "combine two RangeHistograms" >> {
        val stat = geomStat(32, "POINT(-180 -90)", "POINT(180 90)")
        val stat2 = geomStat(32, "POINT(-180 -90)", "POINT(180 90)", observe = false)

        features2.foreach { stat2.observe }

        stat2.length mustEqual 32
        stat2.count(25) mustEqual 10
        stat2.count(28) mustEqual 20
        stat2.count(30) mustEqual 25
        stat2.count(31) mustEqual 45
        forall((0 until 25) ++ (26 until 28) ++ Seq(29))(stat2.count(_) mustEqual 0)

        stat += stat2

        stat.count(11) mustEqual 9
        stat.count(12) mustEqual 44
        stat.count(13) mustEqual 45
        stat.count(14) mustEqual 1
        stat.count(24) mustEqual 1
        stat2.count(25) mustEqual 10
        stat2.count(28) mustEqual 20
        stat2.count(30) mustEqual 25
        stat2.count(31) mustEqual 45
        forall((0 until 11) ++ (15 until 24) ++ (26 until 28) ++ Seq(29))(stat.count(_) mustEqual 0)

        stat2.length mustEqual 32
        stat2.count(25) mustEqual 10
        stat2.count(28) mustEqual 20
        stat2.count(30) mustEqual 25
        stat2.count(31) mustEqual 45
        forall((0 until 25) ++ (26 until 28) ++ Seq(29))(stat2.count(_) mustEqual 0)
      }

      "clear" >> {
        val stat = geomStat(32, "POINT(-180 -90)", "POINT(180 90)")
        stat.clear()

        stat.isEmpty must beTrue
        stat.length mustEqual 32
        forall(0 until 32)(stat.count(_) mustEqual 0)
      }
    }
  }
}
