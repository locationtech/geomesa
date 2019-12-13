/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import java.lang.{Double => jDouble, Float => jFloat, Long => jLong}
import java.util.Date

import org.locationtech.jts.geom.Geometry
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.junit.runner.RunWith
import org.locationtech.geomesa.curve.{BinnedTime, TimePeriod, Z2SFC}
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.sfcurve.zorder.Z2
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class FrequencyTest extends Specification with StatTestHelper {

  def createStat[T](attribute: String, precision: Int, observe: Boolean): Frequency[T] = {
    val s = Stat(sft, Stat.Frequency(attribute, precision))
    if (observe) {
      features.foreach { s.observe }
    }
    s.asInstanceOf[Frequency[T]]
  }

  def stringStat(precision: Int, observe: Boolean = true) =
    createStat[String]("strAttr", precision: Int, observe)

  def intStat(precision: Int, observe: Boolean = true) =
    createStat[Integer]("intAttr", precision: Int, observe)

  def longStat(precision: Int, observe: Boolean = true) =
    createStat[jLong]("longAttr", precision: Int, observe)

  def floatStat(precision: Int, observe: Boolean = true) =
    createStat[jFloat]("floatAttr", precision: Int, observe)

  def doubleStat(precision: Int, observe: Boolean = true) =
    createStat[jDouble]("doubleAttr", precision: Int, observe)

  def dateStat(precision: Int, observe: Boolean = true) =
    createStat[Date]("dtg", precision: Int, observe)

  def geomStat(precision: Int, observe: Boolean = true) =
    createStat[Geometry]("geom", precision: Int, observe)

  def toDate(string: String) = java.util.Date.from(java.time.LocalDateTime.parse(string, GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC))
  def toGeom(string: String) = WKTUtils.read(string)

  "Frequency stat" should {

    "enumerate ranges" >> {
      val min = Z2SFC.invert(Z2(2, 2))
      val max = Z2SFC.invert(Z2(3, 6))
      val ranges = Z2SFC.ranges(Seq((min._1, min._2, max._1, max._2)))
      val indices = Frequency.enumerate(ranges, 64).toSeq
      indices must containTheSameElementsAs(Seq(12, 13, 14, 15, 36, 37, 38, 39, 44, 45))
    }

    "support weekly binning" >> {
      val stat = Stat(sft, Stat.Frequency("longAttr", "dtg", TimePeriod.Week, 1)).asInstanceOf[Frequency[Long]]
      val weekStart = 45 * 52 // approximately jan 2015
      val weeks = Set(weekStart, weekStart + 1, weekStart + 2, weekStart + 3)
      val dayStart = BinnedTime.Epoch.plusWeeks(weekStart).plusHours(1)
      (0 until 28 * 4).foreach { i =>
        val sf = SimpleFeatureBuilder.build(sft, Array[AnyRef](), i.toString)
        sf.setAttribute("longAttr", i)
        sf.setAttribute("geom", "POINT(-75 45)")
        sf.setAttribute("dtg", Date.from(dayStart.plusDays(i % 28).toInstant))
        stat.observe(sf)
      }
      val serializer = StatSerializer(sft)

      stat.sketchMap must haveSize(4)
      stat.sketchMap.keySet mustEqual weeks

      val offsets = (0 until 4).map(_ * 28)
      forall(offsets.flatMap(o => o +  0 until o +  7))(stat.count(weekStart.toShort, _) mustEqual 1)
      forall(offsets.flatMap(o => o +  7 until o + 14))(stat.count((weekStart + 1).toShort, _) mustEqual 1)
      forall(offsets.flatMap(o => o + 14 until o + 21))(stat.count((weekStart + 2).toShort, _) mustEqual 1)
      forall(offsets.flatMap(o => o + 21 until o + 28))(stat.count((weekStart + 3).toShort, _) mustEqual 1)

      val serialized = serializer.serialize(stat)
      val deserialized = serializer.deserialize(serialized)
      stat.isEquivalent(deserialized) must beTrue

      val splits = stat.splitByTime.toMap
      splits must haveSize(4)
      splits.keySet mustEqual weeks
      forall(offsets.flatMap(o => o +  0 until o +  7))(d => splits(weekStart.toShort).count(d) mustEqual 1)
      forall(offsets.flatMap(o => o +  7 until o + 14))(d => splits((weekStart + 1).toShort).count(d) mustEqual 1)
      forall(offsets.flatMap(o => o + 14 until o + 21))(d => splits((weekStart + 2).toShort).count(d) mustEqual 1)
      forall(offsets.flatMap(o => o + 21 until o + 28))(d => splits((weekStart + 3).toShort).count(d) mustEqual 1)
    }

    "work with strings" >> {
      "be empty initially" >> {
        val stat = stringStat(6, observe = false)
        stat.isEmpty must beTrue
        stat.size mustEqual 0
      }

      "correctly bin values"  >> {
        val stat = stringStat(6)
        stat.isEmpty must beFalse
        stat.size mustEqual 100
        forall(0 until 100)(i => stat.count(f"abc$i%03d") must beBetween(1L, 2L))
        stat.count("foo") mustEqual 0
      }

      "serialize and deserialize" >> {
        val stat = stringStat(6)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Frequency[String]]
        unpacked.asInstanceOf[Frequency[String]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[String]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[String]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[String]].toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = stringStat(6, observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Frequency[String]]
        unpacked.asInstanceOf[Frequency[String]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[String]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[String]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[String]].toJson mustEqual stat.toJson
      }

      "deserialize as immutable value" >> {
        val stat = stringStat(6)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed, immutable = true)

        unpacked must beAnInstanceOf[Frequency[String]]
        unpacked.asInstanceOf[Frequency[String]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[String]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[String]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[String]].toJson mustEqual stat.toJson

        unpacked.clear must throwAn[Exception]
        unpacked.+=(stat) must throwAn[Exception]
        unpacked.observe(features.head) must throwAn[Exception]
        unpacked.unobserve(features.head) must throwAn[Exception]
      }

      "combine two Frequencies" >> {
        val stat = stringStat(6)
        val stat2 = stringStat(6, observe = false)

        features2.foreach { stat2.observe }

        stat2.size mustEqual 100
        forall(100 until 200)(i => stat2.count(f"abc$i%03d") must beBetween(1L, 2L))
        stat2.count("foo") mustEqual 0L

        stat += stat2

        stat.size mustEqual 200
        forall(0 until 200)(i => stat.count(f"abc$i%03d") must beBetween(1L, 3L))
        stat.count("foo") mustEqual 0L

        stat2.size mustEqual 100
        forall(100 until 200)(i => stat2.count(f"abc$i%03d") must beBetween(1L, 2L))
        stat2.count("foo") mustEqual 0L
      }

      "clear" >> {
        val stat = stringStat(6)
        stat.clear()

        stat.isEmpty must beTrue
        stat.size mustEqual 0
        forall(0 until 200)(i => stat.count(f"abc$i%3d") mustEqual 0)
      }
    }

    "work with integers" >> {
      "be empty initially" >> {
        val stat = intStat(1, observe = false)
        stat.isEmpty must beTrue
        stat.size mustEqual 0
      }

      "correctly bin values"  >> {
        val stat = intStat(1)
        stat.isEmpty must beFalse
        stat.size mustEqual 100
        forall(0 until 100)(i => stat.count(i) must beBetween(1L, 2L))
        stat.count(200) mustEqual 0
      }

      "serialize and deserialize" >> {
        val stat = intStat(1)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Frequency[Integer]]
        unpacked.asInstanceOf[Frequency[Integer]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[Integer]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[Integer]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[Integer]].toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = intStat(1, observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Frequency[Integer]]
        unpacked.asInstanceOf[Frequency[Integer]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[Integer]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[Integer]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[Integer]].toJson mustEqual stat.toJson
      }

      "combine two Frequencies" >> {
        val stat = intStat(1)
        val stat2 = intStat(1, observe = false)

        features2.foreach { stat2.observe }

        stat2.size mustEqual 100
        forall(100 until 200)(i => stat2.count(i) must beBetween(1L, 2L))
        stat2.count(300) mustEqual 0L

        stat += stat2

        stat.size mustEqual 200
        forall(0 until 200)(i => stat.count(i) must beBetween(1L, 3L))
        stat.count(300) mustEqual 0L

        stat2.size mustEqual 100
        forall(100 until 200)(i => stat2.count(i) must beBetween(1L, 2L))
        stat2.count(300) mustEqual 0L
      }

      "clear" >> {
        val stat = intStat(1)
        stat.clear()

        stat.isEmpty must beTrue
        stat.size mustEqual 0
        forall(0 until 200)(i => stat.count(i) mustEqual 0)
      }
    }

    "work with longs" >> {
      "be empty initially" >> {
        val stat = longStat(1, observe = false)
        stat.isEmpty must beTrue
        stat.size mustEqual 0
      }

      "correctly bin values"  >> {
        val stat = longStat(1)
        stat.isEmpty must beFalse
        stat.size mustEqual 100
        forall(0 until 100)(i => stat.count(i.toLong) must beBetween(1L, 2L))
        stat.count(200L) mustEqual 0
      }

      "serialize and deserialize" >> {
        val stat = longStat(1)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Frequency[jLong]]
        unpacked.asInstanceOf[Frequency[jLong]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[jLong]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[jLong]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[jLong]].toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = longStat(1, observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Frequency[jLong]]
        unpacked.asInstanceOf[Frequency[jLong]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[jLong]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[jLong]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[jLong]].toJson mustEqual stat.toJson
      }

      "combine two Frequencies" >> {
        val stat = longStat(1)
        val stat2 = longStat(1, observe = false)

        features2.foreach { stat2.observe }

        stat2.size mustEqual 100
        forall(100 until 200)(i => stat2.count(i.toLong) must beBetween(1L, 2L))
        stat2.count(300L) mustEqual 0L

        stat += stat2

        stat.size mustEqual 200
        forall(0 until 200)(i => stat.count(i.toLong) must beBetween(1L, 3L))
        stat.count(300L) mustEqual 0L

        stat2.size mustEqual 100
        forall(100 until 200)(i => stat2.count(i.toLong) must beBetween(1L, 2L))
        stat2.count(300L) mustEqual 0L
      }

      "clear" >> {
        val stat = longStat(1)
        stat.clear()

        stat.isEmpty must beTrue
        stat.size mustEqual 0
        forall(0 until 200)(i => stat.count(i.toLong) mustEqual 0)
      }
    }

    "work with floats" >> {
      "be empty initially" >> {
        val stat = floatStat(1, observe = false)
        stat.isEmpty must beTrue
        stat.size mustEqual 0
      }

      "correctly bin values"  >> {
        val stat = floatStat(1)
        stat.isEmpty must beFalse
        stat.size mustEqual 100
        forall(0 until 100)(i => stat.count(i.toFloat) must beBetween(1L, 2L))
        stat.count(200f) mustEqual 0
      }

      "serialize and deserialize" >> {
        val stat = floatStat(1)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Frequency[jFloat]]
        unpacked.asInstanceOf[Frequency[jFloat]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[jFloat]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[jFloat]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[jFloat]].toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = floatStat(1, observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Frequency[jFloat]]
        unpacked.asInstanceOf[Frequency[jFloat]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[jFloat]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[jFloat]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[jFloat]].toJson mustEqual stat.toJson
      }

      "combine two Frequencies" >> {
        val stat = floatStat(1)
        val stat2 = floatStat(1, observe = false)

        features2.foreach { stat2.observe }

        stat2.size mustEqual 100
        forall(100 until 200)(i => stat2.count(i.toFloat) must beBetween(1L, 2L))
        stat2.count(300f) mustEqual 0L

        stat += stat2

        stat.size mustEqual 200
        forall(0 until 200)(i => stat.count(i.toFloat) must beBetween(1L, 3L))
        stat.count(300f) mustEqual 0L

        stat2.size mustEqual 100
        forall(100 until 200)(i => stat2.count(i.toFloat) must beBetween(1L, 2L))
        stat2.count(300f) mustEqual 0L
      }

      "clear" >> {
        val stat = floatStat(1)
        stat.clear()

        stat.isEmpty must beTrue
        stat.size mustEqual 0
        forall(0 until 200)(i => stat.count(i.toFloat) mustEqual 0)
      }
    }

    "work with doubles" >> {
      "be empty initially" >> {
        val stat = doubleStat(1, observe = false)
        stat.isEmpty must beTrue
        stat.size mustEqual 0
      }

      "correctly bin values"  >> {
        val stat = doubleStat(1)
        stat.isEmpty must beFalse
        stat.size mustEqual 100
        forall(0 until 100)(i => stat.count(i.toDouble) must beBetween(1L, 2L))
        stat.count(200d) mustEqual 0
      }

      "serialize and deserialize" >> {
        val stat = doubleStat(1)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Frequency[jDouble]]
        unpacked.asInstanceOf[Frequency[jDouble]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[jDouble]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[jDouble]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[jDouble]].toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = doubleStat(1, observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Frequency[jDouble]]
        unpacked.asInstanceOf[Frequency[jDouble]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[jDouble]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[jDouble]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[jDouble]].toJson mustEqual stat.toJson
      }

      "combine two Frequencies" >> {
        val stat = doubleStat(1)
        val stat2 = doubleStat(1, observe = false)

        features2.foreach { stat2.observe }

        stat2.size mustEqual 100
        forall(100 until 200)(i => stat2.count(i.toDouble) must beBetween(1L, 2L))
        stat2.count(300d) mustEqual 0L

        stat += stat2

        stat.size mustEqual 200
        forall(0 until 200)(i => stat.count(i.toDouble) must beBetween(1L, 3L))
        stat.count(300d) mustEqual 0L

        stat2.size mustEqual 100
        forall(100 until 200)(i => stat2.count(i.toDouble) must beBetween(1L, 2L))
        stat2.count(300d) mustEqual 0L
      }

      "clear" >> {
        val stat = doubleStat(1)
        stat.clear()

        stat.isEmpty must beTrue
        stat.size mustEqual 0
        forall(0 until 200)(i => stat.count(i.toDouble) mustEqual 0)
      }
    }

    "work with dates" >> {
      "be empty initially" >> {
        val stat = dateStat(1, observe = false)
        stat.isEmpty must beTrue
        stat.size mustEqual 0
      }

      "correctly bin values"  >> {
        val stat = dateStat(1)
        stat.isEmpty must beFalse
        stat.size mustEqual 100
        forall(0 until 100)(i => stat.count(toDate(f"2012-01-01T${i%24}%02d:00:00.000Z")) must beBetween(4L, 5L))
        stat.count(toDate(f"2012-01-05T00:00:00.000Z")) mustEqual 0
      }

      "serialize and deserialize" >> {
        val stat = dateStat(1)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Frequency[Date]]
        unpacked.asInstanceOf[Frequency[Date]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[Date]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[Date]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[Date]].toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = dateStat(1, observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Frequency[Date]]
        unpacked.asInstanceOf[Frequency[Date]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[Date]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[Date]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[Date]].toJson mustEqual stat.toJson
      }

      "combine two Frequencies" >> {
        val stat = dateStat(1)
        val stat2 = dateStat(1, observe = false)

        features2.foreach { stat2.observe }

        stat2.size mustEqual 100
        forall(100 until 200)(i => stat2.count(toDate(f"2012-01-02T${i%24}%02d:00:00.000Z")) must beBetween(4L, 5L))
        stat2.count(toDate(f"2012-01-05T00:00:00.000Z")) mustEqual 0L

        stat += stat2

        stat.size mustEqual 200
        forall(0 until 100)(i => stat.count(toDate(f"2012-01-01T${i%24}%02d:00:00.000Z")) must beBetween(4L, 5L))
        forall(100 until 200)(i => stat.count(toDate(f"2012-01-02T${i%24}%02d:00:00.000Z")) must beBetween(4L, 5L))
        stat.count(toDate(f"2012-01-05T00:00:00.000Z")) mustEqual 0L

        stat2.size mustEqual 100
        forall(100 until 200)(i => stat2.count(toDate(f"2012-01-02T${i%24}%02d:00:00.000Z")) must beBetween(4L, 5L))
        stat2.count(toDate(f"2012-01-05T00:00:00.000Z")) mustEqual 0L
      }

      "clear" >> {
        val stat = dateStat(1)
        stat.clear()

        stat.isEmpty must beTrue
        stat.size mustEqual 0
        forall(0 until 100)(i => stat.count(toDate(f"2012-01-01T${i%24}%02d:00:00.000Z")) mustEqual 0)
        forall(100 until 200)(i => stat.count(toDate(f"2012-01-02T${i%24}%02d:00:00.000Z")) mustEqual 0)
      }
    }

    "work with geometries" >> {
      "be empty initially" >> {
        val stat = geomStat(24, observe = false)
        stat.isEmpty must beTrue
        stat.size mustEqual 0
      }

      "correctly bin values"  >> {
        val stat = geomStat(24)
        stat.isEmpty must beFalse
        stat.size mustEqual 100
        forall(0 until 100)(i => stat.count(toGeom(s"POINT(-$i ${i / 2})")) must beBetween(1L, 6L))
      }

      "serialize and deserialize" >> {
        val stat = geomStat(24)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Frequency[Geometry]]
        unpacked.asInstanceOf[Frequency[Geometry]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[Geometry]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[Geometry]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[Geometry]].toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = geomStat(24, observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Frequency[Geometry]]
        unpacked.asInstanceOf[Frequency[Geometry]].property mustEqual stat.property
        unpacked.asInstanceOf[Frequency[Geometry]].precision mustEqual stat.precision
        unpacked.asInstanceOf[Frequency[Geometry]].size mustEqual stat.size
        unpacked.asInstanceOf[Frequency[Geometry]].toJson mustEqual stat.toJson
      }

      "combine two Frequencies" >> {
        val stat = geomStat(24)
        val stat2 = geomStat(24, observe = false)

        features2.foreach { stat2.observe }

        stat2.size mustEqual 100
        forall(100 until 200)(i => stat2.count(toGeom(s"POINT(${i -20} ${i / 2 - 20})")) must beBetween(1L, 6L))

        stat += stat2

        stat.size mustEqual 200
        forall(0 until 100)(i => stat.count(toGeom(s"POINT(-$i ${i / 2})")) must beBetween(1L, 10L))
        forall(100 until 200)(i => stat.count(toGeom(s"POINT(${i -20} ${i / 2 - 20})")) must beBetween(1L, 10L))

        stat2.size mustEqual 100
        forall(100 until 200)(i => stat2.count(toGeom(s"POINT(${i -20} ${i / 2 - 20})")) must beBetween(1L, 6L))
      }

      "clear" >> {
        val stat = geomStat(24)
        stat.clear()

        stat.isEmpty must beTrue
        stat.size mustEqual 0
        forall(0 until 100)(i => stat.count(toGeom(s"POINT(-$i ${i / 2})")) mustEqual 0)
        forall(100 until 200)(i => stat.count(toGeom(s"POINT(${i -20} ${i / 2 - 20})")) mustEqual 0)
      }
    }
  }
}
