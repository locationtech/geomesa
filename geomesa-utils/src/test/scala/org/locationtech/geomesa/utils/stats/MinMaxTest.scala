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
class MinMaxTest extends Specification with StatTestHelper {

  def newStat[T](attribute: String, observe: Boolean = true): MinMax[T] = {
    val stat = Stat(sft, s"MinMax($attribute)")
    if (observe) {
      features.foreach { stat.observe }
    }
    stat.asInstanceOf[MinMax[T]]
  }

  "MinMax stat" should {

    "work with strings" >> {
      "be empty initiallly" >> {
        val minMax = newStat[String]("strAttr", observe = false)
        minMax.property mustEqual "strAttr"
        minMax.isEmpty must beTrue
        minMax.cardinality mustEqual 0
      }

      "observe correct values" >> {
        val minMax = newStat[String]("strAttr")
        minMax.bounds mustEqual ("abc000", "abc099")
        minMax.cardinality must beCloseTo(100L, 5)
      }

      "serialize to json" >> {
        val minMax = newStat[String]("strAttr")
        minMax.toJson must beMatching("""\{ "min": "abc000", "max": "abc099", "cardinality": \d+ \}""" ignoreSpace)
      }

      "serialize empty to json" >> {
        val minMax = newStat[String]("strAttr", observe = false)
        minMax.toJson must beEqualTo("""{ "min": null, "max": null, "cardinality": 0 }""").ignoreSpace
      }

      "serialize and deserialize" >> {
        val minMax = newStat[String]("strAttr")
        val packed = StatSerializer(sft).serialize(minMax)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual minMax.toJson
      }

      "serialize and deserialize empty MinMax" >> {
        val minMax = newStat[String]("strAttr", observe = false)
        val packed = StatSerializer(sft).serialize(minMax)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual minMax.toJson
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

      "combine two MinMaxes" >> {
        val minMax = newStat[String]("strAttr")
        val minMax2 = newStat[String]("strAttr", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.bounds mustEqual ("abc100", "abc199")
        minMax2.cardinality must beCloseTo(100L, 5)

        minMax += minMax2

        minMax.bounds mustEqual ("abc000", "abc199")
        minMax.cardinality must beCloseTo(200L, 5)
        minMax2.bounds mustEqual ("abc100", "abc199")
      }

      "clear" >> {
        val minMax = newStat[String]("strAttr")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.isEmpty must beTrue
        minMax.cardinality mustEqual 0
      }
    }

    "work with ints" >> {
      "be empty initiallly" >> {
        val minMax = newStat[java.lang.Integer]("intAttr", observe = false)
        minMax.property mustEqual "intAttr"
        minMax.isEmpty must beTrue
        minMax.cardinality mustEqual 0
      }

      "observe correct values" >> {
        val minMax = newStat[java.lang.Integer]("intAttr")
        minMax.bounds mustEqual (0, 99)
        minMax.cardinality must beCloseTo(100L, 5)
      }

      "serialize to json" >> {
        val minMax = newStat[java.lang.Integer]("intAttr")
        minMax.toJson must beMatching("""\{ "min": 0, "max": 99, "cardinality": [0-9]+ \}""".ignoreSpace)
      }

      "serialize empty to json" >> {
        val minMax = newStat[java.lang.Integer]("intAttr", observe = false)
        minMax.toJson must beEqualTo("""{ "min": null, "max": null, "cardinality": 0 }""").ignoreSpace
      }

      "serialize and deserialize" >> {
        val minMax = newStat[java.lang.Integer]("intAttr")
        val packed = StatSerializer(sft).serialize(minMax)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual minMax.toJson
      }

      "serialize and deserialize empty MinMax" >> {
        val minMax = newStat[java.lang.Integer]("intAttr", observe = false)
        val packed = StatSerializer(sft).serialize(minMax)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual minMax.toJson
      }

      "combine two MinMaxes" >> {
        val minMax = newStat[java.lang.Integer]("intAttr")
        val minMax2 = newStat[java.lang.Integer]("intAttr", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.bounds mustEqual (100, 199)
        minMax2.cardinality must beCloseTo(100L, 5)

        minMax += minMax2

        minMax.bounds mustEqual (0, 199)
        minMax.cardinality must beCloseTo(200L, 5)
        minMax2.bounds mustEqual (100, 199)
      }

      "clear" >> {
        val minMax = newStat[java.lang.Integer]("intAttr")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.isEmpty must beTrue
        minMax.cardinality mustEqual 0
      }

      "negatives" >> {
        val minMax3 = newStat[java.lang.Integer]("intAttr", observe = false)

        features3.foreach { minMax3.observe }

        minMax3.bounds mustEqual (-100, -1)
        minMax3.cardinality must beCloseTo(100L, 5)
      }
    }

    "work with longs" >> {
      "be empty initiallly" >> {
        val minMax = newStat[java.lang.Long]("longAttr", observe = false)
        minMax.property mustEqual "longAttr"
        minMax.isEmpty must beTrue
        minMax.cardinality mustEqual 0
      }

      "observe correct values" >> {
        val minMax = newStat[java.lang.Long]("longAttr")
        minMax.bounds mustEqual (0L, 99L)
        minMax.cardinality must beCloseTo(100L, 5)
      }

      "serialize to json" >> {
        val minMax = newStat[java.lang.Long]("longAttr")
        minMax.toJson must beMatching("""\{ "min": 0, "max": 99, "cardinality": \d+ \}""" ignoreSpace)
      }

      "serialize empty to json" >> {
        val minMax = newStat[java.lang.Long]("longAttr", observe = false)
        minMax.toJson must beEqualTo("""{ "min": null, "max": null, "cardinality": 0 }""").ignoreSpace
      }

      "serialize and deserialize" >> {
        val minMax = newStat[java.lang.Long]("longAttr")
        val packed = StatSerializer(sft).serialize(minMax)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual minMax.toJson
      }

      "serialize and deserialize empty MinMax" >> {
        val minMax = newStat[java.lang.Long]("longAttr", observe = false)
        val packed = StatSerializer(sft).serialize(minMax)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual minMax.toJson
      }

      "combine two MinMaxes" >> {
        val minMax = newStat[java.lang.Long]("longAttr")
        val minMax2 = newStat[java.lang.Long]("longAttr", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.bounds mustEqual (100L, 199L)
        minMax2.cardinality must beCloseTo(100L, 5)

        minMax += minMax2

        minMax.bounds mustEqual (0L, 199L)
        minMax.cardinality must beCloseTo(200L, 5)
        minMax2.bounds mustEqual (100L, 199L)
      }

      "clear" >> {
        val minMax = newStat[java.lang.Long]("longAttr")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.isEmpty must beTrue
        minMax.cardinality mustEqual 0
      }

      "negatives" >> {
        val minMax3 = newStat[java.lang.Integer]("longAttr", observe = false)

        features3.foreach { minMax3.observe }

        minMax3.bounds mustEqual (-100L, -1L)
        minMax3.cardinality must beCloseTo(100L, 5)
      }
    }

    "work with floats" >> {
      "be empty initiallly" >> {
        val minMax = newStat[java.lang.Float]("floatAttr", observe = false)
        minMax.property mustEqual "floatAttr"
        minMax.isEmpty must beTrue
        minMax.cardinality mustEqual 0
      }

      "observe correct values" >> {
        val minMax = newStat[java.lang.Float]("floatAttr")
        minMax.bounds mustEqual (0f, 99f)
        minMax.cardinality must beCloseTo(100L, 5)
      }

      "serialize to json" >> {
        val minMax = newStat[java.lang.Float]("floatAttr")
        minMax.toJson must beMatching("""\{ "min": 0.0, "max": 99.0, "cardinality": \d+ \}""" ignoreSpace)
      }

      "serialize empty to json" >> {
        val minMax = newStat[java.lang.Float]("floatAttr", observe = false)
        minMax.toJson must beEqualTo("""{ "min": null, "max": null, "cardinality": 0 }""").ignoreSpace
      }

      "serialize and deserialize" >> {
        val minMax = newStat[java.lang.Float]("floatAttr")
        val packed = StatSerializer(sft).serialize(minMax)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual minMax.toJson
      }

      "serialize and deserialize empty MinMax" >> {
        val minMax = newStat[java.lang.Float]("floatAttr", observe = false)
        val packed = StatSerializer(sft).serialize(minMax)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual minMax.toJson
      }

      "combine two MinMaxes" >> {
        val minMax = newStat[java.lang.Float]("floatAttr")
        val minMax2 = newStat[java.lang.Float]("floatAttr", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.bounds mustEqual (100f, 199f)
        minMax2.cardinality must beCloseTo(100L, 5)

        minMax += minMax2

        minMax.bounds mustEqual (0f, 199f)
        minMax.cardinality must beCloseTo(200L, 5)
        minMax2.bounds mustEqual (100f, 199f)
      }

      "clear" >> {
        val minMax = newStat[java.lang.Float]("floatAttr")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.isEmpty must beTrue
        minMax.cardinality mustEqual 0
      }

      "negatives" >> {
        val minMax3 = newStat[java.lang.Integer]("floatAttr", observe = false)

        features3.foreach { minMax3.observe }

        minMax3.bounds mustEqual (-100f, -1f)
        minMax3.cardinality must beCloseTo(100L, 5)
      }
    }

    "work with doubles" >> {
      "be empty initiallly" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr", observe = false)
        minMax.property mustEqual "doubleAttr"
        minMax.isEmpty must beTrue
        minMax.cardinality mustEqual 0
      }

      "observe correct values" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr")
        minMax.bounds mustEqual (0d, 99d)
        minMax.cardinality must beCloseTo(100L, 5)
      }

      "serialize to json" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr")
        minMax.toJson must beMatching("""\{ "min": 0.0, "max": 99.0, "cardinality": \d+ \}""" ignoreSpace)
      }

      "serialize empty to json" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr", observe = false)
        minMax.toJson must beEqualTo("""{ "min": null, "max": null, "cardinality": 0 }""").ignoreSpace
      }

      "serialize and deserialize" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr")
        val packed = StatSerializer(sft).serialize(minMax)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual minMax.toJson
      }

      "serialize and deserialize empty MinMax" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr", observe = false)
        val packed = StatSerializer(sft).serialize(minMax)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual minMax.toJson
      }

      "combine two MinMaxes" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr")
        val minMax2 = newStat[java.lang.Double]("doubleAttr", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.bounds mustEqual (100d, 199d)
        minMax2.cardinality must beCloseTo(100L, 5)

        minMax += minMax2

        minMax.bounds mustEqual (0d, 199d)
        minMax.cardinality must beCloseTo(200L, 10)
        minMax2.bounds mustEqual (100d, 199d)
      }

      "clear" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.isEmpty must beTrue
        minMax.cardinality mustEqual 0
      }

      "negatives" >> {
        val minMax3 = newStat[java.lang.Integer]("doubleAttr", observe = false)

        features3.foreach { minMax3.observe }

        minMax3.bounds mustEqual (-100d, -1d)
        minMax3.cardinality must beCloseTo(100L, 5)
      }
    }

    "work with dates" >> {
      "be empty initiallly" >> {
        val minMax = newStat[Date]("dtg", observe = false)
        minMax.property mustEqual "dtg"
        minMax.isEmpty must beTrue
        minMax.cardinality mustEqual 0
      }

      "observe correct values" >> {
        val minMax = newStat[Date]("dtg")
        minMax.bounds mustEqual (
          java.util.Date.from(java.time.LocalDateTime.parse("2012-01-01T00:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC)),
          java.util.Date.from(java.time.LocalDateTime.parse("2012-01-01T23:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC))
        )
        minMax.cardinality must beCloseTo(24L, 5)
      }

      "serialize to json" >> {
        val minMax = newStat[Date]("dtg")
        minMax.toJson must beMatching("""\{ "min": "2012-01-01T00:00:00.000Z", "max": "2012-01-01T23:00:00.000Z", "cardinality": \d+ \}""" ignoreSpace)
      }

      "serialize empty to json" >> {
        val minMax = newStat[Date]("dtg", observe = false)
        minMax.toJson must beEqualTo("""{ "min": null, "max": null, "cardinality": 0 }""").ignoreSpace
      }

      "serialize and deserialize" >> {
        val minMax = newStat[Date]("dtg")
        val packed = StatSerializer(sft).serialize(minMax)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual minMax.toJson
      }

      "serialize and deserialize empty MinMax" >> {
        val minMax = newStat[Date]("dtg", observe = false)
        val packed = StatSerializer(sft).serialize(minMax)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual minMax.toJson
      }

      "combine two MinMaxes" >> {
        val minMax = newStat[Date]("dtg")
        val minMax2 = newStat[Date]("dtg", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.bounds mustEqual (
          java.util.Date.from(java.time.LocalDateTime.parse("2012-01-02T00:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC)),
          java.util.Date.from(java.time.LocalDateTime.parse("2012-01-02T23:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC))
        )
        minMax2.cardinality must beCloseTo(24L, 5)

        minMax += minMax2

        minMax.bounds mustEqual (
          java.util.Date.from(java.time.LocalDateTime.parse("2012-01-01T00:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC)),
          java.util.Date.from(java.time.LocalDateTime.parse("2012-01-02T23:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC))
        )
        minMax.cardinality must beCloseTo(48L, 5)
        minMax2.bounds mustEqual (
          java.util.Date.from(java.time.LocalDateTime.parse("2012-01-02T00:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC)),
          java.util.Date.from(java.time.LocalDateTime.parse("2012-01-02T23:00:00.000Z", GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC))
        )
      }

      "clear" >> {
        val minMax = newStat[Date]("dtg")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.isEmpty must beTrue
        minMax.cardinality mustEqual 0
      }
    }

    "work with geometries" >> {
      "be empty initiallly" >> {
        val minMax = newStat[Geometry]("geom", observe = false)
        minMax.property mustEqual "geom"
        minMax.isEmpty must beTrue
        minMax.cardinality mustEqual 0
      }

      "observe correct values" >> {
        val minMax = newStat[Geometry]("geom")
        minMax.bounds mustEqual (WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)"))
        minMax.cardinality must beCloseTo(100L, 5)
      }

      "serialize to json" >> {
        val minMax = newStat[Geometry]("geom")
        minMax.toJson must beMatching("""\{ "min": "POINT \(-99 0\)", "max": "POINT \(-0 49\)", "cardinality": \d+ \}""" ignoreSpace)
      }

      "serialize empty to json" >> {
        val minMax = newStat[Geometry]("geom", observe = false)
        minMax.toJson must beEqualTo("""{ "min": null, "max": null, "cardinality": 0 }""").ignoreSpace
      }

      "serialize and deserialize" >> {
        val minMax = newStat[Geometry]("geom")
        val packed = StatSerializer(sft).serialize(minMax)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual minMax.toJson
      }

      "serialize and deserialize empty MinMax" >> {
        val minMax = newStat[Geometry]("geom", observe = false)
        val packed = StatSerializer(sft).serialize(minMax)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual minMax.toJson
      }

      "combine two MinMaxes" >> {
        val minMax = newStat[Geometry]("geom")
        val minMax2 = newStat[Geometry]("geom", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.bounds mustEqual (WKTUtils.read("POINT (80 30)"), WKTUtils.read("POINT (179 79)"))
        minMax2.cardinality must beCloseTo(100L, 5)

        minMax += minMax2

        minMax.bounds mustEqual (WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (179 79)"))
        minMax.cardinality must beCloseTo(200L, 5)
        minMax2.bounds mustEqual (WKTUtils.read("POINT (80 30)"), WKTUtils.read("POINT (179 79)"))
      }

      "combine two MinMaxes2" >> {
        val minMax = newStat[Geometry]("geom")
        val minMax2 = newStat[Geometry]("geom", observe = false)

        minMax.bounds mustEqual (WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)"))
        minMax.cardinality must beCloseTo(100L, 5)

        minMax += minMax2

        minMax.bounds mustEqual (WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)"))
        minMax.cardinality must beCloseTo(100L, 5)
      }

      "combine two MinMaxes3" >> {
        val minMax = newStat[Geometry]("geom")
        val minMax2 = newStat[Geometry]("geom", observe = false)

        minMax.bounds mustEqual (WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)"))
        minMax.cardinality must beCloseTo(100L, 5)

        minMax2 += minMax

        minMax.bounds mustEqual (WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)"))
        minMax2.bounds mustEqual (WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)"))
        minMax2.cardinality must beCloseTo(100L, 5)
      }

      "combine empty MinMaxes" >> {
        val minMax = newStat[Geometry]("geom")
        val minMax2 = newStat[Geometry]("geom", observe = false)

        minMax.bounds mustEqual (WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)"))
        minMax.cardinality must beCloseTo(100L, 5)

        val addRight = minMax2 + minMax
        addRight.bounds mustEqual (WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)"))
        addRight.cardinality must beCloseTo(100L, 5)

        val addLeft = minMax + minMax2
        addLeft.bounds mustEqual (WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)"))
        addLeft.cardinality must beCloseTo(100L, 5)

        minMax.bounds mustEqual (WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)"))
        minMax.cardinality must beCloseTo(100L, 5)
      }

      "clear" >> {
        val minMax = newStat[Geometry]("geom")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.isEmpty must beTrue
        minMax.cardinality mustEqual 0
      }
    }
  }
}
