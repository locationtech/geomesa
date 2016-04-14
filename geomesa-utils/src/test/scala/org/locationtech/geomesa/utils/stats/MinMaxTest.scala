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
        minMax.attribute mustEqual stringIndex
        minMax.bounds must beNone
        minMax.isEmpty must beTrue
      }

      "observe correct values" >> {
        val minMax = newStat[String]("strAttr")
        minMax.bounds must beSome(("abc000", "abc099"))
      }

      "serialize to json" >> {
        val minMax = newStat[String]("strAttr")
        minMax.toJson mustEqual """{ "min": "abc000", "max": "abc099" }"""
      }

      "serialize empty to json" >> {
        val minMax = newStat[String]("strAttr", observe = false)
        minMax.toJson mustEqual """{ "min": null, "max": null }"""
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

      "combine two MinMaxes" >> {
        val minMax = newStat[String]("strAttr")
        val minMax2 = newStat[String]("strAttr", observe = false)

        features2.foreach { minMax2.observe }

        minMax2.bounds must beSome(("abc100", "abc199"))

        minMax += minMax2

        minMax.bounds must beSome(("abc000", "abc199"))
        minMax2.bounds must beSome(("abc100", "abc199"))
      }

      "clear" >> {
        val minMax = newStat[String]("strAttr")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.bounds must beNone
        minMax.isEmpty must beTrue
      }
    }

    "work with ints" >> {
      "be empty initiallly" >> {
        val minMax = newStat[java.lang.Integer]("intAttr", observe = false)
        minMax.attribute mustEqual intIndex
        minMax.bounds must beNone
        minMax.isEmpty must beTrue
      }

      "observe correct values" >> {
        val minMax = newStat[java.lang.Integer]("intAttr")
        minMax.bounds must beSome((0, 99))
      }

      "serialize to json" >> {
        val minMax = newStat[java.lang.Integer]("intAttr")
        minMax.toJson mustEqual """{ "min": 0, "max": 99 }"""
      }

      "serialize empty to json" >> {
        val minMax = newStat[java.lang.Integer]("intAttr", observe = false)
        minMax.toJson mustEqual """{ "min": null, "max": null }"""
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

        minMax2.bounds must beSome((100, 199))

        minMax += minMax2

        minMax.bounds must beSome((0, 199))
        minMax2.bounds must beSome((100, 199))
      }

      "clear" >> {
        val minMax = newStat[java.lang.Integer]("intAttr")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.bounds must beNone
        minMax.isEmpty must beTrue
      }
    }

    "work with longs" >> {
      "be empty initiallly" >> {
        val minMax = newStat[java.lang.Long]("longAttr", observe = false)
        minMax.attribute mustEqual longIndex
        minMax.bounds must beNone
        minMax.isEmpty must beTrue
      }

      "observe correct values" >> {
        val minMax = newStat[java.lang.Long]("longAttr")
        minMax.bounds must beSome((0L, 99L))
      }

      "serialize to json" >> {
        val minMax = newStat[java.lang.Long]("longAttr")
        minMax.toJson mustEqual """{ "min": 0, "max": 99 }"""
      }

      "serialize empty to json" >> {
        val minMax = newStat[java.lang.Long]("longAttr", observe = false)
        minMax.toJson mustEqual """{ "min": null, "max": null }"""
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

        minMax2.bounds must beSome((100L, 199L))

        minMax += minMax2

        minMax.bounds must beSome((0L, 199L))
        minMax2.bounds must beSome((100L, 199L))
      }

      "clear" >> {
        val minMax = newStat[java.lang.Long]("longAttr")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.bounds must beNone
        minMax.isEmpty must beTrue
      }
    }

    "work with floats" >> {
      "be empty initiallly" >> {
        val minMax = newStat[java.lang.Float]("floatAttr", observe = false)
        minMax.attribute mustEqual floatIndex
        minMax.bounds must beNone
        minMax.isEmpty must beTrue
      }

      "observe correct values" >> {
        val minMax = newStat[java.lang.Float]("floatAttr")
        minMax.bounds must beSome((0f, 99f))
      }

      "serialize to json" >> {
        val minMax = newStat[java.lang.Float]("floatAttr")
        minMax.toJson mustEqual """{ "min": 0.0, "max": 99.0 }"""
      }

      "serialize empty to json" >> {
        val minMax = newStat[java.lang.Float]("floatAttr", observe = false)
        minMax.toJson mustEqual """{ "min": null, "max": null }"""
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

        minMax2.bounds must beSome((100f, 199f))

        minMax += minMax2

        minMax.bounds must beSome((0f, 199f))
        minMax2.bounds must beSome((100f, 199f))
      }

      "clear" >> {
        val minMax = newStat[java.lang.Float]("floatAttr")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.bounds must beNone
        minMax.isEmpty must beTrue
      }
    }

    "work with doubles" >> {
      "be empty initiallly" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr", observe = false)
        minMax.attribute mustEqual doubleIndex
        minMax.bounds must beNone
        minMax.isEmpty must beTrue
      }

      "observe correct values" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr")
        minMax.bounds must beSome((0d, 99d))
      }

      "serialize to json" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr")
        minMax.toJson mustEqual """{ "min": 0.0, "max": 99.0 }"""
      }

      "serialize empty to json" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr", observe = false)
        minMax.toJson mustEqual """{ "min": null, "max": null }"""
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

        minMax2.bounds must beSome((100d, 199d))

        minMax += minMax2

        minMax.bounds must beSome((0d, 199d))
        minMax2.bounds must beSome((100d, 199d))
      }

      "clear" >> {
        val minMax = newStat[java.lang.Double]("doubleAttr")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.bounds must beNone
        minMax.isEmpty must beTrue
      }
    }

    "work with dates" >> {
      "be empty initiallly" >> {
        val minMax = newStat[Date]("dtg", observe = false)
        minMax.attribute mustEqual dateIndex
        minMax.bounds must beNone
        minMax.isEmpty must beTrue
      }

      "observe correct values" >> {
        val minMax = newStat[Date]("dtg")
        minMax.bounds must beSome {(
          GeoToolsDateFormat.parseDateTime("2012-01-01T00:00:00.000Z").toDate,
          GeoToolsDateFormat.parseDateTime("2012-01-01T23:00:00.000Z").toDate
        )}
      }

      "serialize to json" >> {
        val minMax = newStat[Date]("dtg")
        minMax.toJson mustEqual """{ "min": "2012-01-01T00:00:00.000Z", "max": "2012-01-01T23:00:00.000Z" }"""
      }

      "serialize empty to json" >> {
        val minMax = newStat[Date]("dtg", observe = false)
        minMax.toJson mustEqual """{ "min": null, "max": null }"""
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

        minMax2.bounds must beSome {(
          GeoToolsDateFormat.parseDateTime("2012-01-02T00:00:00.000Z").toDate,
          GeoToolsDateFormat.parseDateTime("2012-01-02T23:00:00.000Z").toDate
        )}

        minMax += minMax2

        minMax.bounds must beSome {(
          GeoToolsDateFormat.parseDateTime("2012-01-01T00:00:00.000Z").toDate,
          GeoToolsDateFormat.parseDateTime("2012-01-02T23:00:00.000Z").toDate
        )}
        minMax2.bounds must beSome {(
          GeoToolsDateFormat.parseDateTime("2012-01-02T00:00:00.000Z").toDate,
            GeoToolsDateFormat.parseDateTime("2012-01-02T23:00:00.000Z").toDate
        )}
      }

      "clear" >> {
        val minMax = newStat[Date]("dtg")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.bounds must beNone
        minMax.isEmpty must beTrue
      }
    }

    "work with geometries" >> {
      "be empty initiallly" >> {
        val minMax = newStat[Geometry]("geom", observe = false)
        minMax.attribute mustEqual geomIndex
        minMax.bounds must beNone
        minMax.isEmpty must beTrue
      }

      "observe correct values" >> {
        val minMax = newStat[Geometry]("geom")
        minMax.bounds must beSome((WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)")))
      }

      "serialize to json" >> {
        val minMax = newStat[Geometry]("geom")
        minMax.toJson mustEqual """{ "min": "POINT (-99 0)", "max": "POINT (-0 49)" }"""
      }

      "serialize empty to json" >> {
        val minMax = newStat[Geometry]("geom", observe = false)
        minMax.toJson mustEqual """{ "min": null, "max": null }"""
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

        minMax2.bounds must beSome((WKTUtils.read("POINT (80 30)"), WKTUtils.read("POINT (179 79)")))

        minMax += minMax2

        minMax.bounds must beSome((WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (179 79)")))
        minMax2.bounds must beSome((WKTUtils.read("POINT (80 30)"), WKTUtils.read("POINT (179 79)")))
      }

      "combine two MinMaxes2" >> {
        val minMax = newStat[Geometry]("geom")
        val minMax2 = newStat[Geometry]("geom", observe = false)

        minMax.bounds must beSome((WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)")))
        minMax2.bounds must beNone

        minMax += minMax2

        minMax.bounds must beSome((WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)")))
        minMax2.bounds must beNone
      }

      "combine two MinMaxes3" >> {
        val minMax = newStat[Geometry]("geom")
        val minMax2 = newStat[Geometry]("geom", observe = false)

        minMax.bounds must beSome((WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)")))
        minMax2.bounds must beNone

        minMax2 += minMax

        minMax.bounds must beSome((WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)")))
        minMax2.bounds must beSome((WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)")))
      }

      "combine empty MinMaxes" >> {
        val minMax = newStat[Geometry]("geom")
        val minMax2 = newStat[Geometry]("geom", observe = false)

        minMax.bounds must beSome((WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)")))

        minMax2.bounds must beNone

        val addRight = minMax2 + minMax
        addRight.bounds must beSome((WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)")))

        val addLeft = minMax + minMax2
        addLeft.bounds must beSome((WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)")))

        minMax.bounds must beSome((WKTUtils.read("POINT (-99 0)"), WKTUtils.read("POINT (0 49)")))

        minMax2.bounds must beNone
      }

      "clear" >> {
        val minMax = newStat[Geometry]("geom")
        minMax.isEmpty must beFalse

        minMax.clear()

        minMax.bounds must beNone
        minMax.isEmpty must beTrue
      }
    }
  }
}
