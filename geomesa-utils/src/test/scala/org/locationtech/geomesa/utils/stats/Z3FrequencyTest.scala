/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import org.junit.runner.RunWith
import org.locationtech.geomesa.curve.TimePeriod
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Z3FrequencyTest extends Specification with StatTestHelper {

  def createStat(precision: Int, observe: Boolean): Z3Frequency = {
    val s = Stat(sft, Stat.Z3Frequency("geom", "dtg", TimePeriod.Week, precision))
    if (observe) {
      features.foreach { s.observe }
    }
    s.asInstanceOf[Z3Frequency]
  }

  def createStat(observe: Boolean = true): Z3Frequency = createStat(25, observe)

  def toDate(string: String) = java.util.Date.from(java.time.LocalDateTime.parse(string, GeoToolsDateFormat).toInstant(java.time.ZoneOffset.UTC))
  def toGeom(string: String) = WKTUtils.read(string)

  "FrequencyZ3 stat" should {

    "work with geometries and dates" >> {
      "be empty initially" >> {
        val stat = createStat(observe = false)
        stat.isEmpty must beTrue
        stat.size mustEqual 0
      }

      "correctly bin values"  >> {
        val stat = createStat()
        stat.isEmpty must beFalse
        stat.size mustEqual 100
        forall(0 until 100) { i =>
          stat.count(toGeom(s"POINT(-$i ${i / 2})"), toDate(f"2012-01-01T${i%24}%02d:00:00.000Z")) must beBetween(1L, 6L)
        }
      }

      "serialize and deserialize" >> {
        val stat = createStat()
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Z3Frequency]
        unpacked.asInstanceOf[Z3Frequency].geom mustEqual stat.geom
        unpacked.asInstanceOf[Z3Frequency].dtg mustEqual stat.dtg
        unpacked.asInstanceOf[Z3Frequency].precision mustEqual stat.precision
        unpacked.asInstanceOf[Z3Frequency].toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = createStat(observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Z3Frequency]
        unpacked.asInstanceOf[Z3Frequency].geom mustEqual stat.geom
        unpacked.asInstanceOf[Z3Frequency].dtg mustEqual stat.dtg
        unpacked.asInstanceOf[Z3Frequency].precision mustEqual stat.precision
        unpacked.asInstanceOf[Z3Frequency].toJson mustEqual stat.toJson
      }


      "deserialize as immutable value" >> {
        val stat = createStat()
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed, immutable = true)

        unpacked must beAnInstanceOf[Z3Frequency]
        unpacked.asInstanceOf[Z3Frequency].geom mustEqual stat.geom
        unpacked.asInstanceOf[Z3Frequency].dtg mustEqual stat.dtg
        unpacked.asInstanceOf[Z3Frequency].precision mustEqual stat.precision
        unpacked.asInstanceOf[Z3Frequency].toJson mustEqual stat.toJson

        unpacked.clear must throwAn[Exception]
        unpacked.+=(stat) must throwAn[Exception]
        unpacked.observe(features.head) must throwAn[Exception]
        unpacked.unobserve(features.head) must throwAn[Exception]
      }

      "clear" >> {
        val stat = createStat()
        stat.clear()

        stat.isEmpty must beTrue
        stat.size mustEqual 0
        forall(0 until 100) { i =>
          stat.count(toGeom(s"POINT(-$i ${i / 2})"), toDate(f"2012-01-01T${i%24}%02d:00:00.000Z")) mustEqual 0
        }
        stat.count(toGeom("POINT(-180 -90)"), toDate("2012-01-01T00:00:00.000Z")) mustEqual 0
      }
    }
  }
}
