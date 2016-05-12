/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geotools.GeoToolsDateFormat
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class Z3RangeHistogramTest extends Specification with StatTestHelper {

  def createStat(length: Int, observe: Boolean): Z3RangeHistogram = {
    val s = Stat(sft, Stat.Z3RangeHistogram("geom", "dtg", length))
    if (observe) {
      features.foreach { s.observe }
    }
    s.asInstanceOf[Z3RangeHistogram]
  }

  def createStat(observe: Boolean = true): Z3RangeHistogram = createStat(1024, observe)

  def toDate(string: String) = GeoToolsDateFormat.parseDateTime(string).toDate
  def toGeom(string: String) = WKTUtils.read(string)

  "FrequencyZ3 stat" should {

    "work with geometries and dates" >> {
      "be empty initially" >> {
        val stat = createStat(observe = false)
        stat.isEmpty must beTrue
      }

      "correctly bin values"  >> {
        val stat = createStat()
        stat.isEmpty must beFalse
        forall(0 until 100) { i =>
          val (w, idx) = stat.indexOf(toGeom(s"POINT(-$i ${i / 2})"), toDate(f"2012-01-01T${i%24}%02d:00:00.000Z"))
          stat.count(w, idx) must beBetween(1L, 12L)
        }
      }

      "serialize and deserialize" >> {
        val stat = createStat()
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Z3RangeHistogram]
        unpacked.asInstanceOf[Z3RangeHistogram].geomIndex mustEqual stat.geomIndex
        unpacked.asInstanceOf[Z3RangeHistogram].dtgIndex mustEqual stat.dtgIndex
        unpacked.asInstanceOf[Z3RangeHistogram].length mustEqual stat.length
        unpacked.asInstanceOf[Z3RangeHistogram].toJson mustEqual stat.toJson
      }

      "serialize and deserialize empty stats" >> {
        val stat = createStat(observe = false)
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed)

        unpacked must beAnInstanceOf[Z3RangeHistogram]
        unpacked.asInstanceOf[Z3RangeHistogram].geomIndex mustEqual stat.geomIndex
        unpacked.asInstanceOf[Z3RangeHistogram].dtgIndex mustEqual stat.dtgIndex
        unpacked.asInstanceOf[Z3RangeHistogram].length mustEqual stat.length
        unpacked.asInstanceOf[Z3RangeHistogram].toJson mustEqual stat.toJson
      }

      "deserialize as immutable value" >> {
        val stat = createStat()
        val packed   = StatSerializer(sft).serialize(stat)
        val unpacked = StatSerializer(sft).deserialize(packed, immutable = true)

        unpacked must beAnInstanceOf[Z3RangeHistogram]
        unpacked.asInstanceOf[Z3RangeHistogram].geomIndex mustEqual stat.geomIndex
        unpacked.asInstanceOf[Z3RangeHistogram].dtgIndex mustEqual stat.dtgIndex
        unpacked.asInstanceOf[Z3RangeHistogram].length mustEqual stat.length
        unpacked.asInstanceOf[Z3RangeHistogram].toJson mustEqual stat.toJson

        unpacked.clear must throwAn[Exception]
        unpacked.+=(stat) must throwAn[Exception]
        unpacked.observe(features.head) must throwAn[Exception]
        unpacked.unobserve(features.head) must throwAn[Exception]
      }

      "clear" >> {
        val stat = createStat()
        stat.clear()

        stat.isEmpty must beTrue
        forall(0 until 100) { i =>
          val (w, idx) = stat.indexOf(toGeom(s"POINT(-$i ${i / 2})"), toDate(f"2012-01-01T${i%24}%02d:00:00.000Z"))
          stat.count(w, idx) mustEqual 0
        }
        val (w, idx) = stat.indexOf(toGeom("POINT(-180 -90)"), toDate("2012-01-01T00:00:00.000Z"))
        stat.count(w, idx) mustEqual 0
      }
    }
  }
}
