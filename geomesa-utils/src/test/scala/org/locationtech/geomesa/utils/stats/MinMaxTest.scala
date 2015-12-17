/***********************************************************************
  * Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
  * All rights reserved. This program and the accompanying materials
  * are made available under the terms of the Apache License, Version 2.0 which
  * accompanies this distribution and is available at
  * http://www.opensource.org/licenses/apache2.0.php.
  *************************************************************************/

package org.locationtech.geomesa.utils.stats

import java.util.Date

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MinMaxTest extends Specification with StatTestHelper {
  sequential

  "MinMax stat" should {
    "work with" in {
      "dates" in {
        val stat = Stat(sft, "MinMax(dtg)")
        val minMax = stat.asInstanceOf[MinMax[Date]]

        minMax.attrIndex mustEqual dateIndex
        minMax.attrType mustEqual "java.util.Date"
        minMax.min mustEqual new Date(java.lang.Long.MAX_VALUE)
        minMax.max mustEqual new Date(java.lang.Long.MIN_VALUE)
        minMax.isEmpty must beFalse

        features.foreach { stat.observe }

        minMax.min mustEqual StatHelpers.dateFormat.parseDateTime("2012-01-01T00:00:00.000Z").toDate
        minMax.max mustEqual StatHelpers.dateFormat.parseDateTime("2012-01-01T23:00:00.000Z").toDate

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(minMax)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[MinMax[Date]]

          unpacked mustEqual minMax
        }

        "combine two MinMaxes" in {
          val stat2 = Stat(sft, "MinMax(dtg)")
          val minMax2 = stat2.asInstanceOf[MinMax[Date]]

          features2.foreach { stat2.observe }

          minMax2.min mustEqual StatHelpers.dateFormat.parseDateTime("2012-01-02T00:00:00.000Z").toDate
          minMax2.max mustEqual StatHelpers.dateFormat.parseDateTime("2012-01-02T23:00:00.000Z").toDate

          stat.add(stat2)

          minMax.min mustEqual StatHelpers.dateFormat.parseDateTime("2012-01-01T00:00:00.000Z").toDate
          minMax.max mustEqual StatHelpers.dateFormat.parseDateTime("2012-01-02T23:00:00.000Z").toDate
          minMax2.min mustEqual StatHelpers.dateFormat.parseDateTime("2012-01-02T00:00:00.000Z").toDate
          minMax2.max mustEqual StatHelpers.dateFormat.parseDateTime("2012-01-02T23:00:00.000Z").toDate

          "clear them" in {
            minMax.isEmpty must beFalse
            minMax2.isEmpty must beFalse

            minMax.clear()
            minMax2.clear()

            minMax.min mustEqual new Date(java.lang.Long.MAX_VALUE)
            minMax.max mustEqual new Date(java.lang.Long.MIN_VALUE)
            minMax2.min mustEqual new Date(java.lang.Long.MAX_VALUE)
            minMax2.max mustEqual new Date(java.lang.Long.MIN_VALUE)
          }
        }
      }

      "integers" in {
        val stat = Stat(sft, "MinMax(intAttr)")
        val minMax = stat.asInstanceOf[MinMax[java.lang.Integer]]

        minMax.attrIndex mustEqual intIndex
        minMax.attrType mustEqual "java.lang.Integer"
        minMax.min mustEqual java.lang.Integer.MAX_VALUE
        minMax.max mustEqual java.lang.Integer.MIN_VALUE
        minMax.isEmpty must beFalse

        features.foreach { stat.observe }

        minMax.min mustEqual 0
        minMax.max mustEqual 99

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(minMax)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[MinMax[Integer]]

          unpacked mustEqual minMax
        }

        "combine two MinMaxes" in {
          val stat2 = Stat(sft, "MinMax(intAttr)")
          val minMax2 = stat2.asInstanceOf[MinMax[Integer]]

          features2.foreach { stat2.observe }

          minMax2.min mustEqual 100
          minMax2.max mustEqual 199

          stat.add(stat2)

          minMax.min mustEqual 0
          minMax.max mustEqual 199
          minMax2.min mustEqual 100
          minMax2.max mustEqual 199

          "clear them" in {
            minMax.isEmpty must beFalse
            minMax2.isEmpty must beFalse

            minMax.clear()
            minMax2.clear()

            minMax.min mustEqual java.lang.Integer.MAX_VALUE
            minMax.max mustEqual java.lang.Integer.MIN_VALUE
            minMax2.min mustEqual java.lang.Integer.MAX_VALUE
            minMax2.max mustEqual java.lang.Integer.MIN_VALUE
          }
        }
      }

      "longs" in {
        val stat = Stat(sft, "MinMax(longAttr)")
        val minMax = stat.asInstanceOf[MinMax[java.lang.Long]]

        minMax.attrIndex mustEqual longIndex
        minMax.attrType mustEqual "java.lang.Long"
        minMax.min mustEqual java.lang.Long.MAX_VALUE
        minMax.max mustEqual java.lang.Long.MIN_VALUE
        minMax.isEmpty must beFalse

        features.foreach { stat.observe }

        minMax.min mustEqual 0L
        minMax.max mustEqual 99L

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(minMax)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[MinMax[java.lang.Long]]

          unpacked mustEqual minMax
        }

        "combine two MinMaxes" in {
          val stat2 = Stat(sft, "MinMax(longAttr)")
          val minMax2 = stat2.asInstanceOf[MinMax[java.lang.Long]]

          features2.foreach { stat2.observe }

          minMax2.min mustEqual 100L
          minMax2.max mustEqual 199L

          stat.add(stat2)

          minMax.min mustEqual 0L
          minMax.max mustEqual 199L
          minMax2.min mustEqual 100L
          minMax2.max mustEqual 199L

          "clear them" in {
            minMax.isEmpty must beFalse
            minMax2.isEmpty must beFalse

            minMax.clear()
            minMax2.clear()

            minMax.min mustEqual java.lang.Long.MAX_VALUE
            minMax.max mustEqual java.lang.Long.MIN_VALUE
            minMax2.min mustEqual java.lang.Long.MAX_VALUE
            minMax2.max mustEqual java.lang.Long.MIN_VALUE
          }
        }
      }

      "doubles" in {
        val stat = Stat(sft, "MinMax(doubleAttr)")
        val minMax = stat.asInstanceOf[MinMax[java.lang.Double]]

        minMax.attrIndex mustEqual doubleIndex
        minMax.attrType mustEqual "java.lang.Double"
        minMax.min mustEqual java.lang.Double.MAX_VALUE
        minMax.max mustEqual java.lang.Double.MIN_VALUE
        minMax.isEmpty must beFalse

        features.foreach { stat.observe }

        minMax.min mustEqual 0
        minMax.max mustEqual 99

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(minMax)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[MinMax[java.lang.Double]]

          unpacked mustEqual minMax
        }

        "combine two MinMaxes" in {
          val stat2 = Stat(sft, "MinMax(doubleAttr)")
          val minMax2 = stat2.asInstanceOf[MinMax[java.lang.Double]]

          features2.foreach { stat2.observe }

          minMax2.min mustEqual 100
          minMax2.max mustEqual 199

          stat.add(stat2)

          minMax.min mustEqual 0
          minMax.max mustEqual 199
          minMax2.min mustEqual 100
          minMax2.max mustEqual 199

          "clear them" in {
            minMax.isEmpty must beFalse
            minMax2.isEmpty must beFalse

            minMax.clear()
            minMax2.clear()

            minMax.min mustEqual java.lang.Double.MAX_VALUE
            minMax.max mustEqual java.lang.Double.MIN_VALUE
            minMax2.min mustEqual java.lang.Double.MAX_VALUE
            minMax2.max mustEqual java.lang.Double.MIN_VALUE
          }
        }
      }

      "floats" in {
        val stat = Stat(sft, "MinMax(floatAttr)")
        val minMax = stat.asInstanceOf[MinMax[java.lang.Float]]

        minMax.attrIndex mustEqual floatIndex
        minMax.attrType mustEqual "java.lang.Float"
        minMax.min mustEqual java.lang.Float.MAX_VALUE
        minMax.max mustEqual java.lang.Float.MIN_VALUE
        minMax.isEmpty must beFalse

        features.foreach { stat.observe }

        minMax.min mustEqual 0
        minMax.max mustEqual 99

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(minMax)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[MinMax[java.lang.Float]]

          unpacked mustEqual minMax
        }

        "combine two MinMaxes" in {
          val stat2 = Stat(sft, "MinMax(floatAttr)")
          val minMax2 = stat2.asInstanceOf[MinMax[java.lang.Float]]

          features2.foreach { stat2.observe }

          minMax2.min mustEqual 100
          minMax2.max mustEqual 199

          stat.add(stat2)

          minMax.min mustEqual 0
          minMax.max mustEqual 199
          minMax2.min mustEqual 100
          minMax2.max mustEqual 199

          "clear them" in {
            minMax.isEmpty must beFalse
            minMax2.isEmpty must beFalse

            minMax.clear()
            minMax2.clear()

            minMax.min mustEqual java.lang.Float.MAX_VALUE
            minMax.max mustEqual java.lang.Float.MIN_VALUE
            minMax2.min mustEqual java.lang.Float.MAX_VALUE
            minMax2.max mustEqual java.lang.Float.MIN_VALUE
          }
        }
      }
    }
  }
}
