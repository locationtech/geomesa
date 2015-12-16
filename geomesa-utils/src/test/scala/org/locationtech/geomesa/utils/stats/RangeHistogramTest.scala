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
class RangeHistogramTest extends Specification with StatTestHelper {
  sequential

  "RangeHistogram stat" should {
    "create RangeHistogram stats for" in {
      "dates" in {
        val stat = Stat(sft, "RangeHistogram(dtg,24,2012-01-01T00:00:00.000Z,2012-01-03T00:00:00.000Z)")
        val rh = stat.asInstanceOf[RangeHistogram[Date]]
        val lowerEndpoint = StatHelpers.dateFormat.parseDateTime("2012-01-01T00:00:00.000Z").toDate
        val midpoint = StatHelpers.dateFormat.parseDateTime("2012-01-02T00:00:00.000Z").toDate

        rh.isEmpty must beTrue

        features.foreach { stat.observe }

        rh.histogram.size mustEqual 24
        rh.histogram(lowerEndpoint) mustEqual 10
        rh.histogram(midpoint) mustEqual 0

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(rh)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[RangeHistogram[Date]]

          unpacked mustEqual rh
        }

        "combine two RangeHistograms" in {
          val stat2 = Stat(sft, "RangeHistogram(dtg,24,2012-01-01T00:00:00.000Z,2012-01-03T00:00:00.000Z)")
          val rh2 = stat2.asInstanceOf[RangeHistogram[Date]]

          features2.foreach { stat2.observe }

          rh2.histogram.size mustEqual 24
          rh2.histogram(lowerEndpoint) mustEqual 0
          rh2.histogram(midpoint) mustEqual 8

          stat.add(stat2)

          rh.histogram.size mustEqual 24
          rh.histogram(lowerEndpoint) mustEqual 10
          rh.histogram(midpoint) mustEqual 8
          rh2.histogram.size mustEqual 24
          rh2.histogram(lowerEndpoint) mustEqual 0
          rh2.histogram(midpoint) mustEqual 8

          "clear them" in {
            rh.isEmpty must beFalse
            rh2.isEmpty must beFalse

            rh.clear()
            rh2.clear()

            rh.isEmpty must beTrue
            rh2.isEmpty must beTrue

            rh.histogram.size mustEqual 24
            rh.histogram(lowerEndpoint) mustEqual 0
            rh.histogram(midpoint) mustEqual 0
            rh2.histogram.size mustEqual 24
            rh2.histogram(lowerEndpoint) mustEqual 0
            rh2.histogram(midpoint) mustEqual 0
          }
        }
      }

      "integers" in {
        val stat = Stat(sft, "RangeHistogram(intAttr,20,0,200)")
        val rh = stat.asInstanceOf[RangeHistogram[java.lang.Integer]]
        val lowerEndpoint = 0
        val midpoint = 100

        rh.isEmpty must beTrue

        features.foreach { stat.observe }

        rh.histogram.size mustEqual 20
        rh.histogram(lowerEndpoint) mustEqual 10
        rh.histogram(midpoint) mustEqual 0

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(rh)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[RangeHistogram[java.lang.Integer]]

          unpacked mustEqual rh
        }

        "combine two RangeHistograms" in {
          val stat2 = Stat(sft, "RangeHistogram(intAttr,20,0,200)")
          val rh2 = stat2.asInstanceOf[RangeHistogram[java.lang.Integer]]

          features2.foreach { stat2.observe }

          rh2.histogram.size mustEqual 20
          rh2.histogram(lowerEndpoint) mustEqual 0
          rh2.histogram(midpoint) mustEqual 10

          stat.add(stat2)

          rh.histogram.size mustEqual 20
          rh.histogram(lowerEndpoint) mustEqual 10
          rh.histogram(midpoint) mustEqual 10
          rh2.histogram.size mustEqual 20
          rh2.histogram(lowerEndpoint) mustEqual 0
          rh2.histogram(midpoint) mustEqual 10

          "clear them" in {
            rh.isEmpty must beFalse
            rh2.isEmpty must beFalse

            rh.clear()
            rh2.clear()

            rh.isEmpty must beTrue
            rh2.isEmpty must beTrue

            rh.histogram.size mustEqual 20
            rh.histogram(lowerEndpoint) mustEqual 0
            rh.histogram(midpoint) mustEqual 0
            rh2.histogram.size mustEqual 20
            rh2.histogram(lowerEndpoint) mustEqual 0
            rh2.histogram(midpoint) mustEqual 0
          }
        }
      }

      "longs" in {
        val stat = Stat(sft, "RangeHistogram(longAttr,7,90,110)")
        val rh = stat.asInstanceOf[RangeHistogram[java.lang.Long]]
        val lowerEndpoint = 90L
        val midpoint = 96L
        val upperEndpoint = 102L

        rh.isEmpty must beTrue

        features.foreach { stat.observe }

        rh.histogram.size mustEqual 7
        rh.histogram(lowerEndpoint) mustEqual 2L
        rh.histogram(midpoint) mustEqual 2L
        rh.histogram(upperEndpoint) mustEqual 0L

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(rh)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[RangeHistogram[java.lang.Long]]

          unpacked mustEqual rh
        }

        "combine two RangeHistograms" in {
          val stat2 = Stat(sft, "RangeHistogram(longAttr,7,90,110)")
          val rh2 = stat2.asInstanceOf[RangeHistogram[java.lang.Long]]

          features2.foreach { stat2.observe }

          rh2.histogram.size mustEqual 7
          rh2.histogram(lowerEndpoint) mustEqual 0L
          rh2.histogram(midpoint) mustEqual 0L
          rh2.histogram(upperEndpoint) mustEqual 8L

          stat.add(stat2)

          rh.histogram.size mustEqual 7
          rh.histogram(lowerEndpoint) mustEqual 2L
          rh.histogram(midpoint) mustEqual 2L
          rh.histogram(upperEndpoint) mustEqual 8L
          rh2.histogram.size mustEqual 7
          rh2.histogram(lowerEndpoint) mustEqual 0L
          rh2.histogram(midpoint) mustEqual 0L
          rh2.histogram(upperEndpoint) mustEqual 8L

          "clear them" in {
            rh.isEmpty must beFalse
            rh2.isEmpty must beFalse

            rh.clear()
            rh2.clear()

            rh.isEmpty must beTrue
            rh2.isEmpty must beTrue

            rh.histogram.size mustEqual 7
            rh.histogram(lowerEndpoint) mustEqual 0
            rh.histogram(midpoint) mustEqual 0
            rh2.histogram.size mustEqual 7
            rh2.histogram(lowerEndpoint) mustEqual 0
            rh2.histogram(midpoint) mustEqual 0
          }
        }
      }

      "doubles" in {
        val stat = Stat(sft, "RangeHistogram(doubleAttr,7,90,110)")
        val rh = stat.asInstanceOf[RangeHistogram[java.lang.Double]]
        val lowerEndpoint = 90.0
        val midpoint = 98.57142857142857
        val upperEndpoint = 107.14285714285714

        rh.isEmpty must beTrue

        features.foreach { stat.observe }

        rh.histogram.size mustEqual 7
        rh.histogram(lowerEndpoint) mustEqual 3
        rh.histogram(midpoint) mustEqual 1
        rh.histogram(upperEndpoint) mustEqual 0

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(rh)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[RangeHistogram[java.lang.Double]]

          unpacked mustEqual rh
        }

        "combine two RangeHistograms" in {
          val stat2 = Stat(sft, "RangeHistogram(doubleAttr,7,90,110)")
          val rh2 = stat2.asInstanceOf[RangeHistogram[java.lang.Double]]

          features2.foreach { stat2.observe }

          rh2.histogram.size mustEqual 7
          rh2.histogram(lowerEndpoint) mustEqual 0
          rh2.histogram(midpoint) mustEqual 2
          rh2.histogram(upperEndpoint) mustEqual 2

          stat.add(stat2)

          rh.histogram.size mustEqual 7
          rh.histogram(lowerEndpoint) mustEqual 3
          rh.histogram(midpoint) mustEqual 3
          rh.histogram(upperEndpoint) mustEqual 2
          rh2.histogram.size mustEqual 7
          rh2.histogram(lowerEndpoint) mustEqual 0
          rh2.histogram(midpoint) mustEqual 2
          rh2.histogram(upperEndpoint) mustEqual 2

          "clear them" in {
            rh.isEmpty must beFalse
            rh2.isEmpty must beFalse

            rh.clear()
            rh2.clear()

            rh.isEmpty must beTrue
            rh2.isEmpty must beTrue

            rh.histogram.size mustEqual 7
            rh.histogram(lowerEndpoint) mustEqual 0
            rh.histogram(midpoint) mustEqual 0
            rh2.histogram.size mustEqual 7
            rh2.histogram(lowerEndpoint) mustEqual 0
            rh2.histogram(midpoint) mustEqual 0
          }
        }
      }

      "floats" in {
        val stat = Stat(sft, "RangeHistogram(floatAttr,7,90,110)")
        val rh = stat.asInstanceOf[RangeHistogram[java.lang.Float]]
        val lowerEndpoint = 90.0f
        val midpoint = 98.57143f
        val upperEndpoint = 107.14285f

        rh.isEmpty must beTrue

        features.foreach { stat.observe }

        rh.histogram.size mustEqual 7
        rh.histogram(lowerEndpoint) mustEqual 3
        rh.histogram(midpoint) mustEqual 1
        rh.histogram(upperEndpoint) mustEqual 0

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(rh)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[RangeHistogram[java.lang.Float]]

          unpacked mustEqual rh
        }

        "combine two RangeHistograms" in {
          val stat2 = Stat(sft, "RangeHistogram(floatAttr,7,90,110)")
          val rh2 = stat2.asInstanceOf[RangeHistogram[java.lang.Float]]

          features2.foreach { stat2.observe }

          rh2.histogram.size mustEqual 7
          rh2.histogram(lowerEndpoint) mustEqual 0
          rh2.histogram(midpoint) mustEqual 2
          rh2.histogram(upperEndpoint) mustEqual 2

          stat.add(stat2)

          rh.histogram.size mustEqual 7
          rh.histogram(lowerEndpoint) mustEqual 3
          rh.histogram(midpoint) mustEqual 3
          rh.histogram(upperEndpoint) mustEqual 2
          rh2.histogram.size mustEqual 7
          rh2.histogram(lowerEndpoint) mustEqual 0
          rh2.histogram(midpoint) mustEqual 2
          rh2.histogram(upperEndpoint) mustEqual 2

          "clear them" in {
            rh.isEmpty must beFalse
            rh2.isEmpty must beFalse

            rh.clear()
            rh2.clear()

            rh.isEmpty must beTrue
            rh2.isEmpty must beTrue

            rh.histogram.size mustEqual 7
            rh.histogram(lowerEndpoint) mustEqual 0
            rh.histogram(midpoint) mustEqual 0
            rh2.histogram.size mustEqual 7
            rh2.histogram(lowerEndpoint) mustEqual 0
            rh2.histogram(midpoint) mustEqual 0
          }
        }
      }
    }
  }
}
