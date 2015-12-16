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
class EnumeratedHistogramTest extends Specification with StatTestHelper {
  sequential

  "EnumeratedHistogram stat" should {
    "work with" in {
      "dates" in {
        val stat = Stat(sft, "EnumeratedHistogram(dtg)")
        val eh = stat.asInstanceOf[EnumeratedHistogram[Date]]

        val date1 = StatHelpers.dateFormat.parseDateTime("2012-01-01T00:00:00.000Z").toDate
        val date2 = StatHelpers.dateFormat.parseDateTime("2012-01-01T23:00:00.000Z").toDate

        eh.frequencyMap.size mustEqual 0
        eh.isEmpty must beTrue

        features.foreach { stat.observe }

        eh.frequencyMap.size mustEqual 24
        eh.frequencyMap(date1) mustEqual 5
        eh.frequencyMap(date2) mustEqual 4

        features2.foreach { stat.observe }

        eh.frequencyMap.size mustEqual 48
        eh.frequencyMap(date1) mustEqual 5
        eh.frequencyMap(date2) mustEqual 4

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(eh)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[EnumeratedHistogram[Date]]

          unpacked mustEqual eh
        }

        "combine two EnumeratedHistograms" in {
          val stat2 = Stat(sft, "EnumeratedHistogram(dtg)")
          val eh2 = stat2.asInstanceOf[EnumeratedHistogram[Date]]

          features.foreach { stat2.observe }

          stat.add(stat2)

          eh.frequencyMap.size mustEqual 48
          eh.frequencyMap(date1) mustEqual 10
          eh.frequencyMap(date2) mustEqual 8
          eh2.frequencyMap.size mustEqual 24
          eh2.frequencyMap(date1) mustEqual 5
          eh2.frequencyMap(date2) mustEqual 4

          "clear them" in {
            eh.isEmpty must beFalse
            eh2.isEmpty must beFalse

            eh.clear()
            eh2.clear()

            eh.isEmpty must beTrue
            eh2.isEmpty must beTrue
          }
        }
      }

      "integers" in {
        val stat = Stat(sft, "EnumeratedHistogram(intAttr)")
        val eh = stat.asInstanceOf[EnumeratedHistogram[java.lang.Integer]]

        eh.frequencyMap.size mustEqual 0
        eh.isEmpty must beTrue

        features.foreach { stat.observe }

        eh.frequencyMap.size mustEqual 100
        eh.frequencyMap(1) mustEqual 1

        features2.foreach { stat.observe }

        eh.frequencyMap.size mustEqual 200
        eh.frequencyMap(1) mustEqual 1

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(eh)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[EnumeratedHistogram[java.lang.Integer]]

          unpacked mustEqual eh
        }

        "combine two EnumeratedHistograms" in {
          val stat2 = Stat(sft, "EnumeratedHistogram(intAttr)")
          val eh2 = stat2.asInstanceOf[EnumeratedHistogram[java.lang.Integer]]

          features.foreach { stat2.observe }

          stat.add(stat2)

          eh.frequencyMap.size mustEqual 200
          eh.frequencyMap(1) mustEqual 2
          eh2.frequencyMap.size mustEqual 100
          eh2.frequencyMap(1) mustEqual 1

          "clear them" in {
            eh.isEmpty must beFalse
            eh2.isEmpty must beFalse

            eh.clear()
            eh2.clear()

            eh.isEmpty must beTrue
            eh2.isEmpty must beTrue
          }
        }
      }

      "longs" in {
        val stat = Stat(sft, "EnumeratedHistogram(longAttr)")
        val eh = stat.asInstanceOf[EnumeratedHistogram[java.lang.Long]]

        eh.frequencyMap.size mustEqual 0
        eh.isEmpty must beTrue

        features.foreach { stat.observe }

        eh.frequencyMap.size mustEqual 100
        eh.frequencyMap(1L) mustEqual 1

        features2.foreach { stat.observe }

        eh.frequencyMap.size mustEqual 200
        eh.frequencyMap(1L) mustEqual 1

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(eh)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[EnumeratedHistogram[java.lang.Long]]

          unpacked mustEqual eh
        }

        "combine two EnumeratedHistograms" in {
          val stat2 = Stat(sft, "EnumeratedHistogram(longAttr)")
          val eh2 = stat2.asInstanceOf[EnumeratedHistogram[java.lang.Long]]

          features.foreach { stat2.observe }

          stat.add(stat2)

          eh.frequencyMap.size mustEqual 200
          eh.frequencyMap(1L) mustEqual 2
          eh2.frequencyMap.size mustEqual 100
          eh2.frequencyMap(1L) mustEqual 1

          "clear them" in {
            eh.isEmpty must beFalse
            eh2.isEmpty must beFalse

            eh.clear()
            eh2.clear()

            eh.isEmpty must beTrue
            eh2.isEmpty must beTrue
          }
        }
      }

      "doubles" in {
        val stat = Stat(sft, "EnumeratedHistogram(doubleAttr)")
        val eh = stat.asInstanceOf[EnumeratedHistogram[java.lang.Double]]

        eh.frequencyMap.size mustEqual 0
        eh.isEmpty must beTrue

        features.foreach { stat.observe }

        eh.frequencyMap.size mustEqual 100
        eh.frequencyMap(1.0) mustEqual 1

        features2.foreach { stat.observe }

        eh.frequencyMap.size mustEqual 200
        eh.frequencyMap(1.0) mustEqual 1

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(eh)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[EnumeratedHistogram[java.lang.Double]]

          unpacked mustEqual eh
        }

        "combine two EnumeratedHistograms" in {
          val stat2 = Stat(sft, "EnumeratedHistogram(doubleAttr)")
          val eh2 = stat2.asInstanceOf[EnumeratedHistogram[java.lang.Double]]

          features.foreach { stat2.observe }

          stat.add(stat2)

          eh.frequencyMap.size mustEqual 200
          eh.frequencyMap(1.0) mustEqual 2
          eh2.frequencyMap.size mustEqual 100
          eh2.frequencyMap(1.0) mustEqual 1

          "clear them" in {
            eh.isEmpty must beFalse
            eh2.isEmpty must beFalse

            eh.clear()
            eh2.clear()

            eh.isEmpty must beTrue
            eh2.isEmpty must beTrue
          }
        }
      }

      "floats" in {
        val stat = Stat(sft, "EnumeratedHistogram(floatAttr)")
        val eh = stat.asInstanceOf[EnumeratedHistogram[java.lang.Float]]

        eh.frequencyMap.size mustEqual 0
        eh.isEmpty must beTrue

        features.foreach { stat.observe }

        eh.frequencyMap.size mustEqual 100
        eh.frequencyMap(1.0f) mustEqual 1

        features2.foreach { stat.observe }

        eh.frequencyMap.size mustEqual 200
        eh.frequencyMap(1.0f) mustEqual 1

        "serialize and deserialize" in {
          val packed   = StatSerialization.pack(eh)
          val unpacked = StatSerialization.unpack(packed).asInstanceOf[EnumeratedHistogram[java.lang.Float]]

          unpacked mustEqual eh
        }

        "combine two EnumeratedHistograms" in {
          val stat2 = Stat(sft, "EnumeratedHistogram(floatAttr)")
          val eh2 = stat2.asInstanceOf[EnumeratedHistogram[java.lang.Float]]

          features.foreach { stat2.observe }

          stat.add(stat2)

          eh.frequencyMap.size mustEqual 200
          eh.frequencyMap(1.0f) mustEqual 2
          eh2.frequencyMap.size mustEqual 100
          eh2.frequencyMap(1.0f) mustEqual 1

          "clear them" in {
            eh.isEmpty must beFalse
            eh2.isEmpty must beFalse

            eh.clear()
            eh2.clear()

            eh.isEmpty must beTrue
            eh2.isEmpty must beTrue
          }
        }
      }
    }
  }
}
