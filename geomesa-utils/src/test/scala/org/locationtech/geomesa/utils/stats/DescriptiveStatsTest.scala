/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DescriptiveStatsTest extends Specification with StatTestHelper {

  def newStat[T <: Number](attribute: String, observe: Boolean = true): DescriptiveStats = {
    val stat = Stat(sft, s"DescriptiveStats($attribute)")
    if (observe) {
      features.foreach { stat.observe }
    }
    stat.asInstanceOf[DescriptiveStats]
  }

  // NOTE:  This is pattern.
  val JSON_0_to_100 =
    """\{
        |  "count": 100,
        |  "minimum": \[
        |    0.0
        |  \],
        |  "maximum": \[
        |    99.0
        |  \],
        |  "mean": \[
        |    49.5
        |  \],
        |  "population_variance": \[
        |    833.25
        |  \],
        |  "population_standard_deviation": \[
        |    28.866070[0-9]+
        |  \],
        |  "population_skewness": \[
        |    0.0
        |  \],
        |  "population_kurtosis": \[
        |    1.799759[0-9]+
        |  \],
        |  "population_excess_kurtosis": \[
        |    -1.200240[0-9]+
        |  \],
        |  "sample_variance": \[
        |    841.666666[0-9]+
        |  \],
        |  "sample_standard_deviation": \[
        |    29.011491[0-9]+
        |  \],
        |  "sample_skewness": \[
        |    0.0
        |  \],
        |  "sample_kurtosis": \[
        |    1.889747[0-9]+
        |  \],
        |  "sample_excess_kurtosis": \[
        |    -1.110252[0-9]+
        |  \],
        |  "population_covariance": \[
        |    833.25
        |  \],
        |  "population_correlation": \[
        |    1.0
        |  \],
        |  "sample_covariance": \[
        |    841.666666[0-9]+
        |  \],
        |  "sample_correlation": \[
        |    1.0
        |  \]
        |\}""".stripMargin('|').replaceAll("\\s+","")

  val JSON_EMPTY="""{"count":0}"""

  "Stats stat" should {

    "work with ints" >> {
      "be empty initiallly" >> {
        val descStats = newStat[java.lang.Integer]("intAttr", observe = false)
        descStats.properties(0) mustEqual "intAttr"
        descStats.isEmpty must beTrue
        descStats.count mustEqual 0
      }

      "observe correct values" >> {
        val descStats = newStat[java.lang.Integer]("intAttr")
        descStats.bounds(0) mustEqual (0, 99)
        descStats.count must beCloseTo(100L, 5)
      }

      "serialize to json" >> {
        val descStats = newStat[java.lang.Integer]("intAttr")
        descStats.toJson.replaceAll("\\s+","") must beMatching(JSON_0_to_100)
      }

      "serialize empty to json" >> {
        val descStats = newStat[java.lang.Integer]("intAttr", observe = false)
        descStats.toJson.replaceAll("\\s+","") mustEqual JSON_EMPTY
      }

      "serialize and deserialize" >> {
        val descStats = newStat[java.lang.Integer]("intAttr")
        val packed = StatSerializer(sft).serialize(descStats)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual descStats.toJson
      }

      "serialize and deserialize empty descStats" >> {
        val descStats = newStat[java.lang.Integer]("intAttr", observe = false)
        val packed = StatSerializer(sft).serialize(descStats)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual descStats.toJson
      }

      "combine two descStatses" >> {
        val descStats = newStat[java.lang.Integer]("intAttr")
        val descStats2 = newStat[java.lang.Integer]("intAttr", observe = false)

        features2.foreach { descStats2.observe }

        descStats2.bounds(0) mustEqual (100, 199)
        descStats2.count must beCloseTo(100L, 5)

        descStats += descStats2

        descStats.bounds(0) mustEqual (0, 199)
        descStats.count must beCloseTo(200L, 5)
        descStats2.bounds(0) mustEqual (100, 199)
      }

      "clear" >> {
        val descStats = newStat[java.lang.Integer]("intAttr")
        descStats.isEmpty must beFalse

        descStats.clear()

        descStats.isEmpty must beTrue
        descStats.count mustEqual 0
      }

      "negatives" >> {
        val descStats3 = newStat[java.lang.Integer]("intAttr", observe = false)

        features3.foreach { descStats3.observe }

        descStats3.bounds(0) mustEqual (-100, -1)
        descStats3.count must beCloseTo(100L, 5)
      }
    }

    "work with longs" >> {
      "be empty initiallly" >> {
        val descStats = newStat[java.lang.Long]("longAttr", observe = false)
        descStats.properties(0) mustEqual "longAttr"
        descStats.isEmpty must beTrue
        descStats.count mustEqual 0
      }

      "observe correct values" >> {
        val descStats = newStat[java.lang.Long]("longAttr")
        descStats.bounds(0) mustEqual (0L, 99L)
        descStats.count must beCloseTo(100L, 5)
      }

      "serialize to json" >> {
        val descStats = newStat[java.lang.Long]("longAttr")
        descStats.toJson.replaceAll("\\s+","") must beMatching(JSON_0_to_100)
      }

      "serialize empty to json" >> {
        val descStats = newStat[java.lang.Long]("longAttr", observe = false)
        descStats.toJson.replaceAll("\\s+","") mustEqual JSON_EMPTY
      }

      "serialize and deserialize" >> {
        val descStats = newStat[java.lang.Long]("longAttr")
        val packed = StatSerializer(sft).serialize(descStats)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual descStats.toJson
      }

      "serialize and deserialize empty descStats" >> {
        val descStats = newStat[java.lang.Long]("longAttr", observe = false)
        val packed = StatSerializer(sft).serialize(descStats)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual descStats.toJson
      }

      "combine two descStatses" >> {
        val descStats = newStat[java.lang.Long]("longAttr")
        val descStats2 = newStat[java.lang.Long]("longAttr", observe = false)

        features2.foreach { descStats2.observe }

        descStats2.bounds(0) mustEqual (100L, 199L)
        descStats2.count must beCloseTo(100L, 5)

        descStats += descStats2

        descStats.bounds(0) mustEqual (0L, 199L)
        descStats.count must beCloseTo(200L, 5)
        descStats2.bounds(0) mustEqual (100L, 199L)
      }

      "clear" >> {
        val descStats = newStat[java.lang.Long]("longAttr")
        descStats.isEmpty must beFalse

        descStats.clear()

        descStats.isEmpty must beTrue
        descStats.count mustEqual 0
      }

      "negatives" >> {
        val descStats3 = newStat[java.lang.Integer]("longAttr", observe = false)

        features3.foreach { descStats3.observe }

        descStats3.bounds(0) mustEqual (-100L, -1L)
        descStats3.count must beCloseTo(100L, 5)
      }
    }

    "work with floats" >> {
      "be empty initiallly" >> {
        val descStats = newStat[java.lang.Float]("floatAttr", observe = false)
        descStats.properties(0) mustEqual "floatAttr"
        descStats.isEmpty must beTrue
        descStats.count mustEqual 0
      }

      "observe correct values" >> {
        val descStats = newStat[java.lang.Float]("floatAttr")
        descStats.bounds(0) mustEqual (0f, 99f)
        descStats.count must beCloseTo(100L, 5)
      }

      "serialize to json" >> {
        val descStats = newStat[java.lang.Float]("floatAttr")
        descStats.toJson.replaceAll("\\s+","") must beMatching(JSON_0_to_100)
      }

      "serialize empty to json" >> {
        val descStats = newStat[java.lang.Float]("floatAttr", observe = false)
        descStats.toJson.replaceAll("\\s+","") mustEqual JSON_EMPTY
      }

      "serialize and deserialize" >> {
        val descStats = newStat[java.lang.Float]("floatAttr")
        val packed = StatSerializer(sft).serialize(descStats)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual descStats.toJson
      }

      "serialize and deserialize empty descStats" >> {
        val descStats = newStat[java.lang.Float]("floatAttr", observe = false)
        val packed = StatSerializer(sft).serialize(descStats)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual descStats.toJson
      }

      "combine two descStatses" >> {
        val descStats = newStat[java.lang.Float]("floatAttr")
        val descStats2 = newStat[java.lang.Float]("floatAttr", observe = false)

        features2.foreach { descStats2.observe }

        descStats2.bounds(0) mustEqual (100f, 199f)
        descStats2.count must beCloseTo(100L, 5)

        descStats += descStats2

        descStats.bounds(0) mustEqual (0f, 199f)
        descStats.count must beCloseTo(200L, 5)
        descStats2.bounds(0) mustEqual (100f, 199f)
      }

      "clear" >> {
        val descStats = newStat[java.lang.Float]("floatAttr")
        descStats.isEmpty must beFalse

        descStats.clear()

        descStats.isEmpty must beTrue
        descStats.count mustEqual 0
      }

      "negatives" >> {
        val descStats3 = newStat[java.lang.Integer]("floatAttr", observe = false)

        features3.foreach { descStats3.observe }

        descStats3.bounds(0) mustEqual (-100f, -1f)
        descStats3.count must beCloseTo(100L, 5)
      }
    }

    "work with doubles" >> {
      "be empty initiallly" >> {
        val descStats = newStat[java.lang.Double]("doubleAttr", observe = false)
        descStats.properties(0) mustEqual "doubleAttr"
        descStats.isEmpty must beTrue
        descStats.count mustEqual 0
      }

      "observe correct values" >> {
        val descStats = newStat[java.lang.Double]("doubleAttr")
        descStats.bounds(0) mustEqual (0d, 99d)
        descStats.count must beCloseTo(100L, 5)
      }

      "serialize to json" >> {
        val descStats = newStat[java.lang.Double]("doubleAttr")
        descStats.toJson.replaceAll("\\s+","") must beMatching(JSON_0_to_100)
      }

      "serialize empty to json" >> {
        val descStats = newStat[java.lang.Double]("doubleAttr", observe = false)
        descStats.toJson.replaceAll("\\s+","") mustEqual JSON_EMPTY
      }

      "serialize and deserialize" >> {
        val descStats = newStat[java.lang.Double]("doubleAttr")
        val packed = StatSerializer(sft).serialize(descStats)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual descStats.toJson
      }

      "serialize and deserialize empty descStats" >> {
        val descStats = newStat[java.lang.Double]("doubleAttr", observe = false)
        val packed = StatSerializer(sft).serialize(descStats)
        val unpacked = StatSerializer(sft).deserialize(packed)
        unpacked.toJson mustEqual descStats.toJson
      }

      "combine two descStatses" >> {
        val descStats = newStat[java.lang.Double]("doubleAttr")
        val descStats2 = newStat[java.lang.Double]("doubleAttr", observe = false)

        features2.foreach { descStats2.observe }

        descStats2.bounds(0) mustEqual (100d, 199d)
        descStats2.count must beCloseTo(100L, 5)

        descStats += descStats2

        descStats.bounds(0) mustEqual (0d, 199d)
        descStats.count must beCloseTo(200L, 10)
        descStats2.bounds(0) mustEqual (100d, 199d)
      }

      "clear" >> {
        val descStats = newStat[java.lang.Double]("doubleAttr")
        descStats.isEmpty must beFalse

        descStats.clear()

        descStats.isEmpty must beTrue
        descStats.count mustEqual 0
      }

      "negatives" >> {
        val descStats3 = newStat[java.lang.Integer]("doubleAttr", observe = false)

        features3.foreach { descStats3.observe }

        descStats3.bounds(0) mustEqual (-100d, -1d)
        descStats3.count must beCloseTo(100L, 5)
      }
    }
  }
}
