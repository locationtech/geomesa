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
class SeqStatTest extends Specification with StatTestHelper {

  def newStat[T](observe: Boolean = true): SeqStat = {
    val stat = Stat(sft, "MinMax(intAttr);IteratorStackCount();Enumeration(longAttr);Histogram(doubleAttr,20,0,200)")
    if (observe) {
      features.foreach { stat.observe }
    }
    stat.asInstanceOf[SeqStat]
  }

  "Seq stat" should {

    "be empty initiallly" >> {
      val stat = newStat(observe = false)

      stat.stats must haveSize(4)
      stat.isEmpty must beFalse

      val mm = stat.stats(0).asInstanceOf[MinMax[java.lang.Integer]]
      val ic = stat.stats(1).asInstanceOf[IteratorStackCount]
      val eh = stat.stats(2).asInstanceOf[EnumerationStat[java.lang.Long]]
      val rh = stat.stats(3).asInstanceOf[Histogram[java.lang.Double]]

      mm.property mustEqual "intAttr"
      mm.isEmpty must beTrue

      ic.counter mustEqual 1

      eh.property mustEqual "longAttr"
      eh.enumeration must beEmpty

      rh.property mustEqual "doubleAttr"
      forall(0 until rh.length)(rh.count(_) mustEqual 0)
    }

    "observe correct values" >> {
      val stat = newStat()

      val stats = stat.stats

      stats must haveSize(4)
      stat.isEmpty must beFalse

      val mm = stat.stats(0).asInstanceOf[MinMax[java.lang.Integer]]
      val ic = stat.stats(1).asInstanceOf[IteratorStackCount]
      val eh = stat.stats(2).asInstanceOf[EnumerationStat[java.lang.Long]]
      val rh = stat.stats(3).asInstanceOf[Histogram[java.lang.Double]]

      mm.bounds mustEqual (0, 99)

      ic.counter mustEqual 1

      eh.enumeration.size mustEqual 100
      eh.enumeration(0L) mustEqual 1
      eh.enumeration(100L) mustEqual 0

      rh.length mustEqual 20
      rh.count(rh.indexOf(0.0)) mustEqual 10
      rh.count(rh.indexOf(50.0)) mustEqual 10
      rh.count(rh.indexOf(100.0)) mustEqual 0
    }

    "serialize to json" >> {
      val stat = newStat()
      stat.toJson must not(beEmpty)
    }

    "serialize empty to json" >> {
      val stat = newStat(observe = false)
      stat.toJson must not(beEmpty)
    }

    "serialize and deserialize" >> {
      val stat = newStat()
      val packed = StatSerializer(sft).serialize(stat)
      val unpacked = StatSerializer(sft).deserialize(packed)
      unpacked.toJson mustEqual stat.toJson
    }

    "serialize and deserialize empty SeqStat" >> {
      val stat = newStat(observe = false)
      val packed = StatSerializer(sft).serialize(stat)
      val unpacked = StatSerializer(sft).deserialize(packed)
      unpacked.toJson mustEqual stat.toJson
    }

    "deserialize as immutable value" >> {
      val stat = newStat()
      val packed = StatSerializer(sft).serialize(stat)
      val unpacked = StatSerializer(sft).deserialize(packed, immutable = true)
      unpacked.toJson mustEqual stat.toJson

      unpacked.clear must throwAn[Exception]
      unpacked.+=(stat) must throwAn[Exception]
      unpacked.observe(features.head) must throwAn[Exception]
      unpacked.unobserve(features.head) must throwAn[Exception]
    }

    "combine two SeqStats" >> {
      val stat = newStat()
      val stat2 = newStat(observe = false)

      val mm = stat.stats(0).asInstanceOf[MinMax[java.lang.Integer]]
      val ic = stat.stats(1).asInstanceOf[IteratorStackCount]
      val eh = stat.stats(2).asInstanceOf[EnumerationStat[java.lang.Long]]
      val rh = stat.stats(3).asInstanceOf[Histogram[java.lang.Double]]

      val mm2 = stat2.stats(0).asInstanceOf[MinMax[java.lang.Integer]]
      val ic2 = stat2.stats(1).asInstanceOf[IteratorStackCount]
      val eh2 = stat2.stats(2).asInstanceOf[EnumerationStat[java.lang.Long]]
      val rh2 = stat2.stats(3).asInstanceOf[Histogram[java.lang.Double]]

      ic2.counter mustEqual 1
      mm2.isEmpty must beTrue
      eh2.enumeration must beEmpty

      rh2.length mustEqual 20
      forall(0 until 20)(rh2.count(_) mustEqual 0)

      features2.foreach { stat2.observe }

      stat += stat2

      mm.bounds mustEqual (0, 199)

      ic.counter mustEqual 2

      eh.enumeration.size mustEqual 200
      eh.enumeration(0L) mustEqual 1
      eh.enumeration(100L) mustEqual 1

      rh.length mustEqual 20
      rh.count(rh.indexOf(0.0)) mustEqual 10
      rh.count(rh.indexOf(50.0)) mustEqual 10
      rh.count(rh.indexOf(100.0)) mustEqual 10

      mm2.bounds mustEqual (100, 199)

      ic2.counter mustEqual 1

      eh2.enumeration.size mustEqual 100
      eh2.enumeration(0L) mustEqual 0
      eh2.enumeration(100L) mustEqual 1

      rh2.length mustEqual 20
      rh2.count(rh2.indexOf(0.0)) mustEqual 0
      rh2.count(rh2.indexOf(50.0)) mustEqual 0
      rh2.count(rh2.indexOf(100.0)) mustEqual 10
    }

    "clear" >> {
      val stat = newStat()
      stat.isEmpty must beFalse

      stat.clear()

      val mm = stat.stats(0).asInstanceOf[MinMax[java.lang.Integer]]
      val ic = stat.stats(1).asInstanceOf[IteratorStackCount]
      val eh = stat.stats(2).asInstanceOf[EnumerationStat[java.lang.Long]]
      val rh = stat.stats(3).asInstanceOf[Histogram[java.lang.Double]]

      mm.property mustEqual "intAttr"
      mm.isEmpty must beTrue

      ic.counter mustEqual 1

      eh.property mustEqual "longAttr"
      eh.enumeration must beEmpty

      rh.property mustEqual "doubleAttr"
      forall(0 until rh.length)(rh.count(_) mustEqual 0)
    }
  }
}
