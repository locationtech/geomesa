/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kafka

import java.util.Date

import org.junit.runner.RunWith
import org.opengis.filter.Filter
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.locationtech.geomesa.kafka.ReplayTimeHelper.ff

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TimestampFilterSplitTest extends Specification with Mockito {

  "TimestampFilterSplitTest" should {

    val time = new Date(123456789L)
    val timeLiteral = ff.literal(time)
    val prop = ff.property(ReplayTimeHelper.AttributeName)

    "find the timestamp by itself" >> {

      "with time on the left" >> {
        val filter = ff.equals(timeLiteral, prop)

        testSplit(filter, Some(time.getTime), None)
      }

      "with time on the right" >> {
        val filter = ff.equals(prop, timeLiteral)

        testSplit(filter, Some(time.getTime), None)
      }
    }

    "not find the timestamp if the value is not a date" >> {
      val intTime = ff.literal(123456789L)
      val filter = ff.equals(intTime, prop)

      testSplit(filter, None, Some(filter))
    }

    "not find the timestamp if op is not equals" >> {
      val filter = ff.less(timeLiteral, prop)

      testSplit(filter, None, Some(filter))
    }

    "not find the timestamp the property name does not match" >> {
      val filter = ff.equals(timeLiteral, ff.property("foo"))

      testSplit(filter, None, Some(filter))
    }

    "handle an And with no timestamp" >> {
      val m1 = mockAs[Filter]("m1")
      val m2 = mockAs[Filter]("m2")
      val and = ff.and(List(m1, m2))

      testSplit(and, None, Some(and))
    }

    "find the timestamp in an And" >> {

      val tsFilter = ff.equals(prop, timeLiteral)

      "with 0 sibs" >> {
        val and = ff.and(List(tsFilter))

        testSplit(and, Some(time.getTime), None)
      }

      "with 1 sib" >> {
        val m1 = mockAs[Filter]("m1")
        val and = ff.and(List(m1, tsFilter))

        testSplit(and, Some(time.getTime), Some(m1))
      }

      "with 2 sibs" >> {
        val m1 = mockAs[Filter]("m1")
        val m2 = mockAs[Filter]("m2")
        val and = ff.and(List(m1, tsFilter, m2))

        testSplit(and, Some(time.getTime), Some(ff.and(List(m1, m2))))
      }

      "with 3 sibs" >> {
        val m1 = mockAs[Filter]("m1")
        val m2 = mockAs[Filter]("m2")
        val m3 = mockAs[Filter]("m3")
        val and = ff.and(List(tsFilter, m1, m2, m3))

        testSplit(and, Some(time.getTime), Some(ff.and(List(m1, m2, m3))))
      }

      "with multiple timestamps" >> {
        val m1 = mockAs[Filter]("m1")
        val m2 = mockAs[Filter]("m2")
        val and = ff.and(List(tsFilter, m1, tsFilter, m2))

        testSplit(and, Some(time.getTime), Some(ff.and(List(m1, m2))))
      }
    }

    "return None if And contains different timestamps" >> {
      val tsFilter1 = ff.equals(prop, timeLiteral)
      val tsFilter2 = ff.equals(prop, ff.literal(new Date(13456790L)))
      val m1 = mock[Filter]

      val and = ff.and(List(tsFilter1, m1, tsFilter2))

      val result = TimestampFilterSplit.split(and)

      result must beNone
    }

    "handle an Or with no timestamp" >> {
      val m1 = mockAs[Filter]("m1")
      val m2 = mockAs[Filter]("m2")
      val or = ff.or(List(m1, m2))

      testSplit(or, None, Some(or))
    }

    "find the timestamp in an Or" >> {

      val tsFilter = ff.equals(prop, timeLiteral)

      val m1 = mockAs[Filter]("m1")
      val m2 = mockAs[Filter]("m2")
      val m3 = mockAs[Filter]("m3")

      val tsAnd1 = ff.and(tsFilter, m1)
      val tsAnd2 = ff.and(List(m2, tsFilter, m3))
      val tsAnd3 = ff.and(List(m2, m3, m1, tsFilter))

      "with single direct timestamp" >> {
        val or = ff.or(List(tsFilter))

        testSplit(or, Some(time.getTime), None)
      }

      "with 1 child" >> {
        val or = ff.or(List(tsAnd1))

        testSplit(or, Some(time.getTime), Some(m1))
      }

      "with 2 children" >> {
        val or = ff.or(List(tsAnd1, tsAnd2))

        val expectedFilter = ff.or(m1, ff.and(m2, m3))

        testSplit(or, Some(time.getTime), Some(expectedFilter))
      }

      "with three children" >> {
        val or = ff.or(List(tsAnd1, tsAnd2, tsAnd3))

        testSplit(or, Some(time.getTime), Some(ff.or(List(m1, ff.and(m2, m3), ff.and(List(m2, m3, m1))))))
      }
    }

    "return None if Or contains different timestamps" >> {
      val tsFilter1 = ff.equals(prop, ff.literal(new Date(1L)))
      val tsFilter2 = ff.equals(prop, ff.literal(new Date(2L)))

      val m1 = mockAs[Filter]("m1")
      val m2 = mockAs[Filter]("m2")

      val tsAnd1 = ff.and(tsFilter1, m1)
      val tsAnd2 = ff.and(m2, tsFilter2)

      val or = ff.or(tsAnd1, tsAnd2)

      val result = TimestampFilterSplit.split(or)

      result must beNone
    }

    "return None if not all Or children contains timestamps" >> {
      val tsFilter = ff.equals(prop, ff.literal(new Date(1L)))

      val m1 = mockAs[Filter]("m1")
      val m2 = mockAs[Filter]("m2")

      val or = ff.or(m1, ff.and(tsFilter, m2))

      val result = TimestampFilterSplit.split(or)

      result must beNone
    }

    "return None if a nested child is invalid" >> {
      val ts = ff.equals(prop, ff.literal(new Date(1L)))
      val m1 = mockAs[Filter]("m1")

      // the right side of the and is invalid because all child of the or must have timestamp if any do
      val f = ff.and(ts, ff.or(ts, m1))

      val result = TimestampFilterSplit.split(f)
      result must beNone
    }

    "handle Not without a timestamp" >> {
      val f = ff.not(mock[Filter])

      testSplit(f, None, Some(f))
    }

    "return None if Not contains a timestamp" >> {
      val tsFilter = ff.equals(prop, ff.literal(new Date(1L)))
      val f = ff.not(tsFilter)

      val result = TimestampFilterSplit.split(f)
      result must beNone
    }
  }

  def testSplit(filter: Filter, expectedTS: Option[Long], expectedFilter: Option[Filter]) = {
    val result = TimestampFilterSplit.split(filter)

    result must beSome

    result.get.ts mustEqual expectedTS
    result.get.filter mustEqual expectedFilter
  }
}
