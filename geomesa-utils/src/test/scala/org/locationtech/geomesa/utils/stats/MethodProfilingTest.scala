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
class MethodProfilingTest extends Specification {

  "MethodProfiling" should {
    "keep track of explicit timings" in {
      class Profiling extends MethodProfiling {
        val timings = new TimingsImpl
        def slowMethod(): String = {
          Thread.sleep(10)
          "test"
        }
        def exec: String = {
          profile(time => timings.occurrence("1", time))(slowMethod())
        }
      }

      val profiling = new Profiling
      val result = profiling.exec
      result mustEqual "test"
      profiling.timings.occurrences("1") mustEqual 1
      profiling.timings.time("1") must beGreaterThanOrEqualTo(10L)
    }
  }

  "Timing" should {
    "keep track of total time" in {
      val timing = new Timing
      timing.time mustEqual 0
      timing.occurrences mustEqual 0

      timing.occurrence(100)
      timing.time mustEqual 100
      timing.occurrences mustEqual 1

      timing.occurrence(50)
      timing.time mustEqual 150
      timing.occurrences mustEqual 2
    }

    "compute averages" in {
      val timing = new Timing
      timing.average().toString mustEqual Double.NaN.toString

      timing.occurrence(100)
      timing.average mustEqual 100

      timing.occurrence(50)
      timing.average mustEqual 75
    }
  }

  "Timings" should {

    "keep track of total time" in {
      val timings = new TimingsImpl
      timings.time("1") mustEqual 0
      timings.occurrences("1") mustEqual 0

      timings.occurrence("1", 100)
      timings.occurrence("2", 200)
      timings.time("1") mustEqual 100
      timings.occurrences("1") mustEqual 1
      timings.time("2") mustEqual 200
      timings.occurrences("2") mustEqual 1
    }

    "compute average times" in {
      val timings = new TimingsImpl
      timings.averageTimes mustEqual "No occurrences"

      timings.occurrence("1", 100)
      timings.occurrence("2", 200)

      timings.averageTimes() mustEqual "Total time: 300 ms. Percent of time - 1: 33.3% 1 times at 100.0000 ms avg, 2: 66.7% 1 times at 200.0000 ms avg"
    }

    "compute average occurrences" in {
      val timings = new TimingsImpl
      timings.averageTimes mustEqual "No occurrences"

      timings.occurrence("1", 100)
      timings.occurrence("2", 200)

      timings.averageOccurrences() mustEqual "Total occurrences: 2. Percent of occurrences - 1: 50.0%, 2: 50.0%"
    }
  }
}