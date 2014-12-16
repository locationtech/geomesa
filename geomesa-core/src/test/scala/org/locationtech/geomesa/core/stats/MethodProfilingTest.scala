/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.core.stats

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MethodProfilingTest extends Specification {

  "MethodProfiling" should {
    "keep track of implicit timings" in {

      class Profiling extends MethodProfiling {
        implicit val timings = new TimingsImpl
        def slowMethod(): String = {
          Thread.sleep(10)
          "test"
        }
        def exec: String = {
          profile(slowMethod(), "1")
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
      timing.average.toString mustEqual Double.NaN.toString

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

      timings.averageTimes() mustEqual "Total time: 300 ms. Percent of time - 1: 33.3% 100.0000 ms avg, 2: 66.7% 200.0000 ms avg"
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