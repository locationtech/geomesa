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

package org.locationtech.geomesa.utils.time

import org.joda.time.{DateTime, DateTimeZone, Interval}
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.time.Time._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RichIntervalTest extends Specification {

  "RichIntervals" should {
    val dt1 = new DateTime("2012-01-01T01:00:00", DateTimeZone.UTC)
    val dt2 = new DateTime("2012-02-02T02:00:00", DateTimeZone.UTC)
    val dt3 = new DateTime("2012-03-03T03:00:00", DateTimeZone.UTC)
    val dt4 = new DateTime("2012-04-04T04:00:00", DateTimeZone.UTC)
    val dt5 = new DateTime("2012-05-05T05:00:00", DateTimeZone.UTC)

    val int12 = new Interval(dt1, dt2)
    val int13 = new Interval(dt1, dt3)

    val int23 = new Interval(dt2, dt3)
    val int25 = new Interval(dt2, dt5)

    val int34 = new Interval(dt3, dt4)
    val int35 = new Interval(dt3, dt5)
    val int45 = new Interval(dt4, dt5)

    "support unions and intersections" >> {
      val u1 = int12.getSafeUnion(int23)
      u1 must be equalTo int13

      val u2 = u1.getSafeUnion(int13)
      u2 must be equalTo int13

      u2.getSafeUnion(int12)  must be equalTo u2

      // Test intersections
      int13.getSafeIntersection(int12) must be equalTo int12
      int13.getSafeIntersection(int23) must be equalTo int23
    }

    "support empty intersections" >> {
      int12.getSafeIntersection(int34) must beNull
    }.pendingUntilFixed

    "handle expansions" >> {
      int12.expandByDate(dt3.toDate) must be equalTo int13
      int45.expandByDate(dt3.toDate) must be equalTo int35

      int23.expandByInterval(int35) must be equalTo int25
      int45.expandByInterval(int35) must be equalTo int35
    }
  }
}