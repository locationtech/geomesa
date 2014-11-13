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

package org.locationtech.geomesa.utils.geotools

import com.typesafe.scalalogging.slf4j.Logging
import org.joda.time.{DateTime, Duration, Interval}
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TimeSnapTest extends Specification with Logging {

  val buckets = 4
  val startDate = new DateTime(1970, 1, 1, 0, 0)
  val endDate = new DateTime(2010, 1, 1, 0, 0)
  val interval = new Interval(startDate, endDate)
  val timeSnap = new TimeSnap(interval, buckets)


  "TimeSnap" should {
    "create a timesnap around a given time interval" in {
      timeSnap must not beNull
    }

    "compute correct durations given number of bins wanted" in {
      val duration = new Duration(interval.toDurationMillis / buckets)
      timeSnap.dt.equals(duration) must beTrue
    }

    "compute correct bin index given time" in {
      var date = new DateTime(1985, 7, 31, 0, 0)
      var i = timeSnap.i(date)
      i must beEqualTo(1)

      date = date.withYear(2011)
      i = timeSnap.i(date)
      i must beEqualTo(buckets)

      date = date.withYear(1969)
      i = timeSnap.i(date)
      i must beEqualTo(-1)
    }

    "compute correct start time of bin given index" in {
      timeSnap.t(0) must beEqualTo(startDate)
      timeSnap.t(buckets) must beEqualTo(endDate)

      val simpleInterval = new Interval(0, 300)
      val simpleTimeSnap = new TimeSnap(simpleInterval, 3)
      simpleTimeSnap.t(-1).getMillis must beEqualTo(0)
      simpleTimeSnap.t(0).getMillis must beEqualTo(0)
      simpleTimeSnap.t(1).getMillis must beEqualTo(100)
      simpleTimeSnap.t(2).getMillis must beEqualTo(200)
      simpleTimeSnap.t(3).getMillis must beEqualTo(300)
      simpleTimeSnap.t(4).getMillis must beEqualTo(300)
    }
  }
}
