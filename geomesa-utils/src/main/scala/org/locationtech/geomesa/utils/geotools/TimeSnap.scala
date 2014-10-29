/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.utils.geotools

import org.joda.time.{DateTime, Duration, Interval}

class TimeSnap(val interval: Interval, val buckets: Int) {

  // convert to milliseconds and find the step size in milliseconds
  val dt: Duration = new Duration(interval.toDurationMillis / buckets)

  /**
   * Computes the time ordinate of the i'th grid column.
   * @param i the index of a grid column
   * @return the t time ordinate of the column
   */
  def t(i: Int): DateTime = i match {
    case x if x >= buckets => interval.getEnd
    case x if x < 0 => interval.getStart
    case x => interval.getStart.plus(dt.getMillis * i)
  }

  /**
   * Computes the column index of a time ordinate.
   * @param t the time ordinate
   * @return the column index
   */
  def i(t: DateTime): Int = t match {
    case x if x.isAfter(interval.getEnd) => buckets
    case x if x.isBefore(interval.getStart) => -1
    case x =>
      val ret = (x.getMillis - interval.getStart.getMillis) / dt.getMillis
      if (ret >= buckets) buckets - 1
      else ret.toInt
  }
}