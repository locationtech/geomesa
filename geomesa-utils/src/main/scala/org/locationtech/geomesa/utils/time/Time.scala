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

import org.joda.time.Interval

object Time {
  val noInterval: Interval = null

  implicit class RichInterval(self: Interval) {
    def getSafeUnion(other: Interval): Interval = {
      if (self != noInterval && other != noInterval) {
        new Interval(
          if (self.getStart.isBefore(other.getStart)) self.getStart else other.getStart,
          if (self.getEnd.isAfter(other.getEnd)) self.getEnd else other.getEnd
        )
      } else noInterval
    }

    def getSafeIntersection(other: Interval): Interval =
      if (self == noInterval) other
      else if (other == noInterval) self
      else {
        new Interval(
          if (self.getStart.isBefore(other.getStart)) other.getStart else self.getStart,
          if (self.getEnd.isAfter(other.getEnd)) other.getEnd else self.getEnd
        )
      }
  }
}
