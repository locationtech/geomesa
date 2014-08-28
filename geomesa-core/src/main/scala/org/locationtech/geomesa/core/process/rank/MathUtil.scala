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

package org.locationtech.geomesa.core.process.rank

object MathUtil {
  def squaredDifference(v1: Double, v2: Double) = math.pow(v1 - v2, 2.0)

  def avg(list: Iterable[Double]) = list.sum / list.size

  def stdDev(list: Iterable[Double], average: Double): Double = list.isEmpty match {
    case false =>
      val squared = list.foldLeft(0.0)(_ + squaredDifference(_, average))
      math.sqrt(squared / list.size.toDouble)
    case true => 0.0
  }

  // only legitimate when d1 and d2 come from independent data
  def combineStddev(d1: Double, d2: Double) = Math.sqrt(d1 * d1 + d2 * d2)

  /**
   * Initial heading from first to last, where each is lon, lat, in radians
   * @return Angle of heading (in degrees), (-180,180] ranging counterclockwise from South, with 0 as North
   */
  def headingGivenRadians(first: (Double, Double), last: (Double, Double)) = {
    val lonDiff = last._1 - first._1
    val cosLastY = Math.cos(last._2)
    val y = Math.sin(lonDiff) * cosLastY
    val x = Math.cos(first._2) * Math.sin(last._2) - Math.sin(first._2) * cosLastY * Math.cos(lonDiff)
    Math.atan2(y, x).toDegrees
  }

  /**
   * Return the difference between two angles
   * @param first First heading, an angle in degrees in (-180.0,180.0]
   * @param second Second heading, an angle in degrees in (-180.0,180.0]
   * @return The angle between the headings, in degrees, in [0.0, 180.0]
   */
  def headingDifference(first: Double, second: Double) = {
    val clockwiseDifference = Math.max(first, second) - Math.min(first, second)
    if (clockwiseDifference <= 180.0) clockwiseDifference
    else 360.0 - clockwiseDifference
  }
}
