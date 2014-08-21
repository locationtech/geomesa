/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

import com.vividsolutions.jts.geom.Coordinate
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.DateTime

class CoordWithDateTime(val c: Coordinate, val dt: DateTime, maxSpeed: Double = CoordWithDateTime.defaultMaxSpeed,
                        maxTurnRate: Double = CoordWithDateTime.defaultMaxTurnRate) {
  def consistentWithMotion(previous: CoordWithDateTime, beforeThat: CoordWithDateTime): Boolean = {
    if (consistentWithMotion(previous)) {
      // Check that it hasn't changed heading too quickly
      val pair1 = new CoordWithDateTimePair(previous, this)
      val pair2 = new CoordWithDateTimePair(beforeThat, previous)
      val headingDiff1 = pair1.heading - pair2.heading
      val headingDiff = if (headingDiff1 > 180.0) headingDiff1 - 180.0 else headingDiff1
      val turnRate = headingDiff / (pair1.timeDiff + pair2.timeDiff)
      turnRate < maxTurnRate
    }
    else false
  }

  def consistentWithMotion(previous: CoordWithDateTime): Boolean = {
    // check that its speed is reasonable
    val coordPair = new CoordWithDateTimePair(previous, this)
    if (coordPair.timeDiff < RankingDefaults.maxTimeBetweenPings) {
      val speed = coordPair.speed
      (speed > 0.0) && (speed < maxSpeed)
    }
    else false
  }

  /**
   * Check whether this point is consistent with vehicle motion
   * @param otherPoints the list of other points that define existing motion (must be ordered from newest to oldest)
   * @return true if this point is possibly consistent with motion involving the other points
   */
  def consistentWithMotion(otherPoints: List[CoordWithDateTime]): Boolean = {
    otherPoints match {
      case previous :: beforeThat :: rest => consistentWithMotion(previous, beforeThat)
      case previous :: rest => consistentWithMotion(previous)
      case Nil => true
    }
  }
}

// these defaults are reasonably appropriate for airplanes
object CoordWithDateTime {
  val defaultMaxTurnRate = 10.0 // units are degrees per second
  val defaultMaxSpeed = 1000.0 // units are meters per second
}

case class SpeedStatistics(max: Double, min: Double, avg: Double, stddev: Double)

case class CoordWithDateTimePair(first: CoordWithDateTime, last: CoordWithDateTime) {
  /**
   * How far from the first point in the pair to the last
   * @return distance in meters
   */
  def distance = JTS.orthodromicDistance(first.c, last.c, DefaultGeographicCRS.WGS84)

  /**
   * Time difference from first to last point
   * @return seconds
   */
  def timeDiff = Math.abs((first.dt.getMillis - last.dt.getMillis).toDouble) / 1000.0

  /**
   * Speed traveled between points
   * @return meters per second
   */
  def speed = distance / timeDiff

  def heading = {
    val lonDiff = last.c.x - first.c.x
    val cosLastY = Math.cos(last.c.y)
    val y = Math.sin(lonDiff) * cosLastY
    val x = Math.cos(first.c.y) * Math.sin(last.c.y) - Math.sin(first.c.y) * cosLastY * Math.cos(lonDiff)
    Math.atan2(y, x).toDegrees
  }
}

class CoordSequence(val coords: Seq[CoordWithDateTimePair]) {
  def distance: Double = coords.foldLeft(0.0) { (dist, pair) => dist + pair.distance }

  def speedStats = {
    if (speeds.length == 0) SpeedStatistics(0.0, 0.0, 0.0, 0.0)
    else {
      val avg = speeds.sum / speeds.length.toDouble
      SpeedStatistics(speeds.max, speeds.min, avg, MathUtil.stdDev(speeds, avg))
    }
  }

  lazy val speeds: Seq[Double] = coords.map(_.speed)
}

object CoordSequence {
  def fromCoordWithDateTimeList(motionCoords: Seq[CoordWithDateTime]): CoordSequence =
    new CoordSequence(
      motionCoords
        .sortBy(_.dt.getMillis)
        .sliding(2, 1)
        .collect { // if there is only one element in motionCoords, this phase will return empty
          case l :: r :: t => CoordWithDateTimePair(l, r) // sliding should return size-2 Seqs, t is empty
        }
        .toSeq
    )
}
