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

import com.vividsolutions.jts.geom.{PrecisionModel, GeometryFactory, LineString}
import com.vividsolutions.jts.linearref.LocationIndexedLine
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.renderer.label.LineStringCursor

/**
 * A MotionScore rolls up statistics computed for one single sequence of pings along the route or track. The 
 * EvidenceOfMotion object used in the ranking process is an aggregation of one or more MotionScores for a single ID / 
 * entity across many groups of pings that have been segmented to match potential tracks. In the following comments we
 * refer to each sequence of pings as a "tracklet"
 * @param numberOfPings the number of pings in the tracklet 
 * @param cumulativePathDistance the distance along the tracklet
 * @param queryRouteDistance the distance along the query route or track 
 * @param cumulativeDistanceFromRoute the cumulative distance that the tracklet deviates from the route or track,
 *                                    calculated by dividing the track or route up into n divisions (100 by default) and
 *                                    calculating the closest distance from the route to the tracklet at each division
 * @param speedStats max, min, avg, and stddev of speed calculated along the tracklet
 */
case class MotionScore(numberOfPings: Int,
                       cumulativePathDistance: Double,
                       queryRouteDistance: Double,
                       cumulativeDistanceFromRoute: Double,
                       speedStats: SpeedStatistics) {
  /**
   * Normalizes the distance of the tracklet from the route or track by dividing the cumulative distance by the
   * total route distance
   * @return normalized distance from route
   */
  def normalizedDistanceFromRoute =
    if (cumulativePathDistance == 0.0) 0.0
    else cumulativeDistanceFromRoute / cumulativePathDistance

  /**
   * Returns the ratio of the distance along the tracklet to the distance along the query route
   * @return a ration, where 1 indicates a close match and lower or higher values a poorer match
   */
  def lengthRatio =
    if (queryRouteDistance == 0.0) 0.0
    else cumulativePathDistance / queryRouteDistance

  /**
   * Converts the normalized distance from route to a score where a low distance scores near 1.0 and a high distance
   * scores near 0.0. Low distance indicates better match and therefore better evidence of motion along the route.
   * @return e^^(-1 * normalizedDistanceFromRoute)
   */
  def distanceFromRouteScore = Math.exp(-1.0 * normalizedDistanceFromRoute)

  /**
   * Computes a score from the speed stats where 1.0 indicates a reasonably constant speed (consistent with possible
   * motion along the route) and 0.0 indicates speed variability that might be inconsistent with travel.
   * @return a score between 0.0 and 1.0, with 1.0 indicating better evidence of motion along the route
   */
  def constantSpeedScore =
    if (speedStats.avg > 0.0) Math.exp(-1.0 * speedStats.stddev / speedStats.avg)
    else 0.0

  /**
   * Computes a score between 0.0 and 1.0 indicating whether the length of the tracklet matches the length of the route.
   * A score near 1.0 indicates a high match and better evidence of motion along the route.
   * @return a score between 0.0 and 1.0
   */
  def expectedLengthScore =
    if (lengthRatio < 1.0) lengthRatio
    else 1.0 / lengthRatio

  /**
   * Computes a score between 0.0 and 1.0 indicating whether the computed speeds for the tracklet are reasonably close
   * to speeds that might be expected of humans or vehicles.
   * @return a score between 0.0 and 1.0 with 1.0 indicating a reasonable speed consistent with motion along the route
   */
  def reasonableSpeedScore = 1.0

  /**
   * Computes a combined score that takes into account the other scores by computing the geometric mean. Includes a term
   * that scales by the log of the number of pings in the tracklet so that more pings indicate better evidence.
   * @return a score in which higher values indicate more evidence of motion than lower scores. The minimum value is
   *         zero.
   */
  def combined =
    MathUtil.geometricMean(
      Math.log(numberOfPings), distanceFromRouteScore, constantSpeedScore, expectedLengthScore, reasonableSpeedScore
    )
}

class Route(val route: LineString) {

  lazy val distance: Double =
    route.getCoordinates.toList
      .sliding(2, 1)
      .map { case f :: s :: t => JTS.orthodromicDistance(f, s, DefaultGeographicCRS.WGS84) }
      .sum
  lazy private val indexed = new LocationIndexedLine(route)

  /**
   * Calculate all the motion scores associated with a tracklet along the route
   * @param coordSeq the tracklet
   * @param routeDivisions the number of divisions to break the track into for deviation calculations
   * @return MotionScore
   */
  def motionScores(coordSeq: CoordSequence,
                   routeDivisions: Double = RankingDefaults.defaultRouteDivisions): MotionScore = {
    val coordDistance = coordSeq.distance
    val coordsToRouteDistance = cumlativeDistanceToCoordSequence(coordSeq, routeDivisions)
    val speedStats = coordSeq.speedStats
    new MotionScore(coordSeq.coords.length + 1, coordDistance, distance, coordsToRouteDistance, speedStats)
  }

  /**
   * Computes the total distance that the tracklet deviates from the route. Computed iteratively at regular intervals
   * along the tracklet as specified by coordDelta.
   * @param coords the tracklet
   * @param divisions the number of intervals to split the tracklet into
   * @return the total cumulative distance of the tracklet from the route at each coordDelta from the beginning of the
   *         tracklet to the end
   */
  def cumlativeDistanceToCoordSequence(coords: CoordSequence, divisions: Double): Double = {
    val ls = Route.geomFactory.createLineString(
      Array(coords.coords.head.first.c) ++ coords.coords.map(cdt => cdt.last.c)
    )
    val lsCursor = new LineStringCursor(ls)
    val lsLength = ls.getLength
    val lsDelta = lsLength / divisions
    (0.0 to lsLength by lsDelta).map {
      case ordinate =>
        lsCursor.moveTo(ordinate)
        val cp = lsCursor.getCurrentPosition
        val rp = indexed.extractPoint(indexed.project(cp))
        JTS.orthodromicDistance(cp, rp, DefaultGeographicCRS.WGS84)
    }.sum
  }

}

object Route {
  final lazy val geomFactory = new GeometryFactory(new PrecisionModel(), 4326)
}