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

import com.vividsolutions.jts.geom._
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.referencing.GeodeticCalculator
import org.geotools.referencing.crs.DefaultGeographicCRS

/**
 * The object provides convenience methods for common operations on geometries.
 */
object GeometryUtils {

  val geoCalc = new GeodeticCalculator(DefaultGeographicCRS.WGS84)
  val geoFactory = JTSFactoryFinder.getGeometryFactory

  /** Convert meters to dec degrees based on widest point in dec degrees of circles at bounding box corners */
  def distanceDegrees(geometry: Geometry, meters: Double): Double = {
    if (geometry.isInstanceOf[Point]) geometry.distance(farthestPoint(geometry.asInstanceOf[Point], meters))
    else distanceDegrees(geometry.getEnvelopeInternal, meters)
  }

  /** Convert meters to dec degrees based on widest point in dec degrees of circles at envelope corners */
  def distanceDegrees(env: Envelope, meters: Double): Double =
    List(
      distanceDegrees(geoFactory.createPoint(new Coordinate(env.getMaxX, env.getMaxY)), meters),
      distanceDegrees(geoFactory.createPoint(new Coordinate(env.getMaxX, env.getMinY)), meters),
      distanceDegrees(geoFactory.createPoint(new Coordinate(env.getMinX, env.getMinY)), meters),
      distanceDegrees(geoFactory.createPoint(new Coordinate(env.getMinX, env.getMaxY)), meters)
    ).max

  /** Farthest point based on widest point in dec degrees of circle */
  def farthestPoint(startPoint: Point, meters: Double) = {
    val calc = new GeodeticCalculator()
    calc.setStartingGeographicPoint(startPoint.getX, startPoint.getY)
    calc.setDirection(90, meters)
    val dest2D = calc.getDestinationGeographicPoint
    geoFactory.createPoint(new Coordinate(dest2D.getX, dest2D.getY))
  }

  def unfoldRight[A, B](seed: B)(f: B => Option[(A, B)]): List[A] = f(seed) match {
    case None => Nil
    case Some((a, b)) => a :: unfoldRight(b)(f)
  }

  /** Adds way points to Seq[Coordinates] so that they remain valid with Spatial4j, useful for BBOX */
  def addWayPoints(coords: Seq[Coordinate]): List[Coordinate] =
    unfoldRight(coords) {
      case Seq() => None
      case Seq(pt) => Some((pt, Seq()))
      case Seq(first, second, rest @ _*) => second.x - first.x match {
        case dx if dx > 120 =>
          Some((first, new Coordinate(first.x + 120, first.y) +: second +: rest))
        case dx if dx < -120 =>
          Some((first, new Coordinate(first.x - 120, first.y) +: second +: rest))
        case _ => Some((first, second +: rest))
      }
    }

}
