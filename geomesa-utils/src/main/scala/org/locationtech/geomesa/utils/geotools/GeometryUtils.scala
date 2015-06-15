/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.geotools

import com.vividsolutions.jts.geom._
import org.geotools.geometry.jts.JTSFactoryFinder
import org.geotools.referencing.GeodeticCalculator

/**
 * The object provides convenience methods for common operations on geometries.
 */
object GeometryUtils {

  val geoFactory = JTSFactoryFinder.getGeometryFactory

  val zeroPoint = geoFactory.createPoint(new Coordinate(0,0))

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
