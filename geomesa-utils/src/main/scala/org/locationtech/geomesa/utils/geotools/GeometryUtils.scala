/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

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
    geometry match {
      case p: Point => geometry.distance(farthestPoint(p, meters))
      case _ => distanceDegrees(geometry.getEnvelopeInternal, meters)
    }
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

  /**
    * Returns the rough bounds of a geometry
    *
    * @param geometry geometry
    * @return (xmin, ymin, xmax, ymax)
    */
  def bounds(geometry: Geometry): (Double, Double, Double, Double) = {
    val env = geometry.getEnvelopeInternal
    (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
  }

  /**
    * Evaluates the complexity of a geometry. Will return true if the geometry is a point or
    * a rectangular polygon without interior holes.
    *
    * @param geometry geometry
    * @return
    */
  def isRectangular(geometry: Geometry): Boolean = geometry match {
    case _: Point   => true
    case p: Polygon => noInteriorRings(p) && noCutouts(p) && allRightAngles(p)
    case _ => false
  }

  // checks that there are no interior holes
  private def noInteriorRings(p: Polygon): Boolean = p.getNumInteriorRing == 0

  // checks that all points are on the exterior envelope of the polygon
  private def noCutouts(p: Polygon): Boolean = {
    val (xmin, ymin, xmax, ymax) = {
      val env = p.getEnvelopeInternal
      (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
    }
    p.getCoordinates.forall(c => c.x == xmin || c.x == xmax || c.y == ymin || c.y == ymax)
  }

  // checks that there aren't any angled lines
  private def allRightAngles(p: Polygon): Boolean =
    p.getCoordinates.sliding(2).forall { case Array(left, right) => left.x == right.x || left.y == right.y }
}
