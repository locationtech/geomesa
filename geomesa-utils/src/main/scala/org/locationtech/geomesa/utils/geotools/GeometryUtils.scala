/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
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

  val geoFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

  val zeroPoint: Point = geoFactory.createPoint(new Coordinate(0,0))

  /**
    * Convert meters to decimal degrees, based on the latitude of the geometry.
    *
    * Returns two values, ones based on latitude and one based on longitude. The first value
    * will always be &lt;= the second value
    *
    * For non-point geometries, distances are measured from the corners of the geometry envelope
    *
    * @param geom geometry to buffer
    * @param meters meters
    * @return (min degrees, max degrees)
    */
  def distanceDegrees(geom: Geometry, meters: Double): (Double, Double) = {
    geom match {
      case p: Point => distanceDegrees(p, meters)
      case _        => distanceDegrees(geom.getEnvelopeInternal, meters)
    }
  }

  private def distanceDegrees(point: Point, meters: Double): (Double, Double) = {
    val calc = new GeodeticCalculator()
    calc.setStartingGeographicPoint(point.getX, point.getY)
    val north = {
      calc.setDirection(0, meters)
      val dest2D = calc.getDestinationGeographicPoint
      point.distance(geoFactory.createPoint(new Coordinate(dest2D.getX, dest2D.getY)))
    }
    val east = {
      calc.setDirection(90, meters)
      val dest2D = calc.getDestinationGeographicPoint
      point.distance(geoFactory.createPoint(new Coordinate(dest2D.getX, dest2D.getY)))
    }
    // normally east would be the largest in degrees, but sometimes it can be smaller
    // due to variances in the ellipsoid
    if (east > north) { (north, east) } else { (east, north) }
  }

  private def distanceDegrees(env: Envelope, meters: Double): (Double, Double) = {
    val distances = Seq(
      distanceDegrees(geoFactory.createPoint(new Coordinate(env.getMaxX, env.getMaxY)), meters),
      distanceDegrees(geoFactory.createPoint(new Coordinate(env.getMaxX, env.getMinY)), meters),
      distanceDegrees(geoFactory.createPoint(new Coordinate(env.getMinX, env.getMinY)), meters),
      distanceDegrees(geoFactory.createPoint(new Coordinate(env.getMinX, env.getMaxY)), meters)
    )

    (distances.minBy(_._1)._1, distances.maxBy(_._2)._2)
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

  /**
    * This function checks if a segment crosses the IDL.
    * @param point1 The first point in the segment
    * @param point2 The second point in the segment
    * @return boolean true if the segment crosses the IDL, otherwise false
    */
  def crossesIDL(point1:Coordinate, point2:Coordinate): Boolean = {
    Math.abs(point1.x - point2.x) >= 180
  }

  /**
    * Calculate the latitude at which the segment intercepts the IDL.
    * This function assumes that the provided points do actually cross the IDL.
    * @param point1 The first point in the segment
    * @param point2 The second point in the segment
    * @return a double representing the intercept latitude
    */
  def calcIDLIntercept(point1: Coordinate, point2: Coordinate): Double = {
    if (point1.x < 0) {
      calcCrossLat(point1, new Coordinate(point2.x - 360, point2.y), -180)
    } else {
      calcCrossLat(point1, new Coordinate(point2.x + 360, point2.y), 180)
    }
  }

  /**
    * Calculate the latitude at which a segment intercepts a given latitude.
    * @param point1 The first point in the segment
    * @param point2 The second point in the segment
    * @param crossLon The longitude of intercept
    * @return a double representing the intercept latitude
    */
  def calcCrossLat(point1: Coordinate, point2: Coordinate, crossLon: Double): Double = {
    val slope = (point1.y - point2.y) / (point1.x - point2.x)
    val intercept = point1.y - (slope * point1.x)
    (slope * crossLon) + intercept
  }
}
