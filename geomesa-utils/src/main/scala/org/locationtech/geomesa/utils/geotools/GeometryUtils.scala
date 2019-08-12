/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import org.geotools.geometry.jts.{JTSFactoryFinder, ReferencedEnvelope}
import org.geotools.referencing.GeodeticCalculator

import scala.util.control.NonFatal

/**
 * The object provides convenience methods for common operations on geometries.
 */
object GeometryUtils extends LazyLogging {

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
    val slope = (point1.y - point2.y) / (point1.x - point2.x);
    val intercept = point1.y - (slope * point1.x);
    (slope * crossLon) + intercept;
  }

  /**
    * Split a bounding box envelope, which may extend outside [-180,180], into one or more envelopes
    * that are contained within [-180,180]
    *
    * @param envelope envelope of a bounding box
    * @return
    */
  def splitBoundingBox(envelope: ReferencedEnvelope): Seq[ReferencedEnvelope] = {
    try {
      val crs = envelope.getCoordinateReferenceSystem

      // if the bbox is completely outside world bounds, translate to bring it back in
      val translated = if (envelope.getMinX >= 180d) {
        val multiplier = math.ceil((envelope.getMinX - 180d) / 360d)
        val left = envelope.getMinX - 360d * multiplier
        val right = envelope.getMaxX - 360d * multiplier
        new ReferencedEnvelope(left, right, envelope.getMinY, envelope.getMaxY, crs)
      } else if (envelope.getMaxX <= -180d) {
        val multiplier = math.ceil((envelope.getMaxX + 180d) / -360d)
        val left = envelope.getMinX + 360d * multiplier
        val right = envelope.getMaxX + 360d * multiplier
        new ReferencedEnvelope(left, right, envelope.getMinY, envelope.getMaxY, crs)
      } else {
        envelope
      }

      // if the bbox extends past world bounds, split and wrap it
      if (translated.getMinX < -180d) {
        val trimmed = new ReferencedEnvelope(-180d , translated.getMaxX, envelope.getMinY, envelope.getMaxY, crs)
        val wrapped = new ReferencedEnvelope(translated.getMinX + 360d , 180d, envelope.getMinY, envelope.getMaxY, crs)
        Seq(trimmed, wrapped)
      } else if (translated.getMaxX > 180d) {
        val trimmed = new ReferencedEnvelope(translated.getMinX, 180d, envelope.getMinY, envelope.getMaxY, crs)
        val wrapped = new ReferencedEnvelope(-180d, translated.getMaxX - 360d, envelope.getMinY, envelope.getMaxY, crs)
        Seq(trimmed, wrapped)
      } else {
        Seq(translated)
      }
    } catch {
      case NonFatal(e) => logger.warn(s"Error splitting bounding box envelope '$envelope':", e); Seq(envelope)
    }
  }
}
