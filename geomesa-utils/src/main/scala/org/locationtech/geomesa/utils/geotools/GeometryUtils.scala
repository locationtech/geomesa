/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import com.typesafe.scalalogging.LazyLogging
import org.geotools.geometry.jts.{JTSFactoryFinder, ReferencedEnvelope}
import org.geotools.referencing.GeodeticCalculator
import org.locationtech.geomesa.utils.geohash.GeohashUtils
import org.locationtech.geomesa.utils.geohash.GeohashUtils.ResolutionRange
import org.locationtech.jts.geom._

import scala.util.control.NonFatal

/**
 * The object provides convenience methods for common operations on geometries.
 */
object GeometryUtils extends LazyLogging {

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
      case p: Point => distanceDegrees(p, meters, new GeodeticCalculator())
      case _        => distanceDegrees(geom.getEnvelopeInternal, meters)
    }
  }

  /**
    * Convert meters to decimal degrees, based on the latitude of the geometry.
    *
    * Returns two values, ones based on latitude and one based on longitude. The first value
    * will always be &lt;= the second value
    *
    * @param point geometry to buffer
    * @param meters meters
    * @param calc geodetic calculator instance
    * @return (min degrees, max degrees)
    */
  def distanceDegrees(point: Point, meters: Double, calc: GeodeticCalculator): (Double, Double) = {
    val (east, north) = directionalDegrees(point, meters, calc)
    // normally east would be the largest in degrees, but sometimes it can be smaller
    // due to variances in the ellipsoid
    if (east > north) { (north, east) } else { (east, north) }
  }

  /**
    * Convert meters to decimal degrees, based on the latitude of the geometry.
    *
    * Returns two values, ones based on latitude and one based on longitude. The first value
    * will always be &lt;= the second value
    *
    * Distances are measured from the corners of the geometry envelope
    *
    * @param env envelope to buffer
    * @param meters meters
    * @return (min degrees, max degrees)
    */
  def distanceDegrees(env: Envelope, meters: Double): (Double, Double) = {
    val distances = Seq(
      distanceDegrees(geoFactory.createPoint(new Coordinate(env.getMaxX, env.getMaxY)), meters),
      distanceDegrees(geoFactory.createPoint(new Coordinate(env.getMaxX, env.getMinY)), meters),
      distanceDegrees(geoFactory.createPoint(new Coordinate(env.getMinX, env.getMinY)), meters),
      distanceDegrees(geoFactory.createPoint(new Coordinate(env.getMinX, env.getMaxY)), meters)
    )
    (distances.minBy(_._1)._1, distances.maxBy(_._2)._2)
  }

  /**
    * Calculate distance from a point, in degrees.
    *
    * Note: normally east/west would be the largest in degrees, but sometimes it can be smaller
    * due to variances in the ellipsoid
    *
    * @param point point to buffer
    * @param meters distance to buffer, in meters
    * @param calc calculator, for re-use
    * @return (east/west, north/south) distance in degrees
    */
  def directionalDegrees(point: Point, meters: Double, calc: GeodeticCalculator): (Double, Double) = {
    calc.setStartingGeographicPoint(point.getX, point.getY)
    def degrees(azimuth: Double): Double = {
      calc.setDirection(azimuth, meters)
      val dest = calc.getDestinationGeographicPoint
      point.distance(geoFactory.createPoint(new Coordinate(dest.getX, dest.getY)))
    }
    // take min distance to account for crossing the AM/poles
    (math.min(degrees(90), degrees(-90)), math.min(degrees(0), degrees(180)))
  }

  /** Adds way points to Seq[Coordinates] so that they remain valid with Spatial4j, useful for BBOX */
  def addWayPoints(coords: Seq[Coordinate]): List[Coordinate] = {
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

  private def unfoldRight[A, B](seed: B)(f: B => Option[(A, B)]): List[A] = f(seed) match {
    case None => Nil
    case Some((a, b)) => a :: unfoldRight(b)(f)
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
    * Returns the rough bounds of a geometry, decomposing the geometry to provide better accuracy
    *
    * @param geometry geometry
    * @param maxBounds maximum number of bounds that will be returned
    * @param maxBitResolution maximum bit resolution to decompose to
    *                         must be between 1-63, inclusive
    * @return seq of (xmin, ymin, xmax, ymax)
    */
  def bounds(geometry: Geometry, maxBounds: Int, maxBitResolution: Int): Seq[(Double, Double, Double, Double)] = {
    if (maxBounds < 2 || GeometryUtils.isRectangular(geometry)) {
      return Seq(bounds(geometry))
    }

    try {
      // use `maxBitResolution | 1` to ensure oddness, which is required by GeohashUtils
      val resolution = ResolutionRange(0, maxBitResolution | 1, 5)
      val geohashes = GeohashUtils.decomposeGeometry(geometry, maxBounds, resolution, relaxFit = true)
      geohashes.map(gh => (gh.bbox.ll.getX, gh.bbox.ll.getY, gh.bbox.ur.getX, gh.bbox.ur.getY))
    } catch {
      case NonFatal(e) =>
        logger.error("Error decomposing geometry, falling back to envelope bounds:", e)
        Seq(bounds(geometry))
    }
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
    case p: Polygon => isRectangular(p)
    case _ => false
  }

  /**
    * Checks that a polygon is rectangular and has no interior holes
    *
    * @param p polygon
    * @return
    */
  def isRectangular(p: Polygon): Boolean = {
    if (p.isEmpty) {
      true
    } else if (p.getNumInteriorRing != 0) {
      // checks that there are no interior holes
      false
    } else {
      val env = p.getEnvelopeInternal
      val (xmin, ymin, xmax, ymax) = (env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)

      // checks that all points are on the exterior envelope of the polygon
      def cutout(c: Coordinate): Boolean = c.x != xmin && c.x != xmax && c.y != ymin && c.y != ymax

      val coords = p.getCoordinates // note: getCoordinates constructs an array so just call once
      var i = 1
      while (i < coords.length) {
        val c = coords(i)
        // checks that there aren't any cutouts or angled lines
        if (cutout(c) || (c.x != coords(i - 1).x && c.y != coords(i - 1).y)) {
          return false
        }
        i += 1
      }
      // check final coord cutout
      !cutout(coords(0))
    }
  }

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
      if (translated.getMinX < -180d && translated.getMaxX < 180d) {
        val trimmed = new ReferencedEnvelope(-180d , translated.getMaxX, envelope.getMinY, envelope.getMaxY, crs)
        val wrapped = new ReferencedEnvelope(translated.getMinX + 360d , 180d, envelope.getMinY, envelope.getMaxY, crs)
        Seq(trimmed, wrapped)
      } else if (translated.getMaxX > 180d && translated.getMinX > -180d) {
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
