/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geohash

import java.awt.geom.Point2D

import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point, PrecisionModel}
import org.geotools.referencing.GeodeticCalculator
import org.locationtech.geomesa.utils.text.ObjectPoolFactory

/**
 * Encapsulates the notion of a geographic distance, and is primarily intended
 * to simplify the expression of constants.
 */
trait GeomDistance {
  /**
   * Simple class, and its companion, to allow us to express constraint distances
   * a bit more naturally.
   *
   * @param distanceInMeters the distance in meters for this description
   */
  class Distance(distanceInMeters:Double) {
    // conversion functions to be called post-fix, metric
    def meter : Distance = new Distance(distanceInMeters)
    def meters : Distance = meter
    def kilometer : Distance = new Distance(distanceInMeters * 1000.0)
    def kilometers : Distance = kilometer

    // conversion functions to be called post-fix, imperial
    def foot : Distance = new Distance(distanceInMeters * Distance.METERS_PER_FOOT)
    def feet : Distance = foot
    def mile : Distance = new Distance(distanceInMeters * Distance.METERS_PER_FOOT * 5280.0)
    def miles : Distance = mile

    // supports an implicit call in the object to convert back to a Double
    def getDistanceInMeters : Double = distanceInMeters

    override def toString : String = {
      distanceInMeters match {
        case m if m<1000.0 => "%1.1f meters".format(m)
        case m => "%1.1f kilometers".format(m / 1000.0)
      }
    }
  }

  object Distance {
    val METERS_PER_FOOT : Double = 12.0 * 2.54 / 100.0

    // these take care of ensuring that "1 kilometer" is used as
    // "(new Distance(1)).kilometer"
    implicit def double2distance(x:Double) : Distance = new Distance(x)
    implicit def int2distance(x:Int) : Distance = new Distance(x.asInstanceOf[Double])
    implicit def long2distance(x:Long) : Distance = new Distance(x.asInstanceOf[Double])

    // this takes care of ensuring that "1 kilometer", when used as a Double,
    // can be converted back reasonably
    implicit def distance2double(x:Distance) : Double = x.getDistanceInMeters
  }
}

/**
 *  Utility object for computing distances between two points.  The points
 *  are assumed to be specified using WGS-84.
 *
 *  This implementation depends on docs.geotools.org/latest/javadocs/org/geotools/referencing/GeodeticCalculator.html
 *  Which is backed by Thaddeus Vincenty's formulas for calculating distances on an ellipsoid
 *  http://en.wikipedia.org/wiki/Vincenty%27s_formulae
 */

object VincentyModel extends GeomDistance {
  private val geometryFactory = new GeometryFactory(new PrecisionModel, 4326)
  private val geodeticCalculatorPool = ObjectPoolFactory { new GeodeticCalculator }

  /**
   * Computation of the distance between two points.
   *
   * @param a  The starting point
   * @param b  The end point
   * @return   The distance between the two points
   */
  def getDistanceBetweenTwoPoints(a:Point, b:Point) : Distance =
    getDistanceBetweenTwoPoints(a.getX, a.getY, b.getX, b.getY)

  /**
   * Computation of the distance between two points.
   *
   * @param x1   The starting point's x value
   * @param y1   The starting point's y value
   * @param x2   The ending point's x value
   * @param y2   The ending point's y value
   * @return   The distance between the two points
   */
  def getDistanceBetweenTwoPoints(x1: Double, y1: Double, x2: Double, y2: Double) : Distance =
    geodeticCalculatorPool.withResource{ calc => {
      calc.setStartingGeographicPoint(x1, y1)
      calc.setDestinationGeographicPoint(x2, y2)
      new Distance(calc.getOrthodromicDistance)
    }}

  /**
   *
   * @param a         The starting point
   * @param bearing    The bearing expressed in decimal degrees from -180° to 180°.
   *                   NB:  0° is North, 90° is East, (-)180° is South, and West is -90°.
   * @param distance   The orthodromic distance from the starting point expressed in meters.
   * @return           The destination point.
   */
  def moveWithBearingAndDistance(a: Point, bearing: Double, distance: Double): Point =
    moveWithBearingAndDistance(a.getX, a.getY, bearing, distance)

  /**
   *
   * @param x         The starting point's x value
   * @param y         The starting point's y value
   * @param bearing    The bearing expressed in decimal degrees from -180° to 180°.
   *                   NB:  0° is North, 90° is East, (-)180° is South, and West is -90°.
   * @param distance   The orthodromic distance from the starting point expressed in meters.
   * @return           The destination point.
   */
  def moveWithBearingAndDistance(x: Double, y: Double, bearing: Double, distance: Double): Point =
    geodeticCalculatorPool.withResource{ calc => {
      calc.setStartingGeographicPoint(x, y)
      calc.setDirection(bearing, distance)
      val point: Point2D = calc.getDestinationGeographicPoint
      geometryFactory.createPoint(new Coordinate(point.getX, point.getY))
    }}
}