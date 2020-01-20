/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geometry

import org.geotools.referencing.GeodeticCalculator
import org.locationtech.geomesa.utils.geometry.DistanceCalculator.{Distance, MaxDistance, MinDistance}
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.locationtech.jts.geom.Point

/**
  * Calculator for distances between geometries. Not thread safe
  */
class DistanceCalculator {

  private val calc = new GeodeticCalculator()

  /**
    * Calculates the distance between two points, in meters
    *
    * @param a point a
    * @param b point b
    * @return distance in meters
    */
  def meters(a: Point, b: Point): Double = {
    calc.setStartingGeographicPoint(a.getX, a.getY)
    calc.setDestinationGeographicPoint(b.getX, b.getY)
    calc.getOrthodromicDistance
  }

  /**
    * Calculates the distance between two points, in meters and degrees
    *
    * @param a point a
    * @param b point b
    * @return distance
    */
  def distance(a: Point, b: Point): Distance = distance(a, b, meters(a, b))

  /**
    * Calculates the min and max degrees between two points, where the distance in meters has already been
    * determined
    *
    * @param a point a
    * @param b point b
    * @param meters distance between point a and point b, in meters
    * @return distance
    */
  def distance(a: Point, b: Point, meters: Double): Distance = {
    val (a0, a1) = GeometryUtils.distanceDegrees(a, meters, calc)
    val (b0, b1) = GeometryUtils.distanceDegrees(b, meters, calc)
    Distance(meters, math.min(a0, b0), math.max(a1, b1))
  }

  /**
    * Calculates the distance between two points in meters, and the min possible distance in degrees
    *
    * @param a point a
    * @param b point b
    * @return min distance
    */
  def min(a: Point, b: Point): MinDistance = {
    val m = meters(a, b)
    MinDistance(m, min(a, b, m))
  }

  /**
    * Calculates the min possible distance between two points, in degrees, where the distance
    * in meters is already known
    *
    * @param a point a
    * @param b point b
    * @param meters distance between point a and point b in meters
    * @return min possible distance in degrees (generally due N/S)
    */
  def min(a: Point, b: Point, meters: Double): Double = {
    val amin = GeometryUtils.distanceDegrees(a, meters, calc)._1
    val bmin = GeometryUtils.distanceDegrees(b, meters, calc)._1
    math.min(amin, bmin)
  }

  /**
    * Calculates the distance between two points in meters, and the max possible distance in degrees
    *
    * @param a point a
    * @param b point b
    * @return max distance
    */
  def max(a: Point, b: Point): MaxDistance = {
    val m = meters(a, b)
    MaxDistance(m, max(a, b, m))
  }

  /**
    * Calculates the max possible distance between two points, in degrees, where the distance
    * in meters is already known
    *
    * @param a point a
    * @param b point b
    * @param meters distance between point a and point b in meters
    * @return max possible distance in degrees (generally due E/W)
    */
  def max(a: Point, b: Point, meters: Double): Double = {
    val amax = GeometryUtils.distanceDegrees(a, meters, calc)._2
    val bmax = GeometryUtils.distanceDegrees(b, meters, calc)._2
    math.max(amax, bmax)
  }
}

object DistanceCalculator {

  /**
    * Distance relative to some point
    *
    * @param meters distance in meters
    * @param minDegrees min possible distance in degrees (generally due N/S)
    * @param maxDegrees max possible distance in degrees (generally due E/W)
    */
  case class Distance(meters: Double, minDegrees: Double, maxDegrees: Double)

  /**
    * Distance relative to some point
    *
    * @param meters distance in meters
    * @param degrees min possible distance in degrees (generally due N/S)
    */
  case class MinDistance(meters: Double, degrees: Double)

  /**
    * Distance relative to some point
    *
    * @param meters distance in meters
    * @param degrees max possible distance in degrees (generally due E/W)
    */
  case class MaxDistance(meters: Double, degrees: Double)
}
