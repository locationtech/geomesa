/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geohash

import org.locationtech.jts.geom.{Coordinate, Point}
import org.locationtech.geomesa.utils.geohash.GeoHashIterator._


object RadialGeoHashIterator {
  def apply(lat: Double, lon: Double, radius: Double = 0.0, precision: Int): RadialGeoHashIterator =
    apply(geometryFactory.createPoint(new Coordinate(lat, lon)), radius, precision)
  def apply(center: Point, radius: Double, precision: Int): RadialGeoHashIterator = {
    val latPrecision = getDimensionPrecisionInMeters(center.getY, true, precision >> 1)
    val lonPrecision = getDimensionPrecisionInMeters(center.getY, false, (1 + precision) >> 1)
    val effectiveRadius = Math.hypot(radius + 0.6 * latPrecision, radius + 0.6 * lonPrecision)
    val ptLL = VincentyModel.moveWithBearingAndDistance(center, -135.0, effectiveRadius)
    val ptUR = VincentyModel.moveWithBearingAndDistance(center,   45.0, effectiveRadius)
    new RadialGeoHashIterator(center, ptLL.getY, ptLL.getX, ptUR.getY, ptUR.getX, radius, precision)
  }

}

/**
 * Built upon a rectangular iterator, this class makes some estimates about in-circle
 * membership.
 */
class RadialGeoHashIterator(center: Point,
                            llLat: Double,
                            llLon: Double,
                            urLat: Double,
                            urLon: Double,
                            radiusMeters: Double = 0.0,
                            precision: Int)
    extends RectangleGeoHashIterator(llLat, llLon, urLat, urLon, precision) {

  private var currentRadius = Double.NaN
  def distanceInMeters = currentRadius  // prevents outside modifications

  @Override
  override def advance: Boolean = {
    var isDone = false
    while (!isDone) {
      if (!super.advance()) {
        currentRadius = Double.NaN
        return false
      }
      currentRadius = VincentyModel.getDistanceBetweenTwoPoints(center, currentPoint)
                                   .getDistanceInMeters
      isDone = currentRadius <= radiusMeters
    }
    true
  }
}