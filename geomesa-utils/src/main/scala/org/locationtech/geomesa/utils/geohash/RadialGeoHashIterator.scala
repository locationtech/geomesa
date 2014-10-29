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

package org.locationtech.geomesa.utils.geohash

import com.vividsolutions.jts.geom.{Coordinate, Point}
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