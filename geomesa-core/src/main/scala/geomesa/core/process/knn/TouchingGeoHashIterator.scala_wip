/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.utils.geohash

import GeoHashIterator._
import com.vividsolutions.jts.geom.Coordinate
import com.vividsolutions.jts.geom.Point

import scala.collection.immutable.NumericRange


object TouchingGeoHashIterator {
  def apply(lat: Double, lon: Double, radius: Double = 0.0, precision: Int): TouchingGeoHashIterator =
    apply(geometryFactory.createPoint(new Coordinate(lat, lon)), radius, precision)
  def apply(center: Point, radius: Double, precision: Int): TouchingGeoHashIterator = {
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
class TouchingGeoHashIterator(center: Point,
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

object OldTouchingMethod {
  // return all GeoHashes of the same precision that are in contact with this one,
  def touching(gh: GeoHash): Seq[GeoHash] = {
    val thisLoc = gh.getPoint
    val thisPrec = gh.prec
    val thisLatPrec = GeoHash.latitudeDeltaForPrecision(thisPrec)
    val thisLonPrec = GeoHash.longitudeDeltaForPrecision(thisPrec)
    // get the precision (is lat and lon degrees the same?)
    // get the GeoHashes that lie -1<x<1 and -1<y<1 from this one,
    // treating the bonds at the poles and antimeridian correctly
    // this needs to filter out the (0,0) case
    val shiftPattern = for {
      i <- List(-1, 0, 1)
      j <- List(-1, 0, 1)
    } yield (i, j)
    // get a list of new coordinates
    // NOTE THAT BLINDY JUMPING BY TWICE THE PRECISION IS NOT GOING TO PLACE
    // the position squarely into the next geohash.
    // I need to check that there are no overshoots.
    val newCoordShifts = shiftPattern.map { shift => (2.0 * thisLonPrec * x, 2.0 * thisLatPrec * y)}
    // apply the coordinate shifts to the center point
    // if there are any that extend to the pole, I need to wrap around the pole completely
    // by taking the original latitude and cycling through all longs
    // if there are any that jump over the antimeridian, I need to
    // just flip the sign of the longitude.

    def handlePoles(newPoint: Point): List[Point] = {
      if poleCrossing(newPoint) {
        generatePoleWrap(thisLoc, thisPrec) // we actually don't care if we've also jumped over the meridian in this case
      }
      else if antimeridianCrossing(newPoint)
      else
        List(newPoint)
    }
    def poleCrossing(aPoint: Point): Boolean = math.abs(aPoint.getY) > 90
    def antimeridianCrossing(aPoint: Point): Boolean = math.abs(aPoint.getX) > 180
    def generatePoleWrap(aPoint: Point, aPrec: Int): List[Point] = {
      val aLat = aPoint.getY // extract  the latitude of the original point -- it will be used for the wrap
      val lonDelta = GeoHash.lonDeltaMap(aPrec) // get a array of all longitude centers
      //powersOf2Map(aPrec)//
      // I need to confirm that this works.
      val allLongtitudes = NumericRange(-180 + lonDelta, 180.- lonDelta, 2.* lonDelta)
      // generate the list
    }
}