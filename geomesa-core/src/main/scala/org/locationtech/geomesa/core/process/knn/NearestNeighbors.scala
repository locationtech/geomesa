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

package org.locationtech.geomesa.core.process.knn

import com.vividsolutions.jts.geom.Point
import org.locationtech.geomesa.utils.geohash.VincentyModel
import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature

case class SimpleFeatureWithDistance(sf: SimpleFeature, dist: Double)

trait NearestNeighbors {
  def distance(sf: SimpleFeature): Double

  def maxDistance: Option[Double]
}

/**
 *  This object provides a PriorityQueue of SimpleFeatures sorted by distance from a central POINT.
 *
 */
object NearestNeighbors {
  def apply(aFeatureForSearch: SimpleFeature, numDesired: Int): BoundedNearestNeighbors[SimpleFeatureWithDistance] = {
    aFeatureForSearch.point match {
      case aPoint: Point => NearestNeighbors(aPoint, numDesired)
      case _ => throw new RuntimeException("NearestNeighbors not implemented for non-point geometries")
    }
  }

  def apply(aPointForSearch: Point, numDesired: Int): BoundedNearestNeighbors[SimpleFeatureWithDistance] = {

    def distanceCalc(sf: SimpleFeature) =
      VincentyModel.getDistanceBetweenTwoPoints(aPointForSearch, sf.point).getDistanceInMeters

    implicit val orderedSF: Ordering[SimpleFeatureWithDistance] = Ordering.by { _.dist }

    // type aliased to  BoundedNearestNeighbors
    new BoundedPriorityQueue[SimpleFeatureWithDistance](numDesired)(orderedSF.reverse) with NearestNeighbors {

      def distance(sf: SimpleFeature) = distanceCalc(sf)

      def maxDistance = Option(last).map {_.dist }
    }
  }
}

