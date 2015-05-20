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

package org.locationtech.geomesa.accumulo.process.knn

import com.vividsolutions.jts.geom.Point
import org.locationtech.geomesa.utils.geohash.VincentyModel
import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeature
import org.opengis.feature.simple.SimpleFeature

import scala.annotation.tailrec
import scala.collection.mutable

case class SimpleFeatureWithDistance(sf: SimpleFeature, dist: Double)

trait NearestNeighborsMethods {
  // distance to reference point
  def distance(sf: SimpleFeature): Double

  // distance of kth point from reference point, or furthest found so far if size is < K
  def maxDistance: Option[Double]

  // this is "k" --- the desired number of nearest neighbors
  def maxSize: Int

  // current size of the collection, which may be more or less than maxSize in the current implementation
  def size: Int

  // indicates if size is >= maxSize
  def isFull: Boolean

  //  provides a view of the Kth nearest neighbor, or furthest found if the size is < K
  def peekLast: Option[SimpleFeatureWithDistance]

  // adds a collection of SimpleFeatureWithDistance to the NearestNeighbors
  def add(sfWDC: Iterable[SimpleFeatureWithDistance]): Unit

  // adds a single SimpleFeatureWithDistance to the NearestNeighbors
  def add(sfWD: SimpleFeatureWithDistance): Unit

  // returns a NearestNeighbors object, trimmed to contain the "best K" neighbors
  def getKNN: NearestNeighbors

  // return a List of the K NearestNeighbors in sorted order.
  def getK: List[SimpleFeatureWithDistance]
}

/**
 *  This class provides a collection of SimpleFeatures sorted by distance from a central POINT.
 *  This is currently implemented using transactions with a scala mutable PriorityQueue
 *
 */
object NearestNeighbors {
  def apply(aFeatureForSearch: SimpleFeature, numDesired: Int): NearestNeighbors = {
    aFeatureForSearch.point match {
      case aPoint: Point => NearestNeighbors(aPoint, numDesired)
      case _ => throw new RuntimeException("NearestNeighbors not implemented for non-point geometries")
    }
  }

  def apply(aPointForSearch: Point, numDesired: Int): NearestNeighbors = {

    def distanceCalc(sf: SimpleFeature) =
      VincentyModel.getDistanceBetweenTwoPoints(aPointForSearch, sf.point).getDistanceInMeters

    implicit val orderedSF: Ordering[SimpleFeatureWithDistance] = Ordering.by {_.dist}

    new NearestNeighbors(numDesired, distanceCalc)(orderedSF.reverse)
  }
}

class NearestNeighbors(val maxSize: Int,
                       distanceCalc: SimpleFeature => Double)(implicit ord: Ordering[SimpleFeatureWithDistance])
  extends NearestNeighborsMethods {

  val corePQ = mutable.PriorityQueue[SimpleFeatureWithDistance]()

  def distance(sf: SimpleFeature) = distanceCalc(sf)

  def maxDistance = peekLast.map {_.dist}

  def isFull = !(corePQ.length < maxSize)

  @tailrec
  final def dequeueN(n: Int, list:List[SimpleFeatureWithDistance]): List[SimpleFeatureWithDistance] = {
    if (corePQ.isEmpty || list.length == n) list.reverse
    else {
      val newList = corePQ.dequeue() :: list
      dequeueN(n,newList)
    }
  }

  def peekLast = getK.lastOption

  def getKNN = {
    if (isFull) {
      val that = new NearestNeighbors(maxSize, distance)
      that.add(this.dequeueN(maxSize,List[SimpleFeatureWithDistance]()))
      that
    } else this
  }

  def getK = clone().dequeueN(maxSize,List[SimpleFeatureWithDistance]())

  def add(sfWDC: Iterable[SimpleFeatureWithDistance]) = sfWDC.map(add)

  def add(sfWD: SimpleFeatureWithDistance) = corePQ.enqueue(sfWD)

  def size = corePQ.size

  override def clone() = {
    val that = new NearestNeighbors(maxSize, distance)
    that.corePQ ++= this.corePQ
    that
  }
}

