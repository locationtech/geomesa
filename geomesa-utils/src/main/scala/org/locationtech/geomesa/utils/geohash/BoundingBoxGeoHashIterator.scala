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

import java.util.Iterator

import scala.collection.mutable.TreeSet

/**
 *  This iterator traverses a bounding box returning the GeoHashes inside the box
 *   in "z-order" starting with the lower left GeoHash and finishing with the upper
 *   right GeoHash.
 */
class BoundingBoxGeoHashIterator(twoGh: TwoGeoHashBoundingBox) extends Iterator[GeoHash] {
  val (latSteps, lonSteps) = GeoHash.getLatitudeLongitudeSpanCount(twoGh.ll, twoGh.ur, twoGh.prec)

  val Array(endLatIndex, endLonIndex) = GeoHash.gridIndicesForLatLong(twoGh.ur)

  // This is the number of GeoHashes that our iterator will return.
  val ns = latSteps*lonSteps

  // We maintain a queue of possible next available GeoHashes.
  val queue = TreeSet[GeoHash](twoGh.ll)

  var nextGh = twoGh.ll

  def hasNext(): Boolean = queue.nonEmpty

  def next(): GeoHash = {
    if (hasNext) {
      // The next GeoHash is the least of the candidates in the queue.
      nextGh = queue.head

      // Standing at the "next" GeoHash, we need to compute the GeoHash to right and the one above.
      val latIndex = GeoHash.gridIndexForLatitude(nextGh)
      val lonIndex = GeoHash.gridIndexForLongitude(nextGh)

      if (lonIndex + 1 <= endLonIndex) {
        val nextLonGh = GeoHash.composeGeoHashFromBitIndicesAndPrec(
          latIndex, lonIndex + 1, nextGh.prec)

        if (twoGh.bbox.covers(nextLonGh.getPoint))
          queue add nextLonGh
      }

      if (latIndex + 1 <= endLatIndex) {
        val nextLatGh = GeoHash.composeGeoHashFromBitIndicesAndPrec(
          latIndex + 1, lonIndex, nextGh.prec)

        // If the calculated GeoHashes are still with the box, we add them to the queue.
        if (twoGh.bbox.covers(nextLatGh.getPoint))
          queue add nextLatGh
      }

      queue.remove(nextGh)
      nextGh
    }
    else throw new NoSuchElementException("No more geohashes available in iterator")
  }

  def remove = throw new UnsupportedOperationException("Remove operation not supported")
}