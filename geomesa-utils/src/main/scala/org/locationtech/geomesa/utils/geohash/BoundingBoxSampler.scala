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

import scala.annotation.tailrec
import scala.collection.mutable
import scala.util.Random

class BoundingBoxSampler(twoGh: TwoGeoHashBoundingBox) extends Iterator[GeoHash] {
  val rand = new Random
  val (latSteps, lonSteps) = GeoHash.getLatitudeLongitudeSpanCount(twoGh.ll, twoGh.ur, twoGh.prec)

  val numPoints = latSteps*lonSteps

  require(numPoints <= Integer.MAX_VALUE, "Bounding box too big, cannot sample.")
  require(numPoints > 1, "Only one point in bounding box, cannot sample.")

  private val used = mutable.HashSet[Integer]()

  override def hasNext = used.size < numPoints

  @tailrec
  final override def next(): GeoHash = {
    if(!hasNext) throw new NoSuchElementException("No more points available.")

    var idx = rand.nextInt(numPoints)
    while (used.contains(idx)) {
      idx = rand.nextInt(numPoints)
    }
    used.add(idx)

    val gh = GeoHash.composeGeoHashFromBitIndicesAndPrec(
               GeoHash.gridIndexForLatitude(twoGh.ll) + idx / lonSteps,
               GeoHash.gridIndexForLongitude(twoGh.ll) + idx % lonSteps,
               twoGh.ll.prec)

    if(twoGh.bbox.covers(gh.bbox)) gh else next
  }

}

class WithReplacementBoundingBoxSampler(twoGh: TwoGeoHashBoundingBox) extends Iterator[GeoHash] {
  var internalSampler = new BoundingBoxSampler(twoGh)

  private def checkInternal() = if (!internalSampler.hasNext) {
    internalSampler = new BoundingBoxSampler(twoGh)
  }

  override def hasNext = {
    checkInternal()
    internalSampler.hasNext //should always be true unless something has gone horribly wrong
  }

  final override def next() = {
    checkInternal()
    internalSampler.next()
  }
}
