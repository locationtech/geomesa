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

object TwoGeoHashBoundingBox {
  def apply(bbox: BoundingBox, prec: Int): TwoGeoHashBoundingBox =
    TwoGeoHashBoundingBox(GeoHash(bbox.ll, prec), GeoHash(bbox.ur, prec))
}

case class TwoGeoHashBoundingBox(ll: GeoHash, ur: GeoHash) {
  require(ll.prec == ur.prec,
          "Both bounding boxes in a TwoGeoHashBoundingBox must have the same precisions")

  lazy val bbox = BoundingBox.getCoveringBoundingBox(ll.bbox, ur.bbox)

  lazy val prec = ll.prec // same as ur.prec

  def toBase32: String = ll.hash + ur.hash
}