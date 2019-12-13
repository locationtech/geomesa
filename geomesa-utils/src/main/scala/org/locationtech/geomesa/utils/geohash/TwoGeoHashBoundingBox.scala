/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

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