/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.geohash

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class BoundingBoxTest extends Specification {
  "BoundingBox" should {
    "return appropriate hashes" >> {
      val bbox = BoundingBox.apply(GeoHash.apply("dqb00").getPoint, GeoHash.apply("dqbxx").getPoint)
      val hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox)
      hashes must haveLength(24)
    }
    "return appropriate hashes" >> {
      val bbox = BoundingBox.apply(-78, -77.895029, 38.045834, 38)
      val hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox, 32)
      hashes must haveLength(6)
    }
    "return appropriate hashes" >> {
      val bbox = BoundingBox.apply(-78, -77.89503, 38.0458335, 38)
      val hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox, 32)
      hashes must haveLength(6)
    }
    "return appropriate hashes" >> {
      val bbox = BoundingBox.apply(-50, 50, -40, 40)
      val hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox, 32)
      hashes must haveLength(8)
    }
    "return appropriate hashes" >> {
      val bbox = BoundingBox.apply(1, 1, 1, 1)
      val hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox, 32)
      hashes must haveLength(1)
    }
  }
}
