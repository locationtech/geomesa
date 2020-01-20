/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import org.geotools.geometry.jts.ReferencedEnvelope
import org.locationtech.jts.geom.Coordinate
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeometryUtilsTest extends Specification {

  "Geometry Utils" should {

    "calculate intercept of anti-meridian" >> {
      val p1 = new Coordinate(179, 5, 0)
      val p2 = new Coordinate(-179, 7, 0)
      (GeometryUtils.calcIDLIntercept(p1, p2).toInt, GeometryUtils.calcIDLIntercept(p2,p1).toInt) mustEqual (6, 6)
    }

    "calculate intercept of arbitrary longitude" >> {
      val p1 = new Coordinate(170, 5, 0)
      val p2 = new Coordinate(172, 7, 0)
      (GeometryUtils.calcCrossLat(p1, p2, 171).toInt, GeometryUtils.calcCrossLat(p2, p1, 171).toInt) mustEqual (6, 6)
    }

    "calculate min/max degrees" >> {
      foreach(Seq("0 0", "0.0005 0.0005", "-0.0005 -0.0005", "45 45")) { pt =>
        val (min, max) = GeometryUtils.distanceDegrees(WKTUtils.read(s"POINT($pt)"), 150d)
        min must beLessThanOrEqualTo(max)
      }
    }

    "split bounding box envelopes" >> {
      val unchanged = Seq(
        new ReferencedEnvelope(-180, 180, -90, 90, CRS_EPSG_4326),
        new ReferencedEnvelope(-145, 0, -75, -55, CRS_EPSG_4326),
        new ReferencedEnvelope(145, 155, -75, 75, CRS_EPSG_4326)
      )
      foreach(unchanged) { env =>
        GeometryUtils.splitBoundingBox(env) mustEqual Seq(env)
      }

      val high = new ReferencedEnvelope(102.48046875, 237.48046875, -47.63671875, 13.88671875, CRS_EPSG_4326)
      GeometryUtils.splitBoundingBox(high) mustEqual Seq(
        new ReferencedEnvelope(102.48046875, 180, -47.63671875, 13.88671875, CRS_EPSG_4326),
        new ReferencedEnvelope(-180, -122.51953125, -47.63671875, 13.88671875, CRS_EPSG_4326)
      )

      val low = new ReferencedEnvelope(-291.796875, -21.796875, -41.1328125, 81.9140625, CRS_EPSG_4326)
      GeometryUtils.splitBoundingBox(low) mustEqual Seq(
        new ReferencedEnvelope(-180, -21.796875, -41.1328125, 81.9140625, CRS_EPSG_4326),
        new ReferencedEnvelope(68.203125, 180, -41.1328125, 81.9140625, CRS_EPSG_4326)
      )

      val outside = new ReferencedEnvelope(-438.52377, -438.44636, 38.00967, 38.07052, CRS_EPSG_4326)
      val translated = GeometryUtils.splitBoundingBox(outside)
      translated must haveLength(1)
      translated.head.getMinX must beCloseTo(-78.52377, 0.0001)
      translated.head.getMaxX must beCloseTo(-78.44636, 0.0001)
      translated.head.getMinY mustEqual 38.00967
      translated.head.getMaxY mustEqual 38.07052

      val covers = new ReferencedEnvelope(-200, 200, -100, 100, CRS_EPSG_4326)
      GeometryUtils.splitBoundingBox(covers) mustEqual Seq(covers)
    }
  }
}
