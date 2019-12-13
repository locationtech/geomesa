/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.data

import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeoHash, GeohashUtils}
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ClosestAcceptableGeoHashTest extends Specification {

  sequential

  def testClosestAcceptableGeoHash(xMin: Double, xMax: Double, yMin: Double, yMax: Double, expected: String) = {
    val bbox = BoundingBox(xMin, xMax, yMin, yMax)
    val expectedGH = GeoHash(expected)
    val resultGH   = GeohashUtils.getClosestAcceptableGeoHash(bbox)

    resultGH.getOrElse(GeoHash("")).equals(expectedGH)
  }

  "Closest Acceptable GeoHash function" should {
    "Given some bounds that conforms exactly to a GeoHash, return that GeoHash" in {
      val d = GeoHash("d")

      val result = GeohashUtils.getClosestAcceptableGeoHash(d.bbox)

      result must beSome[GeoHash]
      result.get.hash must beEqualTo("d")
    }

    "Given some bounds that are slightly smaller than a GeoHash, return that GeoHash" in {
      val d = GeoHash("d")
      val newEnv = d.geom.buffer(-0.5).getEnvelopeInternal
      val bbox = BoundingBox(newEnv)

      val result = GeohashUtils.getClosestAcceptableGeoHash(bbox)

      result must beSome[GeoHash]
      result.get.hash must beEqualTo("d")
    }

    "Given bounds outside the world, return None" in {
      val bbox = BoundingBox(0, 180, 90, 90)

      val result = GeohashUtils.getClosestAcceptableGeoHash(bbox)
      result must beNone
    }

    "Given bounds outside the world again, return None" in {
      val bbox = BoundingBox(-180, 0, 90, 90)

      val result = GeohashUtils.getClosestAcceptableGeoHash(bbox)
      result must beNone
    }

    "Given a QLevel 1 BoundingBox '-90.0, -67.5, 22.5, 45.0', the closest acceptable GeoHash must be 'd' " in {
      testClosestAcceptableGeoHash(-90.0, -67.5, 22.5, 45.0, "d") must beTrue
    }

    "Given a QLevel 2 BoundingBox '-78.75, -67.5, 33.75, 45.0', the closest acceptable GeoHash must be 'd' " in {
      testClosestAcceptableGeoHash(-78.75, -67.5, 33.75, 45.0, "d") must beTrue
    }

    "Given a QLevel 3 BoundingBox '-78.75, -73.125, 33.75, 39.375', the closest acceptable GeoHash must be 'dq' " in {
      testClosestAcceptableGeoHash(-78.75, -73.125, 33.75, 39.375, "dq") must beTrue
    }

    "Given a QLevel 4 BoundingBox '-78.75, -75.9375, 36.5625, 39.375', the closest acceptable GeoHash must be 'dq' " in {
      testClosestAcceptableGeoHash(-78.75, -75.9375, 36.5625, 39.375, "dq") must beTrue
    }

    "Given a QLevel 5 BoundingBox '-78.75, -77.34375, 37.96875, 39.375', the closest acceptable GeoHash must be 'dqb' " in {
      testClosestAcceptableGeoHash(-78.75, -77.34375, 37.96875, 39.375, "dqb") must beTrue
    }

    "Given a QLevel 6 BoundingBox '-78.75, -78.046875, 38.671875, 39.375', the closest acceptable GeoHash must be 'dqb' " in {
      testClosestAcceptableGeoHash(-78.75, -78.046875, 38.671875, 39.375, "dqb") must beTrue
    }

    "Given a QLevel 7 BoundingBox '-78.75, -78.3984375, 39.0234375, 39.375', the closest acceptable GeoHash must be 'dqb' " in {
      testClosestAcceptableGeoHash(-78.75, -78.3984375, 39.0234375, 39.375, "dqb") must beTrue
    }

    "Given a QLevel 8 BoundingBox '-78.75, -78.57421875, 39.19921875, 39.375', the closest acceptable GeoHash must be 'dqbp' " in {
      testClosestAcceptableGeoHash(-78.75, -78.57421875, 39.19921875, 39.375, "dqbp") must beTrue
    }

    "Given a QLevel 9 BoundingBox '-78.75, -78.662109375, 39.287109375, 39.375', the closest acceptable GeoHash must be 'dqbp' " in {
      testClosestAcceptableGeoHash(-78.75, -78.662109375, 39.287109375, 39.375, "dqbp") must beTrue
    }

    "Given a QLevel 10 BoundingBox '-78.75, -78.7060546875, 39.3310546875, 39.375', the closest acceptable GeoHash must be 'dqbpb' " in {
      testClosestAcceptableGeoHash(-78.75, -78.7060546875, 39.3310546875, 39.375, "dqbpb") must beTrue
    }

    "Given a QLevel 11 BoundingBox '-78.75, -78.72802734375, 39.35302734375, 39.375', the closest acceptable GeoHash must be 'dqbpb' " in {
      testClosestAcceptableGeoHash(-78.75, -78.72802734375, 39.35302734375, 39.375, "dqbpb") must beTrue
    }

    "Given a QLevel 12 BoundingBox '-78.75, -78.739013671875, 39.364013671875, 39.375', the closest acceptable GeoHash must be 'dqbpb' " in {
      testClosestAcceptableGeoHash(-78.75, -78.739013671875, 39.364013671875, 39.375, "dqbpb") must beTrue
    }

    "Given a QLevel 13 BoundingBox '-78.75, -78.7445068359375, 39.3695068359375, 39.375', the closest acceptable GeoHash must be 'dqbpbp' " in {
      testClosestAcceptableGeoHash(-78.75, -78.7445068359375, 39.3695068359375, 39.375, "dqbpbp") must beTrue
    }

    "Given a QLevel 14 BoundingBox '-78.75, -78.74725341796875, 39.37225341796875, 39.375', the closest acceptable GeoHash must be 'dqbpbp' " in {
      testClosestAcceptableGeoHash(-78.75, -78.74725341796875, 39.37225341796875, 39.375, "dqbpbp") must beTrue
    }

    "Given a QLevel 15 BoundingBox '-78.75, -78.74862670898438, 39.373626708984375, 39.375', the closest acceptable GeoHash must be 'dqbpbpb' " in {
      testClosestAcceptableGeoHash(-78.75, -78.74862670898438, 39.373626708984375, 39.375, "dqbpbpb") must beTrue
    }


  }

}
