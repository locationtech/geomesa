/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geohash

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom.Point
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.BitSet
import scala.math._

@RunWith(classOf[JUnitRunner])
class GeoHashTest extends Specification
                          with LazyLogging {

  // set to true to see test output
  val DEBUG_OUPUT = false

  // compute tolerance based on precision
  val precToleranceMap = (0 to 63).map(i => (i, 360.0 * pow(0.5, floor(i/2)))).toMap
  def xTolerance(prec: Int) = precToleranceMap(prec)
  def yTolerance(prec: Int) = precToleranceMap(prec) * 0.5

  "ezs42" should {
    "decode to -5.6, 42.6" in {
      val gh = GeoHash("ezs42")
      gh.x must beCloseTo(-5.6, xTolerance(25))
      gh.y must beCloseTo(42.6, yTolerance(25))
    }
  }

  "-5.6, 42.6" should {
    "hash to ezs42 at 25 bits precision" in {
      GeoHash(-5.6, 42.6, 25).x must beCloseTo(-5.60302734375, xTolerance(25))
      GeoHash(-5.6, 42.6, 25).y must beCloseTo(42.60498046875, yTolerance(25))
      GeoHash(-5.6, 42.6, 25).prec must equalTo(25)
      GeoHash(-5.6, 42.6, 25).bbox must equalTo(BoundingBox(-5.625, -5.5810546875, 42.626953125, 42.5830078125))
      GeoHash(-5.6, 42.6, 25).bitset must equalTo(BitSet(1,2,4,5,6,7,8,9,10,11,17,23))
      GeoHash(-5.6, 42.6, 25).hash must equalTo("ezs42")
    }
  }

  "-78, 38" should {
    "hash to dqb81 at 25 bits precision" in {
      GeoHash(-78, 38, 25).x must beCloseTo(-77.98095703125, xTolerance(25))
      GeoHash(-78, 38, 25).y must beCloseTo(37.99072265625, yTolerance(25))
      GeoHash(-78, 38, 25).prec must equalTo(25)
      GeoHash(-78, 38, 25).bbox must equalTo(BoundingBox(-78.0029296875, -77.958984375, 38.0126953125, 37.96875))
      GeoHash(-78, 38, 25).bitset must equalTo(BitSet(1,2,5,7,8,11,13,16,24))
      GeoHash(-78, 38, 25).hash must equalTo("dqb81")
    }
  }

  "-78, 38" should {
    "hash to dqb81h at 27 bits precision" in {
      GeoHash(-78, 38, 27).x must beCloseTo(-77.991943359375, xTolerance(27))
      GeoHash(-78, 38, 27).y must beCloseTo(38.001708984375, yTolerance(27))
      GeoHash(-78, 38, 27).prec must equalTo(27)
      GeoHash(-78, 38, 27).bbox must equalTo(BoundingBox(-78.0029296875, -77.98095703125, 38.0126953125, 37.99072265625))
      GeoHash(-78, 38, 27).bitset must equalTo(BitSet(1,2,5,7,8,11,13,16,24,25))
      GeoHash(-78, 38, 27).hash must equalTo("dqb81h")
    }
  }

  "-78, 38" should {
    "hash to dqb81jdn at 40 bits precision" in {
      GeoHash(-78, 38, 40).x must beCloseTo(-78.0000114440918, xTolerance(40))
      GeoHash(-78, 38, 40).y must beCloseTo(38.000078201293945, yTolerance(40))
      GeoHash(-78, 38, 40).prec must equalTo(40)
      GeoHash(-78, 38, 40).bbox must equalTo(BoundingBox(-78.00018310546875, -77.99983978271484, 38.00016403198242, 37.99999237060547))
      GeoHash(-78, 38, 40).bitset must equalTo(BitSet(1,2,5,7,8,11,13,16,24,25,29,31,32,35,37))
      GeoHash(-78, 38, 40).hash must equalTo("dqb81jdn")
    }
  }

  "-78, 38" should {
    "hash to dqb81jdnh32t8 at 63 bits precision" in {
      GeoHash(-78, 38, 63).x must beCloseTo(-78.00000000279397, xTolerance(63))
      GeoHash(-78, 38, 63).y must beCloseTo(38.00000004004687, yTolerance(63))
      GeoHash(-78, 38, 63).prec must equalTo(63)
      GeoHash(-78, 38, 63).bbox must equalTo(BoundingBox(-78.00000004470348, -77.99999996088445, 38.00000008195639, 37.999999998137355))
      GeoHash(-78, 38, 63).bitset must equalTo(BitSet(1,2,5,7,8,11,13,16,24,25,29,31,32,35,37,40,48,49,53,55,56,59,61))
      GeoHash(-78, 38, 63).hash must equalTo("dqb81jdnh32t8")
    }
  }

  "dqb0c" should {
    "decode to -78.68408203125,38.12255859375 at 25 bits precision" in {
      val gh = GeoHash("dqb0c")
      gh.x must beCloseTo(-78.68408203125, xTolerance(25))
      gh.y must beCloseTo(38.12255859375, yTolerance(25))
    }
  }

  "dqb0cn" should {
    "decode to -78.695068359375,38.133544921875 at 27 bits precision" in {
      val gh = GeoHash("dqb0cn", 27)
      gh.x must beCloseTo(-78.695068359375, xTolerance(27))
      gh.y must beCloseTo(38.133544921875, yTolerance(27))
    }
  }

  "dqb0cne4" should {
    "decode to -78.70176315307617,38.13672065734863 at 40 bits precision" in {
      val gh = GeoHash("dqb0cne4", 40)
      gh.x must beCloseTo(-78.70176315307617, xTolerance(40))
      gh.y must beCloseTo(38.13672065734863, yTolerance(40))
    }
  }

  "-78, 33" should {
    "encode and decode correctly at multiple precisions" in {
      val x : Double = -78.0
      val y : Double = 38.0
      for (precision <- 20 to 63) yield {
        if(DEBUG_OUPUT) logger.debug(s"precision: $precision")

        // encode this value
        val ghEncoded : GeoHash = GeoHash(x, y, precision)

        // the encoded values should not match the inputs exactly
        // (if the GeoHash assumes the centroid of the cell-rectangle)
        ghEncoded.x must not equalTo(x)
        ghEncoded.y must not equalTo(y)

        // decode this value
        val ghDecoded : GeoHash = GeoHash(ghEncoded.hash, precision)

        // the bit-strings must match
        ghEncoded.toBinaryString must equalTo(ghDecoded.toBinaryString)
        ghEncoded.bbox must equalTo(ghDecoded.bbox)
        ghEncoded.hash must equalTo(ghDecoded.hash)
        ghEncoded.bitset must equalTo(ghDecoded.bitset)
        ghEncoded.prec must equalTo(ghDecoded.prec)

        if(DEBUG_OUPUT) logger.debug(s"decoded: ${ghDecoded.y}, ${ghDecoded.x}; encoded: ${ghEncoded.y}, ${ghEncoded.x}")

        // the round-trip geometry must be within tolerance of the original
        ghDecoded.x must beCloseTo(x, xTolerance(precision))
        // latitude has half the degree-span of longitude
        ghDecoded.y must beCloseTo(y, yTolerance(precision))
      }
    }
  }

  "POINT(-5.6,42.6)" should {
    "decode to ezs42" in {
      val p = WKTUtils.read("POINT(-5.6 42.6)").asInstanceOf[Point]
      GeoHash(p, 25).hash must equalTo("ezs42")
    }
  }

  "Lat index of 23248, Long index of 5232, and 30 bit precision" should {
    "decode to 9q8ys0" in {
      val gh = GeoHash.composeGeoHashFromBitIndicesAndPrec(23248, 5232, 30)
      gh.hash must equalTo("9q8ys0")
    }
  }

  "Lat index of 23248, Long index of 5232, and 32 bit precision" should {
    "decode to 2ek7q00" in {
      val gh = GeoHash.composeGeoHashFromBitIndicesAndPrec(23248, 5232, 32)
      gh.hash must equalTo("2ek7q00")
    }
  }

  "GeoHash of 9q8ys0" should {
    val gh = GeoHash("9q8ys0")
    "be represented internally as BitSet(1, 4, 5, 7, 8, 11, 15, 16, 17, 18, 20, 21)" in {
      gh.bitset must equalTo(BitSet(1, 4, 5, 7, 8, 11, 15, 16, 17, 18, 20, 21))
    }
    "have 30 bit precision" in {
      gh.prec must equalTo(30)
    }
    "produce a lat index of 23248" in {
      GeoHash.gridIndexForLatitude(gh) must equalTo(23248)
    }
    "produce a long index of 5232" in {
      GeoHash.gridIndexForLongitude(gh) must equalTo(5232)
    }
    "produce a lat/long index array of Array(23248,5232)" in {
      GeoHash.gridIndicesForLatLong(gh) must equalTo(Array(23248, 5232))
    }
  }

  "BitSet(1, 4, 5, 7, 8, 11, 15, 16, 17, 18, 20, 21) with 30 bit precision" should {
    "create the GeoHash 9q8ys0" in {
      GeoHash(BitSet(1, 4, 5, 7, 8, 11, 15, 16, 17, 18, 20, 21), 30) must equalTo(GeoHash("9q8ys0"))
    }
  }

  "Containment: " should {
    val large = GeoHash("dqb0", 18)
    val medium = GeoHash("dqb0", 20)
    val small = GeoHash("dqb0c")

    "GeoHash(\"dqb0\", 18) contains GeoHash(\"dqb0\", 20) and GeoHash(\"dqb0c\")" in {
      large.contains(medium) must equalTo(true)
      large.contains(small) must equalTo(true)
    }

    "GeoHash(\"dqb0\", 20) does not contain GeoHash(\"dqb0\", 18)" in {
      medium.contains(large) must equalTo(false)
    }

    "GeoHash(\"dqb0\", 20) does contain GeoHash(\"dqb0c\")" in {
      medium.contains(small) must equalTo(true)
    }

    "GeoHash(\"dqb0c\") contains neither GeoHash(\"dqb0\", 20) nor GeoHash(\"dqb0\", 18)" in {
      small.contains(medium) must equalTo(false)
      small.contains(large) must equalTo(false)
    }

  }

  "Span Counts" should {
    "return the correct values" in {
      (1,2) must equalTo(GeoHash.getLatitudeLongitudeSpanCount(GeoHash("dm"), GeoHash("dt"), 10))
      (2,3) must equalTo(GeoHash.getLatitudeLongitudeSpanCount(GeoHash("dq"), GeoHash("dv"), 10))
      (2,2) must equalTo(GeoHash.getLatitudeLongitudeSpanCount(GeoHash("dq"), GeoHash("dt"), 10))
      (3,1) must equalTo(GeoHash.getLatitudeLongitudeSpanCount(GeoHash("du"), GeoHash("dy"), 10))
    }
  }

  "binary-string encoding" should {
    "go round-trip from string to string" in {
      val binaryStringIn = "01100101100101000000"
      val gh = GeoHash.fromBinaryString(binaryStringIn)
      gh.hash must be equalTo "dqb0"
      gh.toBinaryString must be equalTo binaryStringIn
    }

    "go round trip from GH to GH" in {
      val hashIn = "dqb0"
      val gh = GeoHash(hashIn)
      val ghOut = GeoHash.fromBinaryString(gh.toBinaryString)
      ghOut.hash must be equalTo hashIn
    }
  }

  "The point (180.0, 0.5) should be in GeoHash 'x'" in {
    val gh = GeoHash(180.0, 0.5, 5)
    gh.hash must be equalTo "x"
  }

  "The point (180.0, 90.0) should be in GeoHash 'z'" in {
    val gh = GeoHash(180.0, 90.0, 5)
    gh.hash must be equalTo "z"
  }

  "The point (180.0, -90.0) should be in GeoHash 'p'" in {
    val gh = GeoHash(180.0, -90.0, 5)
    gh.hash must be equalTo "p"
  }

  "The point (-180.0, -90.0) should be in GeoHash '0'" in {
    val gh = GeoHash(-180.0, -90.0, 5)
    gh.hash must be equalTo "0"
  }

  "The point (-180.0, 90.0) should be in GeoHash 'b'" in {
    val gh = GeoHash(-180.0, 90.0, 5)
    gh.hash must be equalTo "b"
  }

  "Points outside the world" should {
    "throw exceptions" in {
      GeoHash(180.1, 0.0, 5) should throwA[Exception]
      GeoHash(180.1, 90.1, 5) should throwA[Exception]
      GeoHash(180.1, -90.1, 5) should throwA[Exception]
      GeoHash(0.0, 90.1, 5) should throwA[Exception]
      GeoHash(0.0, -90.1, 5) should throwA[Exception]
      GeoHash(-180.1, 0.0, 5) should throwA[Exception]
      GeoHash(-180.1, 90.1, 5) should throwA[Exception]
      GeoHash(-180.1, -90.1, 5) should throwA[Exception]
    }
  }
}
