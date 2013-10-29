/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
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

package geomesa.utils.geohash

import collection.BitSet
import com.vividsolutions.jts.geom.Point
import geomesa.utils.text.WKTUtils
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import scala.math._
import com.typesafe.scalalogging.slf4j.Logging

@RunWith(classOf[JUnitRunner])
class GeoHashTest extends Specification
                          with Logging {

  "ezs42" should {
    "decode to -5.6, 42.6" in {
      val gh = GeoHash("ezs42")
      gh.x must beCloseTo(-5.6, 0.01)
      gh.y must beCloseTo(42.6, 0.01)
    }
  }

  "-5.6, 42.6" should {
    "hash to ezs42 at 25 bits precision" in {
      GeoHash(-5.6, 42.6, 25).x must equalTo(-5.60302734375)
      GeoHash(-5.6, 42.6, 25).y must equalTo(42.60498046875)
      GeoHash(-5.6, 42.6, 25).prec must equalTo(25)
      GeoHash(-5.6, 42.6, 25).bbox must equalTo(BoundingBox(-5.625, -5.5810546875, 42.626953125, 42.5830078125))
      GeoHash(-5.6, 42.6, 25).bitset must equalTo(BitSet(1,2,4,5,6,7,8,9,10,11,17,23))
      GeoHash(-5.6, 42.6, 25).hash must equalTo("ezs42")
    }
  }

  "-78, 38" should {
    "hash to dqb81 at 25 bits precision" in {
      GeoHash(-78, 38, 25).x must equalTo(-77.98095703125)
      GeoHash(-78, 38, 25).y must equalTo(37.99072265625)
      GeoHash(-78, 38, 25).prec must equalTo(25)
      GeoHash(-78, 38, 25).bbox must equalTo(BoundingBox(-78.0029296875, -77.958984375, 38.0126953125, 37.96875))
      GeoHash(-78, 38, 25).bitset must equalTo(BitSet(1,2,5,7,8,11,13,16,24))
      GeoHash(-78, 38, 25).hash must equalTo("dqb81")
    }
  }

  "-78, 38" should {
    "hash to dqb81h at 27 bits precision" in {
      GeoHash(-78, 38, 27).x must equalTo(-77.991943359375)
      GeoHash(-78, 38, 27).y must equalTo(38.001708984375)
      GeoHash(-78, 38, 27).prec must equalTo(27)
      GeoHash(-78, 38, 27).bbox must equalTo(BoundingBox(-78.0029296875, -77.98095703125, 38.0126953125, 37.99072265625))
      GeoHash(-78, 38, 27).bitset must equalTo(BitSet(1,2,5,7,8,11,13,16,24,25))
      GeoHash(-78, 38, 27).hash must equalTo("dqb81h")
    }
  }

  "-78, 38" should {
    "hash to dqb81jdn at 40 bits precision" in {
      GeoHash(-78, 38, 40).x must equalTo(-78.0000114440918)
      GeoHash(-78, 38, 40).y must equalTo(38.000078201293945)
      GeoHash(-78, 38, 40).prec must equalTo(40)
      GeoHash(-78, 38, 40).bbox must equalTo(BoundingBox(-78.00018310546875, -77.99983978271484, 38.00016403198242, 37.99999237060547))
      GeoHash(-78, 38, 40).bitset must equalTo(BitSet(1,2,5,7,8,11,13,16,24,25,29,31,32,35,37))
      GeoHash(-78, 38, 40).hash must equalTo("dqb81jdn")
    }
  }

  "dqb0c" should {
    "decode to -78.68408203125,38.12255859375 at 25 bits precision" in {
      val gh = GeoHash("dqb0c")
      gh.x must beCloseTo(-78.68408203125, 0.015)
      gh.y must beCloseTo(38.12255859375, 0.015)
    }
  }

  "dqb0cn" should {
    "decode to -78.695068359375,38.133544921875 at 27 bits precision" in {
      val gh = GeoHash("dqb0cn", 27)
      gh.x must beCloseTo(-78.695068359375, 0.015)
      gh.y must beCloseTo(38.133544921875, 0.015)
    }
  }

  "-78, 33" should {
    "encode and decode correctly at multiple precisions" in {
      val x : Double = -78.0
      val y : Double = 38.0
      for (precision <- 20 to 40) yield {
        logger.debug(s"precision: $precision")

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

        // compute tolerance based on precision
        val minHalvings : Double = floor(precision/2)
        val tolerance : Double = 360.0 * pow(0.5, minHalvings)

        logger.debug(s"decoded: ${ghDecoded.y}, ${ghDecoded.x}; encoded: ${ghEncoded.y}, ${ghEncoded.x}")

        // the round-trip geometry must be within tolerance of the original
        ghDecoded.x must beCloseTo(x, tolerance)
        // latitude has half the degree-span of longitude
        ghDecoded.y must beCloseTo(y, tolerance*0.5)
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
}
