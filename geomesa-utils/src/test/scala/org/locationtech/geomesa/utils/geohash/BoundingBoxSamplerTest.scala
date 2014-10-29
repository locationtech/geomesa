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

import org.junit.{Assert, Test}

class BoundingBoxSamplerTest {
  @Test
  def testBoundingBoxSamplerTest() {
    // Great pictures at http://www.bigdatamodeling.org/2013/01/intuitive-geohash.html
    val latRow = new BoundingBoxSampler(new TwoGeoHashBoundingBox(GeoHash("d0"), GeoHash("d8")))

    val lonRow = new BoundingBoxSampler(new TwoGeoHashBoundingBox(GeoHash("d8"), GeoHash("dw")))


    val box16 = new BoundingBoxSampler(new TwoGeoHashBoundingBox(GeoHash("dh"), GeoHash("dz")))

    val cross = new BoundingBoxSampler(new TwoGeoHashBoundingBox(GeoHash("d0"), GeoHash("g8")))

    def testAndCheck(bbs: BoundingBoxSampler, num: Int, list: List[String]) {
      val answer = for(i <- 1 to num) yield bbs.next.hash
      Assert.assertEquals("The list returned was wrong", answer.toList.sorted, list)
      try {
        bbs.next
        Assert.fail("should have thrown NoSuchElementException")
      } catch {
        case nsee: NoSuchElementException =>
      }
    }

    testAndCheck(cross, 63,
      List("d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "db", "dc", "dd", "de",
        "df", "dg", "dh", "dj", "dk", "dm", "dn", "dp", "dq", "dr", "ds", "dt", "du", "dv", "dw",
        "dx", "dy", "dz", "e0", "e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9", "ed", "ee",
        "eh", "ej", "ek", "em", "en", "ep", "eq", "er", "es", "et", "ew", "ex", "f0", "f2", "f8",
        "fb", "g0", "g2", "g8"))

    testAndCheck(box16, 16,
      List("dh", "dj", "dk", "dm", "dn", "dp", "dq", "dr",
      "ds", "dt", "du", "dv", "dw", "dx", "dy", "dz"))

    testAndCheck(latRow, 3, List("d0", "d2", "d8"))

    testAndCheck(lonRow, 7, List("d8", "d9", "dd", "de", "ds", "dt", "dw"))

  }
}
