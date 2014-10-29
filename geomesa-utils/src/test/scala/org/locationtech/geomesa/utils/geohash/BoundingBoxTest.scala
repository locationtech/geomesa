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

class BoundingBoxTest {
  @Test def boundingBoxTest {
    var bbox = BoundingBox.apply(GeoHash.apply("dqb00").getPoint, GeoHash.apply("dqbxx").getPoint)
    var hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox)
    println(hashes.size + "\n" + hashes)
    Assert.assertEquals(24, hashes.size)

    bbox = BoundingBox.apply(-78, -77.895029, 38.045834, 38)
    hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox, 32)
    println(hashes.size + "\n" + hashes)
    Assert.assertEquals(6, hashes.size)

    bbox = BoundingBox.apply(-78, -77.89503, 38.0458335, 38)
    hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox, 32)
    println(hashes.size + "\n" + hashes)
    Assert.assertEquals(6, hashes.size)

    bbox = BoundingBox.apply(-50, 50, -40, 40)
    hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox, 32)
    println(hashes.size + "\n" + hashes)
    Assert.assertEquals(8, hashes.size)

    bbox = BoundingBox.apply(1, 1, 1, 1)
    hashes = BoundingBox.getGeoHashesFromBoundingBox(bbox, 32)
    println(hashes.size + "\n" + hashes)
    Assert.assertEquals(1, hashes.size)

  }
}
