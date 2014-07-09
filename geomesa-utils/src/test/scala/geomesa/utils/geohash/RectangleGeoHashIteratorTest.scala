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

import GeoHashIterator._
import com.vividsolutions.jts.geom.Coordinate
import org.junit.Assert
import org.junit.Test


class RectangleGeoHashIteratorTest {

  // enable to debug output for tests
  val DEBUG_OUTPUT = false

  @Test
  def testCorners() {
    val bitsPrecision = 35
    val ghBig = GeoHash.apply("9q8ys0")
    val bbox = ghBig.bbox
    val ll = geometryFactory.createPoint(new Coordinate(bbox.ll.getX, bbox.ll.getY))
    val ur = geometryFactory.createPoint(new Coordinate(bbox.ur.getX, bbox.ur.getY))
    val rghi = new RectangleGeoHashIterator(ll.getY, ll.getX,
                                            ur.getY, ur.getX,
                                            bitsPrecision)
    Assert.assertNotNull("Could not instantiate RectangleGeoHashIterator.", rghi)

    val length = rghi.foldLeft(0)({case (count, gh) => {
      Assert.assertNotNull("Fetched a NULL GeoHash.", gh)
      if (DEBUG_OUTPUT) println("RectangleGHI " + count + ".  " + gh)
      val pt = geometryFactory.createPoint(new Coordinate(gh.x, gh.y))
      val s = s"${gh.toString} == (${pt.getY},${pt.getX}) <-> (${bbox.ll.getY},${bbox.ll.getX}::${bbox.ur.getY},${bbox.ur.getX})"
      Assert.assertEquals("Latitude too low " + s, false, pt.getY < bbox.ll.getY)
      Assert.assertEquals("Latitude too high " + s, false, pt.getY > bbox.ur.getY)
      Assert.assertEquals("Longitude too low " + s, false, pt.getX < bbox.ll.getX)
      Assert.assertEquals("Longitude too high " + s, false, pt.getX > bbox.ur.getX)
      count + 1
    }})
    Assert.assertEquals("Wrong number of RGHI iterations found", 32, length)
  }

  @Test
  def testSuccessfulIteration() {
    val longitude = -78.0
    val latitude = 38.0
    val radiusMeters = 500.0
    val bitsPrecision = 35
    val ptCenter = geometryFactory.createPoint(new Coordinate(longitude, latitude))
    val ptLL = VincentyModel.moveWithBearingAndDistance(ptCenter, -135.0, radiusMeters * 707.0 / 500.0)
    val ptUR = VincentyModel.moveWithBearingAndDistance(ptCenter,   45.0, radiusMeters * 707.0 / 500.0)
    if(DEBUG_OUTPUT) println(s"[RectangleGeoHashIterator]  On circle test-bed from $ptLL to $ptUR...")
    val rghi = new RectangleGeoHashIterator(ptLL.getY, ptLL.getX,
                                            ptUR.getY, ptUR.getX,
                                            bitsPrecision)
    Assert.assertNotNull("Could not instantiate RectangleGeoHashIterator.", rghi)
    val ll = geometryFactory.createPoint(new Coordinate(ptLL.getX, ptLL.getY))
    val ur = geometryFactory.createPoint(new Coordinate(ptUR.getX, ptUR.getY))
    val ghLL = GeoHash.apply(ll, bitsPrecision)
    val ghUR = GeoHash.apply(ur, bitsPrecision)
    val list = List(ptLL, ptUR)
    val (llgh, urgh) = GeoHashIterator.getBoundingGeoHashes(list, bitsPrecision, 0.0)
    Assert.assertEquals("LL boundary of the RGHI does not match",
                        ghLL.toBinaryString,
                        llgh.toBinaryString)
    Assert.assertEquals("UR boundary of the RGHI does not match",
                        ghUR.toBinaryString,
                        urgh.toBinaryString)
    val length = rghi.foldLeft(0)({case (count, gh) => {
      Assert.assertNotNull("Fetched a NULL GeoHash.", gh)
      if (DEBUG_OUTPUT) println("RectangleGHI " + count + ".  " + gh)
      count + 1
    }})
    Assert.assertEquals("Wrong number of RGHI iterations found", 80, length)
  }
}
