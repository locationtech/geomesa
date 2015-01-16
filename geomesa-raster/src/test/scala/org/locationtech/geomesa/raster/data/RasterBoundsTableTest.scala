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

package org.locationtech.geomesa.raster.data

import org.junit.runner.RunWith
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RasterBoundsTableTest extends Specification{
  sequential

  var testIteration = 0

  val wholeWorld = BoundingBox(-180.0, 180.0, -90.0, 90.0)

  def getNewIteration() = {
    testIteration += 1
    s"testRBTT_Table_$testIteration"
  }

  "RasterStore" should {
    "return the bounds of a empty table as the whole world" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // get bounds
      val theBounds = theStore.getBounds()

      theBounds must beAnInstanceOf[BoundingBox]
      theBounds must beEqualTo(wholeWorld)
    }

    "return the bounds of a table with a single raster" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // populate store
      val testRaster = RasterUtils.generateTestRaster(0, 50, 0, 50)
      theStore.putRaster(testRaster)

      // get bounds
      val theBounds = theStore.getBounds()

      theBounds must beAnInstanceOf[BoundingBox]
      theBounds.maxLon must beEqualTo(50.0)
      theBounds.maxLat must beEqualTo(50.0)
      theBounds.minLon must beEqualTo(0.0)
      theBounds.minLat must beEqualTo(0.0)
    }

    "return the bounds of a table with two identical rasters" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // populate store
      val testRaster1 = RasterUtils.generateTestRaster(0, 50, 0, 50)
      theStore.putRaster(testRaster1)
      val testRaster2 = RasterUtils.generateTestRaster(0, 50, 0, 50)
      theStore.putRaster(testRaster2)

      // get bounds
      val theBounds = theStore.getBounds()

      theBounds must beAnInstanceOf[BoundingBox]
      theBounds.maxLon must beEqualTo(50.0)
      theBounds.maxLat must beEqualTo(50.0)
      theBounds.minLon must beEqualTo(0.0)
      theBounds.minLat must beEqualTo(0.0)
    }

    "return the bounds of a table with two adjacent rasters" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // populate store
      val testRaster1 = RasterUtils.generateTestRaster(0, 50, 0, 50)
      theStore.putRaster(testRaster1)
      val testRaster2 = RasterUtils.generateTestRaster(-50, 0, 0, 50)
      theStore.putRaster(testRaster2)

      // get bounds
      val theBounds = theStore.getBounds()

      theBounds must beAnInstanceOf[BoundingBox]
      theBounds.maxLon must beEqualTo(50.0)
      theBounds.maxLat must beEqualTo(50.0)
      theBounds.minLon must beEqualTo(-50.0)
      theBounds.minLat must beEqualTo(0.0)
    }

    "return the bounds of a table with two non-adjacent rasters" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // populate store
      val testRaster1 = RasterUtils.generateTestRaster(-180, -170, -90, -80)
      theStore.putRaster(testRaster1)
      val testRaster2 = RasterUtils.generateTestRaster(170, 180, 80, 90)
      theStore.putRaster(testRaster2)

      // get bounds
      val theBounds = theStore.getBounds()

      theBounds must beAnInstanceOf[BoundingBox]
      theBounds.maxLon must beEqualTo(180.0)
      theBounds.maxLat must beEqualTo(90.0)
      theBounds.minLon must beEqualTo(-180.0)
      theBounds.minLat must beEqualTo(-90.0)
    }

  }
}
