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

    "return the bounds of a table with two adjacent rasters at different resolutions" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // populate store
      val testRaster1 = RasterUtils.generateTestRaster(0, 50, 0, 50, res = 1.0)
      theStore.putRaster(testRaster1)
      val testRaster2 = RasterUtils.generateTestRaster(-50, 0, 0, 50, res = 2.0)
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

    "Return an empty set of resolutions for an empty table" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // get bounds
      val theResolutions = theStore.getAvailableResolutions()

      theResolutions must beEmpty[Seq[Double]]
    }

    "Return a set of one resolution for a table with one raster ingested" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // populate the table
      val testRaster = RasterUtils.generateTestRaster(0, 50, 0, 50, res = 10.0)
      theStore.putRaster(testRaster)

      // get bounds
      val theResolutions = theStore.getAvailableResolutions()

      theResolutions must not(beEmpty[Seq[Double]])
      theResolutions.size must beEqualTo(1)
      theResolutions must beEqualTo(Seq(10.0))
    }

    "Return a set of one resolution for a table with duplicated rasters ingested" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // populate the table
      val testRaster1 = RasterUtils.generateTestRaster(0, 50, 0, 50, res = 10.0)
      theStore.putRaster(testRaster1)
      val testRaster2 = RasterUtils.generateTestRaster(0, 50, 0, 50, res = 10.0)
      theStore.putRaster(testRaster2)

      // get bounds
      val theResolutions = theStore.getAvailableResolutions()

      theResolutions must not(beEmpty[Seq[Double]])
      theResolutions.size must beEqualTo(1)
      theResolutions must beEqualTo(Seq(10.0))
    }

    "Return a set of one resolution for a table with multiple similar rasters ingested" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // populate the table
      val testRaster1 = RasterUtils.generateTestRaster(0, 50, 0, 50, res = 10.0)
      theStore.putRaster(testRaster1)
      val testRaster2 = RasterUtils.generateTestRaster(0, -50, 0, 50, res = 10.0)
      theStore.putRaster(testRaster2)
      val testRaster3 = RasterUtils.generateTestRaster(0, -50, 0, -50, res = 10.0)
      theStore.putRaster(testRaster3)
      val testRaster4 = RasterUtils.generateTestRaster(0, 50, 0, -50, res = 10.0)
      theStore.putRaster(testRaster4)

      // get bounds
      val theResolutions = theStore.getAvailableResolutions()

      theResolutions must not(beEmpty[Seq[Double]])
      theResolutions.size must beEqualTo(1)
      theResolutions must beEqualTo(Seq(10.0))
    }

    "Return a set of many resolutions for a table with multiple rasters ingested" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // populate the table
      val testRaster1 = RasterUtils.generateTestRaster(0, 50, 0, 50, res = 6.0)
      theStore.putRaster(testRaster1)
      val testRaster2 = RasterUtils.generateTestRaster(0, 40, 0, 40, res = 7.0)
      theStore.putRaster(testRaster2)
      val testRaster3 = RasterUtils.generateTestRaster(0, 30, 0, 30, res = 8.0)
      theStore.putRaster(testRaster3)
      val testRaster4 = RasterUtils.generateTestRaster(0, 20, 0, 20, res = 9.0)
      theStore.putRaster(testRaster4)
      val testRaster5 = RasterUtils.generateTestRaster(0, 10, 0, 10, res = 10.0)
      theStore.putRaster(testRaster5)

      // get bounds
      val theResolutions = theStore.getAvailableResolutions()

      theResolutions must not(beEmpty[Seq[Double]])
      theResolutions.size must beEqualTo(5)
      theResolutions must beEqualTo(Seq(6.0, 7.0, 8.0, 9.0, 10.0))
    }

    "Return the default GridRange for an empty table" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // get GridRange
      val theGridRange = theStore.getGridRange()

      theGridRange.width must beEqualTo(360)
      theGridRange.height must beEqualTo(180)
    }

    "Return the correct GridRange for a table with one Raster with specific resolution" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // populate the table
      val testRaster = RasterUtils.generateTestRaster(0, 50, 0, 50, res = 1.0)
      theStore.putRaster(testRaster)

      // get GridRange
      val theGridRange = theStore.getGridRange()

      theGridRange.width must beEqualTo(50)
      theGridRange.height must beEqualTo(50)
    }

    "Return the correct GridRange for a table with four Rasters with specific resolution" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // populate the table
      val testRaster1 = RasterUtils.generateTestRaster(0, 50, 0, 50, res = 1.0)
      theStore.putRaster(testRaster1)
      val testRaster2 = RasterUtils.generateTestRaster(0, -50, 0, 50, res = 1.0)
      theStore.putRaster(testRaster2)
      val testRaster3 = RasterUtils.generateTestRaster(0, -50, 0, -50, res = 1.0)
      theStore.putRaster(testRaster3)
      val testRaster4 = RasterUtils.generateTestRaster(0, 50, 0, -50, res = 1.0)
      theStore.putRaster(testRaster4)

      // get GridRange
      val theGridRange = theStore.getGridRange()

      theGridRange.width must beEqualTo(100)
      theGridRange.height must beEqualTo(100)
    }

    "Return the correct GridRange for a table of an image pyramid, defaulting to highest resolution" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // populate the table
      val testRaster1 = RasterUtils.generateTestRaster(0, 50, 0, 50, res = 50.0 / 256)
      theStore.putRaster(testRaster1)
      val testRaster2 = RasterUtils.generateTestRaster(0, 40, 0, 40, res = 40.0 / 256)
      theStore.putRaster(testRaster2)
      val testRaster3 = RasterUtils.generateTestRaster(0, 30, 0, 30, res = 30.0 / 256)
      theStore.putRaster(testRaster3)
      val testRaster4 = RasterUtils.generateTestRaster(0, 20, 0, 20, res = 20.0 / 256)
      theStore.putRaster(testRaster4)
      val testRaster5 = RasterUtils.generateTestRaster(0, 10, 0, 10, res = 10.0 / 256)
      theStore.putRaster(testRaster5)

      // get GridRange
      val theGridRange = theStore.getGridRange()

      theGridRange.width must beEqualTo(1280)
      theGridRange.height must beEqualTo(1280)
    }

  }
}
