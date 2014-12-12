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

package org.locationtech.geomesa.raster.iterators

import com.typesafe.scalalogging.slf4j.Logging
import org.junit.runner.RunWith
import org.locationtech.geomesa.raster.RasterTestUtils
import org.locationtech.geomesa.raster.data.RasterStore
import org.locationtech.geomesa.raster.feature.Raster
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RasterStoreQueryIntegratedTest extends Specification with Logging {

  sequential

  var testIteration = 0

  def getNewIteration() = {
    testIteration += 1
    s"testRFITTable_$testIteration"
  }

  "RasterFilteringIterator" should {
    "Properly filter in a raster via a query bbox" in {
      val tableName = getNewIteration()
      val rasterStore = RasterTestUtils.createRasterStore(tableName)

      // general setup
      val testRaster = RasterTestUtils.generateTestRaster(0, 10, 0, 10)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = RasterTestUtils.generateQuery(0, 10, 0, 10)

      rasterStore must beAnInstanceOf[RasterStore]
      val theIterator = rasterStore.getRasters(query)
      val theRaster = theIterator.next()
      theRaster must beAnInstanceOf[Raster]
    }

    "Properly filter out a raster via a query bbox" in {
      val tableName = getNewIteration()
      val rasterStore = RasterTestUtils.createRasterStore(tableName)

      // general setup
      val testRaster = RasterTestUtils.generateTestRaster(-10, -20, -10, -20)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = RasterTestUtils.generateQuery(0, 10, 0, 10)

      rasterStore must beAnInstanceOf[RasterStore]
      val theIterator = rasterStore.getRasters(query)
      theIterator.isEmpty must beTrue
    }

    "Properly filter out a raster via a query bbox and but maintain a valid raster in the results" in {
      val tableName = getNewIteration()
      val rasterStore = RasterTestUtils.createRasterStore(tableName)

      // general setup
      val testRaster1 = RasterTestUtils.generateTestRaster(-10, -20, -10, -20)
      rasterStore.putRaster(testRaster1)
      val testRaster2 = RasterTestUtils.generateTestRaster(0, 10, 0, 10)
      rasterStore.putRaster(testRaster2)

      //generate query
      val query = RasterTestUtils.generateQuery(0, 10, 0, 10)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return a group of four Rasters" in {
      val tableName = getNewIteration()
      val rasterStore = RasterTestUtils.createRasterStore(tableName)

      // general setup
      val testRaster1 = RasterTestUtils.generateTestRaster(0, 5, 0, 5)
      rasterStore.putRaster(testRaster1)
      val testRaster2 = RasterTestUtils.generateTestRaster(5, 10, 0, 5)
      rasterStore.putRaster(testRaster2)
      val testRaster3 = RasterTestUtils.generateTestRaster(0, 5, 5, 10)
      rasterStore.putRaster(testRaster3)
      val testRaster4 = RasterTestUtils.generateTestRaster(5, 10, 5, 10)
      rasterStore.putRaster(testRaster4)

      //generate query
      val query = RasterTestUtils.generateQuery(0, 10, 0, 10)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(3) //TODO: Fix the geohash enumeration, this should be 4!
    }

    "Properly filter in a raster via a query bbox and resolution" in {
      val tableName = getNewIteration()
      val rasterStore = RasterTestUtils.createRasterStore(tableName)

      // general setup
      val testRaster = RasterTestUtils.generateTestRaster(0, 10, 0, 10, res = 10.0)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = RasterTestUtils.generateQuery(0, 10, 0, 10, res = 10.0)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly filter out a raster via a query bbox and resolution" in {
      val tableName = getNewIteration()
      val rasterStore = RasterTestUtils.createRasterStore(tableName)

      // general setup
      val testRaster = RasterTestUtils.generateTestRaster(0, 10, 0, 10, res = 5.0)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = RasterTestUtils.generateQuery(0, 10, 0, 10, res = 10.0)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(0)
    }

  }

}
