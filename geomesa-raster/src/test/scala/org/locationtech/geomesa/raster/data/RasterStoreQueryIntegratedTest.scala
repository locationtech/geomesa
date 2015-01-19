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
import org.locationtech.geomesa.utils.geohash.GeoHash
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RasterStoreQueryIntegratedTest extends Specification {

  sequential

  var testIteration = 0

  def getNewIteration() = {
    testIteration += 1
    s"testRSQIT_Table_$testIteration"
  }

  "RasterStore" should {
    "create an empty RasterStore and return nothing" in {
      val tableName = getNewIteration()
      val rasterStore = RasterUtils.createRasterStore(tableName)

      //generate query
      val query = RasterUtils.generateQuery(0, 50, 0, 50)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(0)
    }

    "create a Raster Store, populate it and run a query" in {
      val tableName = getNewIteration()
      val theStore = RasterUtils.createRasterStore(tableName)

      // populate store
      val testRaster = RasterUtils.generateTestRaster(0, 50, 0, 50)
      theStore.putRaster(testRaster)

      //generate query
      val query = RasterUtils.generateQuery(0, 50, 0, 50)

      theStore must beAnInstanceOf[RasterStore]
      val theIterator = theStore.getRasters(query)
      val theRaster = theIterator.next()
      theRaster must beAnInstanceOf[Raster]
    }

    "Properly filter in a raster via a query bbox" in {
      val tableName = getNewIteration()
      val rasterStore = RasterUtils.createRasterStore(tableName)

      // general setup
      val testRaster = RasterUtils.generateTestRasterFromGeoHash(GeoHash("s"))
      rasterStore.putRaster(testRaster)

      //generate query
      val query = RasterUtils.generateQuery(0, 50, 0, 50)

      rasterStore must beAnInstanceOf[RasterStore]
      val theIterator = rasterStore.getRasters(query)
      val theRaster = theIterator.next()
      theRaster must beAnInstanceOf[Raster]
    }

    "Properly filter out a raster via a query bbox" in {
      val tableName = getNewIteration()
      val rasterStore = RasterUtils.createRasterStore(tableName)

      // general setup
      val testRaster = RasterUtils.generateTestRasterFromGeoHash(GeoHash("d"))
      rasterStore.putRaster(testRaster)

      //generate query
      val query = RasterUtils.generateQuery(0, 45, 0, 45)

      rasterStore must beAnInstanceOf[RasterStore]
      val theIterator = rasterStore.getRasters(query)
      theIterator.isEmpty must beTrue
    }

    "Properly filter out a raster via a query bbox and maintain a valid raster in the results" in {
      val tableName = getNewIteration()
      val rasterStore = RasterUtils.createRasterStore(tableName)

      // general setup
      val testRaster1 = RasterUtils.generateTestRasterFromGeoHash(GeoHash("s"))
      rasterStore.putRaster(testRaster1)
      val testRaster2 = RasterUtils.generateTestRasterFromGeoHash(GeoHash("d"))
      rasterStore.putRaster(testRaster2)

      //generate query
      val query = RasterUtils.generateQuery(0, 50, 0, 50)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly filter in a raster conforming to GeoHashes via a query bbox and resolution" in {
      val tableName = getNewIteration()
      val rasterStore = RasterUtils.createRasterStore(tableName)

      // general setup
      val testRaster = RasterUtils.generateTestRasterFromGeoHash(GeoHash("s"), res = 5.0)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = RasterUtils.generateQuery(0, 50, 0, 50, res = 5.0)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return a raster slightly smaller than a GeoHash via a query bbox" in {
      val tableName = getNewIteration()
      val rasterStore = RasterUtils.createRasterStore(tableName)

      // general setup
      val gh = GeoHash("dqcjr")
      val env = gh.getEnvelopeInternal
      val inG = gh.geom.buffer(-0.0001).getEnvelopeInternal
      val testRaster = RasterUtils.generateTestRaster(inG.getMinX, inG.getMaxX, inG.getMinY, inG.getMaxY)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = RasterUtils.generateQuery(env.getMinX-0.0001, env.getMaxX+0.0001, env.getMinY-0.0001, env.getMaxY+0.0001)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return a raster slightly larger than a GeoHash via a query bbox" in {
      val tableName = getNewIteration()
      val rasterStore = RasterUtils.createRasterStore(tableName)

      // general setup
      val gh = GeoHash("dqcjr")
      val env = gh.getEnvelopeInternal
      val inG = gh.geom.buffer(0.0001).getEnvelopeInternal
      val testRaster = RasterUtils.generateTestRaster(inG.getMinX, inG.getMaxX, inG.getMinY, inG.getMaxY)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = RasterUtils.generateQuery(env.getMinX-0.0001, env.getMaxX+0.0001, env.getMinY-0.0001, env.getMaxY+0.0001)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

//    "Properly filter out a raster conforming to GeoHashes via a query bbox and resolution" in {
//      val tableName = getNewIteration()
//      val rasterStore = RasterUtils.createRasterStore(tableName)
//
//      // general setup
//      val testRaster = RasterUtils.generateTestRasterFromGeoHash(GeoHash("s"), res = 5.0)
//      rasterStore.putRaster(testRaster)
//
//      //generate query
//      val query = RasterUtils.generateQuery(0, 50, 0, 50, res = 10.0)
//
//      rasterStore must beAnInstanceOf[RasterStore]
//      val theResults = rasterStore.getRasters(query).toList
//      theResults.length must beEqualTo(0)
//    }

    "Properly return a group of four Rasters Conforming to GeoHashes Near (0, 0)" in {
      val tableName = getNewIteration()
      val rasterStore = RasterUtils.createRasterStore(tableName)

      // general setup
      val bbox1 = GeoHash("7").bbox
      val testRaster1 = RasterUtils.generateTestRasterFromBoundingBox(bbox1)
      rasterStore.putRaster(testRaster1)
      val bbox2 = GeoHash("k").bbox
      val testRaster2 = RasterUtils.generateTestRasterFromBoundingBox(bbox2)
      rasterStore.putRaster(testRaster2)
      val bbox3 = GeoHash("s").bbox
      val testRaster3 = RasterUtils.generateTestRasterFromBoundingBox(bbox3)
      rasterStore.putRaster(testRaster3)
      val bbox4 = GeoHash("e").bbox
      val testRaster4 = RasterUtils.generateTestRasterFromBoundingBox(bbox4)
      rasterStore.putRaster(testRaster4)

      //generate query
      val query = RasterUtils.generateQuery(bbox1.minLon, bbox3.maxLon, bbox1.minLat, bbox3.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(4)
    }

    "Properly return a group of four Small Rasters Conforming to GeoHashes" in {
      val tableName = getNewIteration()
      val rasterStore = RasterUtils.createRasterStore(tableName)

      // general setup
      val bbox1 = GeoHash("dqb0m").bbox
      val testRaster1 = RasterUtils.generateTestRasterFromBoundingBox(bbox1)
      rasterStore.putRaster(testRaster1)
      val bbox2 = GeoHash("dqb0q").bbox
      val testRaster2 = RasterUtils.generateTestRasterFromBoundingBox(bbox2)
      rasterStore.putRaster(testRaster2)
      val bbox3 = GeoHash("dqb0w").bbox
      val testRaster3 = RasterUtils.generateTestRasterFromBoundingBox(bbox3)
      rasterStore.putRaster(testRaster3)
      val bbox4 = GeoHash("dqb0t").bbox
      val testRaster4 = RasterUtils.generateTestRasterFromBoundingBox(bbox4)
      rasterStore.putRaster(testRaster4)

      //generate query
      val query = RasterUtils.generateQuery(bbox1.minLon, bbox3.maxLon, bbox1.minLat, bbox3.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(4)
    }

  }
}
