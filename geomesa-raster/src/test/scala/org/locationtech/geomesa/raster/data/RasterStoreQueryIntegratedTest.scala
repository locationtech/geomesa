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
import org.locationtech.geomesa.raster.RasterTestsUtils
import org.locationtech.geomesa.raster.RasterTestsUtils._
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
      val rasterStore = createMockRasterStore(tableName)

      //generate query
      val query = generateQuery(0, 50, 0, 50)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(0)
    }

    "create a Raster Store, populate it and run a query" in {
      val tableName = getNewIteration()
      val theStore = createMockRasterStore(tableName)

      // populate store
      val testRaster = generateTestRaster(0, 50, 0, 50)
      theStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(0, 50, 0, 50)

      theStore must beAnInstanceOf[RasterStore]
      val theIterator = theStore.getRasters(query)
      val theRaster = theIterator.next()
      theRaster must beAnInstanceOf[Raster]
    }

    "Properly filter in a raster via a query bbox" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val testRaster = generateTestRasterFromGeoHash(GeoHash("s"))
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(0, 50, 0, 50)

      rasterStore must beAnInstanceOf[RasterStore]
      val theIterator = rasterStore.getRasters(query)
      val theRaster = theIterator.next()
      theRaster must beAnInstanceOf[Raster]
    }

    "Properly filter out a raster via a query bbox" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val testRaster = generateTestRasterFromGeoHash(GeoHash("d"))
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(0, 45, 0, 45)

      rasterStore must beAnInstanceOf[RasterStore]
      val theIterator = rasterStore.getRasters(query)
      theIterator.isEmpty must beTrue
    }

    "Properly filter out a raster via a query bbox and maintain a valid raster in the results" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val testRaster1 = generateTestRasterFromGeoHash(GeoHash("s"))
      rasterStore.putRaster(testRaster1)
      val testRaster2 = generateTestRasterFromGeoHash(GeoHash("d"))
      rasterStore.putRaster(testRaster2)

      //generate query
      val query = generateQuery(0, 50, 0, 50)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly filter in a raster conforming to GeoHashes via a query bbox and resolution" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val testRaster = generateTestRasterFromGeoHash(GeoHash("s"), res = 5.0)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(0, 50, 0, 50, res = 5.0)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return a raster slightly smaller than a GeoHash via a query bbox" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val gh = GeoHash("dqcjr")
      val env = gh.getEnvelopeInternal
      val inG = gh.geom.buffer(-0.0001).getEnvelopeInternal
      val testRaster = generateTestRaster(inG.getMinX, inG.getMaxX, inG.getMinY, inG.getMaxY)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(env.getMinX-0.0001, env.getMaxX+0.0001, env.getMinY-0.0001, env.getMaxY+0.0001)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return a raster slightly larger than a GeoHash via a query bbox" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val gh = GeoHash("dqcjr")
      val env = gh.getEnvelopeInternal
      val inG = gh.geom.buffer(0.0001).getEnvelopeInternal
      val testRaster = generateTestRaster(inG.getMinX, inG.getMaxX, inG.getMinY, inG.getMaxY)
      rasterStore.putRaster(testRaster)

      //generate query
      val query = generateQuery(env.getMinX-0.0001, env.getMaxX+0.0001, env.getMinY-0.0001, env.getMaxY+0.0001)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }
    
    "Properly return a group of four Rasters Conforming to GeoHashes Near (0, 0)" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val bbox1 = GeoHash("7").bbox
      val testRaster1 = generateTestRasterFromBoundingBox(bbox1)
      rasterStore.putRaster(testRaster1)
      val bbox2 = GeoHash("k").bbox
      val testRaster2 = generateTestRasterFromBoundingBox(bbox2)
      rasterStore.putRaster(testRaster2)
      val bbox3 = GeoHash("s").bbox
      val testRaster3 = generateTestRasterFromBoundingBox(bbox3)
      rasterStore.putRaster(testRaster3)
      val bbox4 = GeoHash("e").bbox
      val testRaster4 = generateTestRasterFromBoundingBox(bbox4)
      rasterStore.putRaster(testRaster4)

      //generate query
      val query = generateQuery(bbox1.minLon, bbox3.maxLon, bbox1.minLat, bbox3.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(4)
    }

    "Properly return a group of four Small Rasters Conforming to GeoHashes" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val bbox1 = GeoHash("dqb0m").bbox
      val testRaster1 = generateTestRasterFromBoundingBox(bbox1)
      rasterStore.putRaster(testRaster1)
      val bbox2 = GeoHash("dqb0q").bbox
      val testRaster2 = generateTestRasterFromBoundingBox(bbox2)
      rasterStore.putRaster(testRaster2)
      val bbox3 = GeoHash("dqb0w").bbox
      val testRaster3 = generateTestRasterFromBoundingBox(bbox3)
      rasterStore.putRaster(testRaster3)
      val bbox4 = GeoHash("dqb0t").bbox
      val testRaster4 = generateTestRasterFromBoundingBox(bbox4)
      rasterStore.putRaster(testRaster4)

      //generate query
      val query = generateQuery(bbox1.minLon, bbox3.maxLon, bbox1.minLat, bbox3.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(4)
    }

    "Properly return one raster in a QLevel 1 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.quadrant1
      RasterTestsUtils.generateQuadTreeLevelRasters(1).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }.pendingUntilFixed

    "Properly return one raster in a QLevel 2 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(2, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(2).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }.pendingUntilFixed

    "Properly return one raster in a QLevel 3 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(3, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(3).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 4 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(4, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(4).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 5 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(5, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(5).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 6 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(6, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(6).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 7 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(7, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(7).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 8 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(8, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(8).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 9 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(9, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(9).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 10 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(10, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(10).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 11 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(11, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(11).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 12 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(12, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(12).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 13 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(13, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(13).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 14 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(14, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(14).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

    "Properly return one raster in a QLevel 15 bounding box" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      // general setup
      val qbbox = RasterTestsUtils.generateSubQuadrant(15, RasterTestsUtils.quadrant1, 1)
      RasterTestsUtils.generateQuadTreeLevelRasters(15).map(rasterStore.putRaster)

      //generate query
      val query = generateQuery(qbbox.minLon, qbbox.maxLon, qbbox.minLat, qbbox.maxLat)

      rasterStore must beAnInstanceOf[RasterStore]
      val theResults = rasterStore.getRasters(query).toList
      theResults.length must beEqualTo(1)
    }

  }
}
