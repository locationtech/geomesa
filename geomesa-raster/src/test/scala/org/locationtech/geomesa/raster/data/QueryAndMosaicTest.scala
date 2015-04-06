/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.raster.data

import java.awt.image.BufferedImage

import org.junit.runner.RunWith
import org.locationtech.geomesa.raster.RasterTestsUtils._
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class QueryAndMosaicTest extends Specification {
  sequential

  var testIteration = 0

  val bboxNorthOf    = BoundingBox(-77.1152343750, -77.104248046875, 43.01220703125, 43.023193359375)
  val bboxEastOf     = BoundingBox(-77.104248046875, -77.09326171875, 43.001220703125, 43.0122070313125)
  val bboxOfInterest = BoundingBox(-77.1152343750, -77.104248046875, 43.001220703125, 43.0122070313125)
  val bboxSouthOf    = BoundingBox(-77.1152343750, -77.104248046875, 42.9902343750, 43.001220703125)
  val bboxWestOf     = BoundingBox(-77.126220703125, -77.1152343750, 43.001220703125, 43.0122070313125)

  val bboxSouthEastOf  = BoundingBox(-77.104248046875, -77.09326171875, 42.9902343750, 43.001220703125)
  val bboxSouthWestOf  = BoundingBox(-77.126220703125, -77.1152343750, 42.9902343750, 43.001220703125)
  val bboxNorthWestOf  = BoundingBox(-77.126220703125, -77.1152343750, 43.01220703125, 43.023193359375)
  val bboxNorthEastOf  = BoundingBox(-77.104248046875, -77.09326171875, 43.01220703125, 43.023193359375)

  val north:  List[Raster] = (1 to 3).map(i => generateRaster(bboxNorthOf, redHerring, s"$i")).toList
  val center: List[Raster] = (1 to 3).map(i => generateRaster(bboxOfInterest, testRasterIntVSplit, s"$i")).toList
  val south:  List[Raster] = (1 to 3).map(i => generateRaster(bboxSouthOf, redHerring, s"$i")).toList

  def getNewIteration() = {
    testIteration += 1
    s"testQAMT_Table_$testIteration"
  }

  def compareBufferedImages(act: BufferedImage, exp: BufferedImage): Boolean = {
    //compare basic info
    if (act.getWidth != exp.getWidth) false
    else if (act.getHeight != exp.getHeight) false
    else {
      val actWR = act.getRaster
      val expWR = exp.getRaster
      val actElements = for(i <- 0 until act.getWidth; j <- 0 until act.getHeight) yield actWR.getSample(i, j, 0)
      val expElements = for(i <- 0 until act.getWidth; j <- 0 until act.getHeight) yield expWR.getSample(i, j, 0)
      actElements.sameElements(expElements)
    }
  }

  "A RasterStore" should {
    "Return the same tile we store" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)
      val testBBox = BoundingBox(-77.1152343750, -77.104248046875, 43.001220703125, 43.0122070313125)

      //populate store
      val testRaster = generateRaster(testBBox, testRasterIntVSplit)
      rasterStore.putRaster(testRaster)

      //generate full precision query
      val query = generateQuery(-77.1152343750, -77.104248046875, 43.001220703125, 43.0122070313125)

      //view results
      rasterStore must beAnInstanceOf[RasterStore]
      val rasters = rasterStore.getRasters(query).toList
      val (mosaic, count) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, testBBox)
      count mustEqual 1
      mosaic must beAnInstanceOf[BufferedImage]
      compareBufferedImages(mosaic, testRasterIntVSplit) must beTrue
    }
    
    "Return the pixel data we request for with less precise query case one" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      //populate store {3,1,2}
      rasterStore.putRaster(north(2))
      rasterStore.putRaster(center(0))
      rasterStore.putRaster(south(1))

      //generate query
      val qBox = BoundingBox(-77.1152343750, -77.1042480469, 43.0012207031, 43.0122070313)
      val query = generateQuery(qBox.minLon, qBox.maxLon, qBox.minLat, qBox.maxLat)

      //view results
      val rasters = rasterStore.getRasters(query).toList
      val (mosaic, _) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, qBox)
      compareBufferedImages(mosaic, testRasterIntVSplit) must beTrue
    }

    "Return the pixel data we request for with less precise query case two" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      //populate store {3,2,1}
      rasterStore.putRaster(north(2))
      rasterStore.putRaster(center(1))
      rasterStore.putRaster(south(0))

      //generate query
      val qBox = BoundingBox(-77.1152343750, -77.1042480469, 43.0012207031, 43.0122070313)
      val query = generateQuery(qBox.minLon, qBox.maxLon, qBox.minLat, qBox.maxLat)

      //view results
      val rasters = rasterStore.getRasters(query).toList
      val (mosaic, _) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, qBox)
      compareBufferedImages(mosaic, testRasterIntVSplit) must beTrue
    }

    "Return the pixel data we request for with less precise query case three" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      //populate store {1,3,2}
      rasterStore.putRaster(north(0))
      rasterStore.putRaster(center(2))
      rasterStore.putRaster(south(1))

      //generate query
      val qBox = BoundingBox(-77.1152343750, -77.1042480469, 43.0012207031, 43.0122070313)
      val query = generateQuery(qBox.minLon, qBox.maxLon, qBox.minLat, qBox.maxLat)

      //view results
      val rasters = rasterStore.getRasters(query).toList
      val (mosaic, _) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, qBox)
      compareBufferedImages(mosaic, testRasterIntVSplit) must beTrue
    }

    "Return the pixel data we request for with less precise query case four" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      //populate store {1,2,3}
      rasterStore.putRaster(north(0))
      rasterStore.putRaster(center(1))
      rasterStore.putRaster(south(2))

      //generate query
      val qBox = BoundingBox(-77.1152343750, -77.1042480469, 43.0012207031, 43.0122070313)
      val query = generateQuery(qBox.minLon, qBox.maxLon, qBox.minLat, qBox.maxLat)

      //view results
      val rasters = rasterStore.getRasters(query).toList
      val (mosaic, _) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, qBox)
      compareBufferedImages(mosaic, testRasterIntVSplit) must beTrue
    }

    "Return the pixel data we request for with less precise query case five" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      //populate store {2,1,3}
      rasterStore.putRaster(north(1))
      rasterStore.putRaster(center(0))
      rasterStore.putRaster(south(2))

      //generate query
      val qBox = BoundingBox(-77.1152343750, -77.1042480469, 43.0012207031, 43.0122070313)
      val query = generateQuery(qBox.minLon, qBox.maxLon, qBox.minLat, qBox.maxLat)

      //view results
      val rasters = rasterStore.getRasters(query).toList
      val (mosaic, _) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, qBox)
      compareBufferedImages(mosaic, testRasterIntVSplit) must beTrue
    }

    "Return the pixel data we request for with less precise query case six" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      //populate store {2,3,1}
      rasterStore.putRaster(north(1))
      rasterStore.putRaster(center(2))
      rasterStore.putRaster(south(0))

      //generate query
      val qBox = BoundingBox(-77.1152343750, -77.1042480469, 43.0012207031, 43.0122070313)
      val query = generateQuery(qBox.minLon, qBox.maxLon, qBox.minLat, qBox.maxLat)

      //view results
      val rasters = rasterStore.getRasters(query).toList
      val (mosaic, _) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, qBox)
      compareBufferedImages(mosaic, testRasterIntVSplit) must beTrue
    }
    
    "Return the pixel data we request for with less precise query with 8 adjacent tiles" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)
      
      //populate store
      rasterStore.putRaster(generateRaster(bboxOfInterest,  testRasterIntVSplit,"0"))
      rasterStore.putRaster(generateRaster(bboxNorthOf,     redHerring, "1"))
      rasterStore.putRaster(generateRaster(bboxNorthEastOf, redHerring, "2"))
      rasterStore.putRaster(generateRaster(bboxEastOf,      redHerring, "3"))
      rasterStore.putRaster(generateRaster(bboxSouthEastOf, redHerring, "4"))
      rasterStore.putRaster(generateRaster(bboxSouthOf,     redHerring, "5"))
      rasterStore.putRaster(generateRaster(bboxSouthWestOf, redHerring, "6"))
      rasterStore.putRaster(generateRaster(bboxWestOf,      redHerring, "7"))
      rasterStore.putRaster(generateRaster(bboxNorthWestOf, redHerring, "8"))

      //generate query
      val qBox = BoundingBox(-77.1152343750, -77.1042480469, 43.0012207031, 43.0122070313)
      val query = generateQuery(qBox.minLon, qBox.maxLon, qBox.minLat, qBox.maxLat)

      //view results
      val rasters = rasterStore.getRasters(query).toList
      val (mosaic, _) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, qBox)
      compareBufferedImages(mosaic, testRasterIntVSplit) must beTrue
    }

  }

}
