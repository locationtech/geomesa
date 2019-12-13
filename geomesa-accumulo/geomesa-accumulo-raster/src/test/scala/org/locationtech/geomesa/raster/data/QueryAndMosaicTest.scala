/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

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

  val north:      List[Raster] = (1 to 3).map(i => generateRaster(bboxNorthOf, redHerring, s"$i")).toList
  val northEast:  List[Raster] = (1 to 3).map(i => generateRaster(bboxNorthEastOf, redHerring, s"$i")).toList
  val northWest:  List[Raster] = (1 to 3).map(i => generateRaster(bboxNorthWestOf, redHerring, s"$i")).toList
  val east:       List[Raster] = (1 to 3).map(i => generateRaster(bboxEastOf, redHerring, s"$i")).toList
  val center:     List[Raster] = (1 to 3).map(i => generateRaster(bboxOfInterest, testRasterIntVSplit, s"$i")).toList
  val south:      List[Raster] = (1 to 3).map(i => generateRaster(bboxSouthOf, redHerring, s"$i")).toList
  val southEast:  List[Raster] = (1 to 3).map(i => generateRaster(bboxSouthEastOf, redHerring, s"$i")).toList
  val southWest:  List[Raster] = (1 to 3).map(i => generateRaster(bboxSouthWestOf, redHerring, s"$i")).toList
  val west:       List[Raster] = (1 to 3).map(i => generateRaster(bboxWestOf, redHerring, s"$i")).toList

  val permutationsOfThree = List(0, 1, 2).permutations.toSeq

  val lessPreciseQBox = BoundingBox(-77.1152343750, -77.1042480469, 43.0012207031, 43.0122070313)
  val lessPreciseQuery = RasterQuery(lessPreciseQBox, 10.0)

  def getNewIteration() = {
    testIteration += 1
    s"testQAMT_Table_$testIteration"
  }

  "Our Mosaicing" should {
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
      rasterStore must beAnInstanceOf[AccumuloRasterStore]
      val rasters = rasterStore.getRasters(query).toList
      val (mosaic, count) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, testBBox)
      count mustEqual 1
      mosaic must beAnInstanceOf[BufferedImage]
      compareIntBufferedImages(mosaic, testRasterIntVSplit) must beTrue
    }

    "Return the pixel data we request for with less precise query with 8 adjacent tiles" in {
      val tableName = getNewIteration()
      val rasterStore = createMockRasterStore(tableName)

      //populate store
      rasterStore.putRaster(generateRaster(bboxOfInterest, testRasterIntVSplit, "0"))
      rasterStore.putRaster(generateRaster(bboxNorthOf, redHerring, "1"))
      rasterStore.putRaster(generateRaster(bboxNorthEastOf, redHerring, "2"))
      rasterStore.putRaster(generateRaster(bboxEastOf, redHerring, "3"))
      rasterStore.putRaster(generateRaster(bboxSouthEastOf, redHerring, "4"))
      rasterStore.putRaster(generateRaster(bboxSouthOf, redHerring, "5"))
      rasterStore.putRaster(generateRaster(bboxSouthWestOf, redHerring, "6"))
      rasterStore.putRaster(generateRaster(bboxWestOf, redHerring, "7"))
      rasterStore.putRaster(generateRaster(bboxNorthWestOf, redHerring, "8"))

      val rasters = rasterStore.getRasters(lessPreciseQuery).toList
      val (mosaic, _) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, lessPreciseQBox)
      compareIntBufferedImages(mosaic, testRasterIntVSplit) must beTrue
    }

    "Return the Correct thing for all Horizontal case permutations with less precise query" in {
      val res = for (perm <- permutationsOfThree.iterator) yield {
        val tableName = getNewIteration()
        val rasterStore = createMockRasterStore(tableName)

        rasterStore.putRaster(west(perm(0)))
        rasterStore.putRaster(center(perm(1)))
        rasterStore.putRaster(east(perm(2)))

        val rasters = rasterStore.getRasters(lessPreciseQuery).toList
        val (mosaic, _) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, lessPreciseQBox)
        compareIntBufferedImages(mosaic, testRasterIntVSplit)
      }
      forall(res.toSeq)(_ must beTrue)
    }

    "Return the Correct thing for all Vertical case permutations with less precise query" in {
      val res = for (perm <- permutationsOfThree.iterator) yield {
        val tableName = getNewIteration()
        val rasterStore = createMockRasterStore(tableName)

        rasterStore.putRaster(north(perm(0)))
        rasterStore.putRaster(center(perm(1)))
        rasterStore.putRaster(south(perm(2)))

        val rasters = rasterStore.getRasters(lessPreciseQuery).toList
        val (mosaic, _) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, lessPreciseQBox)
        compareIntBufferedImages(mosaic, testRasterIntVSplit)
      }
      forall(res.toSeq)(_ must beTrue)
    }

    "Return the Correct thing for all NW to SE case permutations with less precise query" in {
      val res = for (perm <- permutationsOfThree.iterator) yield {
        val tableName = getNewIteration()
        val rasterStore = createMockRasterStore(tableName)

        rasterStore.putRaster(northWest(perm(0)))
        rasterStore.putRaster(center(perm(1)))
        rasterStore.putRaster(southEast(perm(2)))

        val rasters = rasterStore.getRasters(lessPreciseQuery).toList
        val (mosaic, _) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, lessPreciseQBox)
        compareIntBufferedImages(mosaic, testRasterIntVSplit)
      }
      forall(res.toSeq)(_ must beTrue)
    }

    "Return the Correct thing for all SW to NE case permutations with less precise query" in {
      val res = for (perm <- permutationsOfThree.iterator) yield {
        val tableName = getNewIteration()
        val rasterStore = createMockRasterStore(tableName)

        rasterStore.putRaster(southWest(perm(0)))
        rasterStore.putRaster(center(perm(1)))
        rasterStore.putRaster(northEast(perm(2)))

        val rasters = rasterStore.getRasters(lessPreciseQuery).toList
        val (mosaic, _) = RasterUtils.mosaicChunks(rasters.iterator, 16, 16, lessPreciseQBox)
        compareIntBufferedImages(mosaic, testRasterIntVSplit)
      }
      forall(res.toSeq)(_ must beTrue)
    }
  }
}
