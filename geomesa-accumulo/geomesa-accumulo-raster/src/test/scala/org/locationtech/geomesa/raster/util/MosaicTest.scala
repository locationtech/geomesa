/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.util

import java.awt.image.{BufferedImage, RenderedImage}

import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.raster.RasterTestsUtils._
import org.locationtech.geomesa.raster.data.Raster
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MosaicTest extends Specification {

  sequential

  def generateFourAdjacentRaster(): Seq[Raster] = {
    val testRaster1 = generateTestRaster(-50, 0, 0, 50, color = lightGray)
    val testRaster2 = generateTestRaster(0, 50, 0, 50, color = darkGray)
    val testRaster3 = generateTestRaster(0, 50, -50, 0, color = lightGray)
    val testRaster4 = generateTestRaster(-50, 0, -50, 0, color = darkGray)
    Seq(testRaster1, testRaster2, testRaster3, testRaster4)
  }

  "Mosaic Chunks" should {
    "Mosaic two adjacent Rasters together with a Query of equal extent and equal resolution" in {
      val testRaster1 = generateTestRaster(-50, 0, 0, 50, color = darkGray)
      val testRaster2 = generateTestRaster(0, 50, 0, 50, color = white)
      val rasterSeq = Seq(testRaster1, testRaster2)

      val queryEnv = new ReferencedEnvelope(-50.0, 50.0, 0.0, 50.0, DefaultGeographicCRS.WGS84)
      val testMosaic = RasterUtils.mosaicChunks(rasterSeq.iterator, 512, 256, queryEnv)._1

      testMosaic must beAnInstanceOf[RenderedImage]
      testMosaic.getHeight mustEqual 256
      testMosaic.getWidth mustEqual 512
    }

    "Mosaic four Rasters together with a Query of larger extent and finer resolution" in {
      val rasterSeq = generateFourAdjacentRaster()

      val queryEnv = new ReferencedEnvelope(-60.0, 60.0, -60.0, 60.0, DefaultGeographicCRS.WGS84)
      val testMosaic = RasterUtils.mosaicChunks(rasterSeq.iterator, 800, 800, queryEnv)._1

      testMosaic must beAnInstanceOf[RenderedImage]
      testMosaic.getHeight mustEqual 800
      testMosaic.getWidth mustEqual 800
    }

    "Mosaic four Rasters together with a Query of larger extent and equal resolution" in {
      val rasterSeq = generateFourAdjacentRaster()

      val queryEnv = new ReferencedEnvelope(-60.0, 60.0, -60.0, 60.0, DefaultGeographicCRS.WGS84)
      val testMosaic = RasterUtils.mosaicChunks(rasterSeq.iterator, 614, 614, queryEnv)._1

      testMosaic must beAnInstanceOf[RenderedImage]
      testMosaic.getHeight mustEqual 614
      testMosaic.getWidth mustEqual 614
    }

    "Mosaic four Rasters together with a Query of larger extent and courser resolution" in {
      val rasterSeq = generateFourAdjacentRaster()

      val queryEnv = new ReferencedEnvelope(-60.0, 60.0, -60.0, 60.0, DefaultGeographicCRS.WGS84)
      val testMosaic = RasterUtils.mosaicChunks(rasterSeq.iterator, 307, 307, queryEnv)._1

      testMosaic must beAnInstanceOf[RenderedImage]
      testMosaic.getHeight mustEqual 307
      testMosaic.getWidth mustEqual 307
    }

    "Mosaic four Rasters together with a Query of equal extent and finer resolution" in {
      val rasterSeq = generateFourAdjacentRaster()

      val queryEnv = new ReferencedEnvelope(-50.0, 50.0, -50.0, 50.0, DefaultGeographicCRS.WGS84)
      val testMosaic = RasterUtils.mosaicChunks(rasterSeq.iterator, 800, 800, queryEnv)._1

      testMosaic must beAnInstanceOf[RenderedImage]
      testMosaic.getHeight mustEqual 800
      testMosaic.getWidth mustEqual 800
    }

    "Mosaic four Rasters together with a Query of equal extent and equal resolution" in {
      val rasterSeq = generateFourAdjacentRaster()

      val queryEnv = new ReferencedEnvelope(-50.0, 50.0, -50.0, 50.0, DefaultGeographicCRS.WGS84)
      val testMosaic = RasterUtils.mosaicChunks(rasterSeq.iterator, 512, 512, queryEnv)._1

      testMosaic must beAnInstanceOf[RenderedImage]
      testMosaic.getHeight mustEqual 512
      testMosaic.getWidth mustEqual 512
    }

    "Mosaic four Rasters together with a Query of equal extent and courser resolution" in {
      val rasterSeq = generateFourAdjacentRaster()

      val queryEnv = new ReferencedEnvelope(-50.0, 50.0, -50.0, 50.0, DefaultGeographicCRS.WGS84)
      val testMosaic = RasterUtils.mosaicChunks(rasterSeq.iterator, 64, 64, queryEnv)._1

      testMosaic must beAnInstanceOf[RenderedImage]
      testMosaic.getHeight mustEqual 64
      testMosaic.getWidth mustEqual 64
    }

    "Mosaic four Rasters together with a Query of smaller extent and finer resolution" in {
      val rasterSeq = generateFourAdjacentRaster()

      val queryEnv = new ReferencedEnvelope(-25.0, 25.0, -25.0, 25.0, DefaultGeographicCRS.WGS84)
      val testMosaic = RasterUtils.mosaicChunks(rasterSeq.iterator, 800, 800, queryEnv)._1

      testMosaic must beAnInstanceOf[RenderedImage]
      testMosaic.getHeight mustEqual 800
      testMosaic.getWidth mustEqual 800
    }

    "Mosaic four Rasters together with a Query of smaller extent and equal resolution" in {
      val rasterSeq = generateFourAdjacentRaster()

      val queryEnv = new ReferencedEnvelope(-25.0, 25.0, -25.0, 25.0, DefaultGeographicCRS.WGS84)
      val testMosaic = RasterUtils.mosaicChunks(rasterSeq.iterator, 256, 256, queryEnv)._1

      testMosaic must beAnInstanceOf[RenderedImage]
      testMosaic.getHeight mustEqual 256
      testMosaic.getWidth mustEqual 256
    }

    "Mosaic four Rasters together with a Query of smaller extent and equal resolution offsetted" in {
      val rasterSeq = generateFourAdjacentRaster()

      val queryEnv = new ReferencedEnvelope(-35.0, 15.0, -25.0, 25.0, DefaultGeographicCRS.WGS84)
      val testMosaic = RasterUtils.mosaicChunks(rasterSeq.iterator, 256, 256, queryEnv)._1

      testMosaic must beAnInstanceOf[RenderedImage]
      testMosaic.getHeight mustEqual 256
      testMosaic.getWidth mustEqual 256
    }

    "Mosaic four Rasters together with a Query of smaller extent and courser resolution" in {
      val rasterSeq = generateFourAdjacentRaster()

      val queryEnv = new ReferencedEnvelope(-25.0, 25.0, -25.0, 25.0, DefaultGeographicCRS.WGS84)
      val testMosaic = RasterUtils.mosaicChunks(rasterSeq.iterator, 64, 64, queryEnv)._1

      testMosaic must beAnInstanceOf[RenderedImage]
      testMosaic.getHeight mustEqual 64
      testMosaic.getWidth mustEqual 64
    }

    "Mosaic several Rasters together with a Rectangular Query of wider extent" in {
      val rasterSeq = generateFourAdjacentRaster()

      val queryEnv = new ReferencedEnvelope(-81.0, 87.0, -60.0, 60.0, DefaultGeographicCRS.WGS84)
      val testMosaic = RasterUtils.mosaicChunks(rasterSeq.iterator, 700, 500, queryEnv)._1

      testMosaic must beAnInstanceOf[RenderedImage]
      testMosaic.getHeight mustEqual 500
      testMosaic.getWidth mustEqual 700
    }

    "Mosaic several Rasters together with a Rectangular Query of taller extent" in {
      val rasterSeq = generateFourAdjacentRaster()

      val queryEnv = new ReferencedEnvelope(-30.0, 30.0, -81.0, 87.0, DefaultGeographicCRS.WGS84)
      val testMosaic = RasterUtils.mosaicChunks(rasterSeq.iterator, 200, 600, queryEnv)._1

      testMosaic must beAnInstanceOf[RenderedImage]
      testMosaic.getHeight mustEqual 600
      testMosaic.getWidth mustEqual 200
    }

  }

  "cropRaster" should {

    "not crop a raster when the cropEnv is identical to raster extent" in {
      val cropEnv = new ReferencedEnvelope(0.0, 50.0, 0.0, 50.0, DefaultGeographicCRS.WGS84)
      val testRaster = generateTestRaster(0, 50, 0, 50)

      val croppedRaster = RasterUtils.cropRaster(testRaster, cropEnv)

      croppedRaster must beAnInstanceOf[Some[BufferedImage]]
      croppedRaster.map(_.getHeight).getOrElse(0) mustEqual 256
      croppedRaster.map(_.getWidth).getOrElse(0) mustEqual 256
    }

    "crop a raster into a square quarter" in {
      val cropEnv = new ReferencedEnvelope(0.0, 25.0, 0.0, 25.0, DefaultGeographicCRS.WGS84)
      val testRaster = generateTestRaster(0, 50, 0, 50)

      val croppedRaster = RasterUtils.cropRaster(testRaster, cropEnv)

      croppedRaster must beAnInstanceOf[Some[BufferedImage]]
      croppedRaster.map(_.getHeight).getOrElse(0) mustEqual 128
      croppedRaster.map(_.getWidth).getOrElse(0)  mustEqual 128
    }

    "crop a raster with a offsetted cropping envelope" in {
      val cropEnv = new ReferencedEnvelope(-10.0, 10.0, 0.0, 25.0, DefaultGeographicCRS.WGS84)
      val testRaster = generateTestRaster(0, 50, 0, 50)

      val croppedRaster = RasterUtils.cropRaster(testRaster, cropEnv)

      croppedRaster must beAnInstanceOf[Some[BufferedImage]]
      croppedRaster.map(_.getHeight).getOrElse(0) mustEqual 128
      croppedRaster.map(_.getWidth).getOrElse(0)  mustEqual 52
    }

    "crop a raster into nothing when raster is touching a corner of the cropping envelope" in {
      val cropEnv = new ReferencedEnvelope(0.0, 50.0, 0.0, 50.0, DefaultGeographicCRS.WGS84)
      val testRaster = generateTestRaster(-50, 0, -50, 0)

      val croppedRaster = RasterUtils.cropRaster(testRaster, cropEnv)

      croppedRaster must beNone
    }

    "crop a raster into nothing when raster is touching a vertical edge of the cropping envelope" in {
      val cropEnv = new ReferencedEnvelope(0.0, 50.0, 0.0, 50.0, DefaultGeographicCRS.WGS84)
      val testRaster = generateTestRaster(-50, 0, 0, 50)

      val croppedRaster = RasterUtils.cropRaster(testRaster, cropEnv)

      croppedRaster must beNone
    }

    "crop a raster into nothing when raster is touching a horizontal edge of the cropping envelope" in {
      val cropEnv = new ReferencedEnvelope(0.0, 50.0, 0.0, 50.0, DefaultGeographicCRS.WGS84)
      val testRaster = generateTestRaster(0, 50, -50, 0)

      val croppedRaster = RasterUtils.cropRaster(testRaster, cropEnv)

      croppedRaster must beNone
    }


    "crop a raster into nothing when raster is outside cropping envelope" in {
      val cropEnv = new ReferencedEnvelope(0.0, 50.0, 0.0, 50.0, DefaultGeographicCRS.WGS84)
      val testRaster = generateTestRaster(-150, -100, 0, 50)

      val croppedRaster = RasterUtils.cropRaster(testRaster, cropEnv)

      croppedRaster must beNone
    }

  }

  "scaleBufferedImage" should {

    "do nothing when told to scale 10x1 Floats to same dimensions" in {
      val expected = testRasterFloat10x1
      val actual = RasterUtils.scaleBufferedImage(10, 1, expected)

      actual must beEqualTo(expected)
    }

    "do nothing when told to scale 1x10 Floats to same dimensions" in {
      val expected = testRasterFloat1x10
      val actual = RasterUtils.scaleBufferedImage(1, 10, expected)

      actual must beEqualTo(expected)
    }

    "scale 4x4 Ints to 2x2" in {
      val actual = RasterUtils.scaleBufferedImage(2, 2, testRasterIntQuadrants4x4)
      compareIntBufferedImages(actual, testRasterIntQuadrants2x2)
    }

    "scale 2x2 Ints to 4x4" in {
      val actual = RasterUtils.scaleBufferedImage(4, 4, testRasterIntQuadrants2x2)
      compareIntBufferedImages(actual, testRasterIntQuadrants4x4)
    }

    "scale 825x41 Ints to 660x330" in {
      val actual = RasterUtils.scaleBufferedImage(660, 330, RasterUtils.getNewImage(825, 41, gray))
      val expected = RasterUtils.getNewImage(660, 330, gray)
      compareIntBufferedImages(actual, expected)
    }

    "scale 10x1 Ints to 5x1" in {
      val source = testRasterInt10x1
      val expected: BufferedImage = {
        val image = new BufferedImage(5, 1, BufferedImage.TYPE_BYTE_GRAY)
        val wr = image.getRaster
        wr.setPixels(0,0,2,1, Array.fill[Int](2)(1))
        wr.setPixels(2,0,3,1, Array.fill[Int](3)(2))
        image
      }
      val actual = RasterUtils.scaleBufferedImage(5, 1, source)
      compareIntBufferedImages(actual, expected) must beTrue
    }

    "scale 10x1 Floats to 5x1" in {
      val expected = Array(Array.fill[Float](2)(1.6.toFloat) ++ Array.fill[Float](3)(2.5.toFloat))
      val actual = RasterUtils.scaleBufferedImage(5, 1, testRasterFloat10x1)
      compareFloatBufferedImages(actual, expected) must beTrue
    }.pendingUntilFixed

    "scale 10x1 Floats to 5x5" in {
      val expected = Array.fill(5)(Array.fill[Float](2)(1.6.toFloat) ++ Array.fill[Float](3)(2.5.toFloat))
      val actual = RasterUtils.scaleBufferedImage(5, 5, testRasterFloat10x1)
      compareFloatBufferedImages(actual, expected) must beTrue
    }.pendingUntilFixed

    "scale 10x1 Floats to 20x1" in {
      val expected = Array(Array.fill[Float](10)(1.6.toFloat) ++ Array.fill[Float](10)(2.5.toFloat))
      val actual = RasterUtils.scaleBufferedImage(20, 1, testRasterFloat10x1)
      compareFloatBufferedImages(actual, expected) must beTrue
    }.pendingUntilFixed

    "scale 1x10 Floats to 1x5" in {
      val expected =  Array.fill(2)(Array.fill[Float](1)(1.6.toFloat)) ++
                      Array.fill(3)(Array.fill[Float](1)(2.5.toFloat))
      val actual = RasterUtils.scaleBufferedImage(1, 5, testRasterFloat1x10)
      compareFloatBufferedImages(actual, expected) must beTrue
    }.pendingUntilFixed

    "scale 1x10 Floats to 5x5" in {
      val expected = Array.fill(2)(Array.fill[Float](5)(1.6.toFloat)) ++ Array.fill(3)(Array.fill[Float](5)(2.5.toFloat))
      val actual = RasterUtils.scaleBufferedImage(5, 5, testRasterFloat1x10)
      compareFloatBufferedImages(actual, expected) must beTrue
    }.pendingUntilFixed

    "scale 1x10 Floats to 1x20" in {
      val expected =  Array.fill(10)(Array.fill[Float](1)(1.6.toFloat)) ++
                      Array.fill(10)(Array.fill[Float](1)(2.5.toFloat))
      val actual = RasterUtils.scaleBufferedImage(1, 20, testRasterFloat1x10)
      compareFloatBufferedImages(actual, expected) must beTrue
    }.pendingUntilFixed

    "scale 821x41 Floats to 660x330" in {
      val actual = RasterUtils.scaleBufferedImage(660, 330, Array.fill[Float](41, 821)(123.456.toFloat))
      val expected: BufferedImage = Array.fill[Float](330, 660)(123.456.toFloat)
      compareFloatBufferedImages(actual, expected) must beTrue
    }.pendingUntilFixed("Fixed scaling large Float Raster with prime number dimensions")

    "scale 800x40 Floats to 600x300" in {
      val actual = RasterUtils.scaleBufferedImage(660, 330, Array.fill[Float](40, 800)(123.456.toFloat))
      val expected: BufferedImage = Array.fill[Float](330, 660)(123.456.toFloat)
      compareFloatBufferedImages(actual, expected) must beTrue
    }.pendingUntilFixed("Fixed scaling large Float raster")

  }

}
