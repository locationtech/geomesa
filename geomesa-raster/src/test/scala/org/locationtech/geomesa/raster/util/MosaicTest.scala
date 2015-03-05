/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
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

}
