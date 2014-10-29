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

package org.locationtech.geomesa.plugin.wms

import java.awt.Point
import java.awt.image._
import javax.media.jai.{PlanarImage, RasterFactory}

import org.locationtech.geomesa.utils.geohash.{GeoHash, TwoGeoHashBoundingBox}

object ImageUtils {
  def createTileImage(ghBbox: TwoGeoHashBoundingBox,
                              ghvs: Iterator[(GeoHash,Double)]): RenderedImage = {
    val xdim: Int = math
                    .round(ghBbox.bbox.longitudeSize /
                           ghBbox.ur.bbox.longitudeSize - 1.0)
                    .asInstanceOf[Int]
    val ydim: Int = math
                    .round(ghBbox.bbox.latitudeSize /
                           ghBbox.ur.bbox.latitudeSize - 1.0)
                    .asInstanceOf[Int]
    val buffer = createImageBuffer(xdim, ydim)
    setImagePixels(ghvs, buffer, xdim, ydim, ghBbox)
    val resized: BufferedImage = drawImage(buffer, xdim, ydim)
    resized
  }

  private def createImageBuffer(xdim: Int, ydim: Int) = {
    val defaultColor = 0//black won't work - it has full transparency
    val bufferSize: Int = xdim * ydim
    val iBuffer = Array.ofDim[Byte](1, bufferSize)
    var i: Int = 0
    while (i < ydim) {
          var j: Int = 0
          while (j < xdim) {
            val idx: Int = i * xdim + j
            iBuffer(0)(idx) = defaultColor.asInstanceOf[Byte]
            j += 1
      }
      i += 1
    }
    iBuffer
  }

  private def setImagePixels(ghvs: Iterator[(GeoHash, Double)],
                             buffer: Array[Array[Byte]],
                             xdim: Int,
                             ydim: Int,
                             ghBbox: TwoGeoHashBoundingBox) {
    val dxDegrees = ghBbox.ur.bbox.longitudeSize
    val dyDegrees = ghBbox.ur.bbox.latitudeSize
    for (ghv <- ghvs) {
      if (ghBbox.bbox.covers(ghv._1.bbox)// @todo check by index
      ){
        val gh: GeoHash = ghv._1
        val x: Int = Math
                     .round((gh.x - ghBbox.ll.x) /
                            dxDegrees)
                     .asInstanceOf[Int]
        val y: Int = Math.max(ydim -
                              Math.round((gh.y - ghBbox.ll.y) / dyDegrees)
                            .asInstanceOf[Int] -1, 0)
        val idx: Int = math.min(y * xdim + x, xdim*ydim -1 )
        val thisVal: Double = ghv._2
          val tmpColor = thisVal * 255
          buffer(0)(idx) = tmpColor.asInstanceOf[Byte]
      }
    }
  }

  def drawImage(buffer: Array[Array[Byte]], xdim: Int, ydim: Int): BufferedImage = {
    val dbuffer: DataBufferByte = new DataBufferByte(buffer, xdim * ydim)
    val sampleModel: SampleModel = RasterFactory.createBandedSampleModel(DataBuffer.TYPE_BYTE,
                                                                         xdim,
                                                                         ydim,
                                                                         1)
    val colorModel: ColorModel = PlanarImage.createColorModel(sampleModel)
    val raster: WritableRaster = RasterFactory.createWritableRaster(sampleModel,
                                                                    dbuffer,
                                                                    new Point(0, 0))
    new BufferedImage(colorModel, raster, false, null)
  }
}
