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
package org.locationtech.geomesa.tools.ingest

import java.io.File
import java.util.UUID

import breeze.numerics.ceil
import org.geotools.coverage.grid.GridCoverage2D
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeoHash, GeohashUtils}
import com.typesafe.scalalogging.slf4j.Logging

import scala.collection.parallel.ForkJoinTaskSupport
import scala.sys.process._
import scala.util.Try

class RasterChunking(config: Map[String, Option[String]]) extends RasterIngest {
  lazy val path = config(IngestRasterParams.FILE_PATH).get
  lazy val fileType = config(IngestRasterParams.FORMAT).get
  lazy val parLevel = config(IngestRasterParams.PARLEVEL).get.toInt
  lazy val chunkSize = config(IngestRasterParams.CHUNKSIZE).get.toInt

  def runChunkTask() = Try {
    val outDir = s"/tmp/raster_ingest_${UUID.randomUUID.toString}"
    chunkRasterLocal(path, fileType, outDir, chunkSize)
    outDir
  }

  /**
   * Cut image into chunks so that the size of each chunk is about the given size in bytes,
   * and save chunks into local geotiff files.
   *
   * @param file Raster file to be divided
   * @param fileType Raster file type
   * @param outDir Directory to put chunk files in
   * @param size  Desired size limit in bytes
   */
  def chunkRasterLocal(file: String, fileType: String, outDir: String, size: Double) {
    val rasterReader = getReader(new File(file), fileType)
    val rasterGrid: GridCoverage2D = rasterReader.read(null)
    val envelope = rasterGrid.getEnvelope2D
    val (minX, minY, maxX, maxY) =
      RasterChunking.checkAndModifyBounds(envelope.getMinX, envelope.getMinY, envelope.getMaxX, envelope.getMaxY)
    val bbox = BoundingBox(minX, maxX, minY, maxY)
    val (gridStepX, gridStepY) = RasterChunking.getSteps(rasterGrid)
    val chunkPrec = RasterChunking.GetGeoHashPrecisionBySize(rasterGrid, size)
    val llGh = GeoHash(minX, minY, chunkPrec)
    val urGh = GeoHash(maxX, maxY, chunkPrec)
    val (llX, llY, urX, urY) = (llGh.getPoint.getX, llGh.getPoint.getY, urGh.getPoint.getX, urGh.getPoint.getY)
    val (deltaX, deltaY) = ((llGh.bbox.ur.getX - llGh.bbox.ll.getX), (llGh.bbox.ur.getY - llGh.bbox.ll.getY))
    val (stepsX, stepsY) = (Math.ceil((urX - llX) / deltaX).toInt, Math.ceil((urY - llY) / deltaY).toInt)

    new File(outDir).mkdir

    val indexes =
      (for {
        i <- 0 to stepsX
        j <- 0 to stepsY
      } yield (i, j)).par
    indexes.tasksupport = new ForkJoinTaskSupport(new scala.concurrent.forkjoin.ForkJoinPool(parLevel))

    indexes.foreach { case (i, j) =>
      val geoHash = GeoHash(llX + deltaX * i, llY + deltaY * j, chunkPrec)
      getChunkBounds(geoHash, bbox, gridStepX, gridStepY).foreach { case (minX, maxX, minY, maxY) =>
        if (maxX > minX && maxY > minY) {
          val cmd = Seq("gdal_translate", "-q", "-of", "GTIFF", "-projwin", s"${minX}", s"${maxY}",
            s"${maxX}", s"${minY}", file, s"${outDir}/gh_${geoHash.hash}.tif")
          cmd.!!
        }
      }
    }
  }

  //Get bounds from given GeoHash that are used to crop raster
  def getChunkBounds(gh: GeoHash,
                     bbox: BoundingBox,
                     stepX: Double,
                     stepY: Double): Option[(Double, Double, Double, Double)] = {
    BoundingBox.intersects(bbox, gh.bbox) match {
      case false => None
      case _ =>
        val (ghMinX, ghMinY, ghMaxX, ghMaxY) = (gh.bbox.ll.getX, gh.bbox.ll.getY, gh.bbox.ur.getX, gh.bbox.ur.getY)
        val (rMinX, rMinY, rMaxX, rMaxY) = (bbox.ll.getX, bbox.ll.getY, bbox.ur.getX, bbox.ur.getY)
        Some(getCropLocation(ghMinX, rMinX, rMaxX, stepX, false),
             getCropLocation(ghMaxX, rMinX, rMaxX, stepX, true),
             getCropLocation(ghMinY, rMinY, rMaxY, stepY, false),
             getCropLocation(ghMaxY, rMinY, rMaxY, stepY, true))
    }
  }

  //Get location on raster grid for cropping operation
  def getCropLocation(coor: Double,
                      minCoor: Double,
                      maxCoor: Double,
                      step: Double,
                      isMax: Boolean): Double = {
    val steps = ((coor - minCoor) / step).toInt
    val calCoor = {
      val tmpCoor = minCoor + step * steps.toDouble
      //NO DUPLICATION: With the following line, adjacent chunks have no overlapped edge.
      if (coor - tmpCoor > 0.5D * step) tmpCoor + step
      //DUPLICATION POSSIBLE: With the following line, adjacent chunks may have one edge
      //(width = 1) overlapped.
//      if (isMax && coor - tmpCoor > 0D) tmpCoor + step
      else tmpCoor
    }
    Math.min(Math.max(calCoor, minCoor), maxCoor)
  }
}

object RasterChunking extends Logging {
  val BOUND_MIN_X = -180.0
  val BOUND_MIN_Y = -90.0
  val BOUND_MAX_X = 180.0
  val BOUND_MAX_Y = 89.999999

  //Get precision of geohashe with size no larger than given bound
  def GetGeoHashPrecisionBySize(rasterGrid: GridCoverage2D, size: Double): Int = {
    val envelope = rasterGrid.getEnvelope2D
    val (stepX, stepY) = getSteps(rasterGrid)
    val pixelSize = getPixelSize(rasterGrid)
    val bbox = BoundingBox(envelope.getMinX, envelope.getMaxX, envelope.getMinY, envelope.getMaxY)
    var gh = GeohashUtils.getClosestAcceptableGeoHash(bbox.geom.getEnvelopeInternal).getOrElse(
      throw new Exception("No closest acceptable Geohash found. Cannot chunk raster."))

    while (getGridSize(gh, stepX, stepY) * pixelSize / 1000.0 > size) {
      gh = GeoHash(s"${gh.hash}0")
    }

    gh.prec
  }

  //Get steps on horizontal and vertical dimensions of given raster
  def getSteps(rasterGrid: GridCoverage2D): (Double, Double) = {
    val envelope = rasterGrid.getEnvelope2D
    val (width, height) = (envelope.getWidth, envelope.getHeight)
    val gridRange = rasterGrid.getGridGeometry.getGridRange
    val (dimX, dimY) = (gridRange.getSpan(0), gridRange.getSpan(1))
    if (dimX == 0 || dimY == 0)
      throw new Exception("Raster has empty dimension. Cannot calculate steps.")
    (width / dimX, height / dimY)
  }

  //Get pixel size in bytes
  def getPixelSize(gh: GridCoverage2D): Int = {
    val sm = gh.getRenderedImage.getSampleModel
    (0 to sm.getNumBands - 1).map { i => sm.getSampleSize(i) }.sum / 8
  }

  //Get total amount of points on the grid defined by given GeoHash
  def getGridSize(gh: GeoHash, stepX: Double, stepY: Double): Int =
    ceil((gh.bbox.ur.getX - gh.bbox.ll.getX) / stepX).toInt * ceil((gh.bbox.ur.getY - gh.bbox.ll.getY) / stepY).toInt

  //Check if input bounds are beyond bounds limits and adjust bounds if they are
  def checkAndModifyBounds(minX: Double, minY: Double, maxX: Double, maxY: Double): (Double, Double, Double, Double) = {
    if (minX < BOUND_MIN_X || minY < BOUND_MIN_Y || maxX > BOUND_MAX_X || maxY > BOUND_MAX_Y)
      logger.info(s"Warning: Raster bounds ($minX, $minY, $maxX, $maxY) are beyond bound limits.")
    (Math.max(minX, BOUND_MIN_X), Math.max(minY, BOUND_MIN_Y), Math.min(maxX, BOUND_MAX_X), Math.min(maxY, BOUND_MAX_Y))
  }
}