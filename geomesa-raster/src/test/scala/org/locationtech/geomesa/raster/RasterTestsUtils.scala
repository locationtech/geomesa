/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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


package org.locationtech.geomesa.raster

import java.awt.image.{RenderedImage, WritableRaster}

import org.geotools.coverage.grid.{GridCoverage2D, GridCoverageFactory}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.DateTime
import org.locationtech.geomesa.core.index.DecodedIndex
import org.locationtech.geomesa.raster.data.{Raster, RasterQuery, RasterStore}
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeoHash}
import org.opengis.geometry.Envelope

object RasterTestsUtils {

  val white     = Array[Int] (255, 255, 255)
  val lightGray = Array[Int] (200, 200, 200)
  val gray      = Array[Int] (128, 128, 128)
  val darkGray  = Array[Int] (54, 54, 54)
  val black     = Array[Int] (0, 0, 0)

  val quadrant1 = BoundingBox(-90.0, -67.5, 22.5, 45.0)
  val quadrant2 = BoundingBox(-112.5, -90.0, 22.5, 45.0)
  val quadrant3 = BoundingBox(-112.5, -90.0, 0, 22.5)
  val quadrant4 = BoundingBox(-90.0, -67.5, 0, 22.5)

  val defaultGridCoverageFactory = new GridCoverageFactory

  def createMockRasterStore(tableName: String) = {
    val rs = RasterStore("user", "pass", "testInstance", "zk", tableName, "", "", true)
    rs
  }

  def generateQuery(minX: Double, maxX: Double, minY: Double, maxY: Double, res: Double = 10.0) = {
    val bb = BoundingBox(new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84))
    new RasterQuery(bb, res, None, None)
  }

  def generateTestRaster(minX: Double, maxX: Double, minY: Double, maxY: Double,
                         w: Int = 256, h: Int = 256, res: Double = 10.0,
                         color: Array[Int] = white): Raster = {
    val ingestTime = new DateTime()
    val env = new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84)
    val bbox = BoundingBox(env)
    val metadata = DecodedIndex(Raster.getRasterId("testRaster"), bbox.geom, Option(ingestTime.getMillis))
    val image = RasterUtils.getNewImage(w, h, color)
    val coverage = imageToCoverage(image.getRaster, env, defaultGridCoverageFactory)
    Raster(coverage.getRenderedImage, metadata, res)
  }

  def generateTestRasterFromBoundingBox(bbox: BoundingBox, w: Int = 256, h: Int = 256, res: Double = 10.0): Raster = {
    generateTestRaster(bbox.minLon, bbox.maxLon, bbox.minLat, bbox.maxLat, w, h, res)
  }

  def generateTestRasterFromGeoHash(gh: GeoHash, w: Int = 256, h: Int = 256, res: Double = 10.0): Raster = {
    generateTestRasterFromBoundingBox(gh.bbox, w, h, res)
  }

  def imageToCoverage(img: WritableRaster, env: ReferencedEnvelope, cf: GridCoverageFactory) = {
    cf.create("testRaster", img, env)
  }

  def renderedImageToGridCoverage2d(name: String, image: RenderedImage, env: Envelope): GridCoverage2D =
    defaultGridCoverageFactory.create(name, image, env)

  def generateTestRastersFromBBoxes(bboxes: List[BoundingBox]): List[Raster] = {
    bboxes.map(generateTestRasterFromBoundingBox(_))
  }

  def generateQuadTreeLevelRasters(level: Int,
                                   q1: BoundingBox = quadrant1,
                                   q2: BoundingBox = quadrant2,
                                   q3: BoundingBox = quadrant3,
                                   q4: BoundingBox = quadrant4): List[Raster] = level match {
    case lowBound if level <= 1  =>
      generateTestRastersFromBBoxes(List(q1, q2, q3, q4))
    case highBound if level > 30 =>
      val bboxList = List(generateSubQuadrant(30, q1, 1),
        generateSubQuadrant(30, q2, 2),
        generateSubQuadrant(30, q3, 3),
        generateSubQuadrant(30, q4, 4))
      generateTestRastersFromBBoxes(bboxList)
    case _                       =>
      val bboxList = List(generateSubQuadrant(level, q1, 1),
        generateSubQuadrant(level, q2, 2),
        generateSubQuadrant(level, q3, 3),
        generateSubQuadrant(level, q4, 4))
      generateTestRastersFromBBoxes(bboxList)
  }

  def generateSubQuadrant(level: Int, b: BoundingBox, q: Int): BoundingBox = {
    val delta = getDelta(level)
    q match {
     case 1 =>
       BoundingBox(b.ll.getX, b.ll.getX + delta, b.ll.getY, b.ll.getY + delta)
     case 2 =>
       BoundingBox(b.ur.getX - delta, b.ur.getX, b.ll.getY, b.ll.getY + delta)
     case 3 =>
       BoundingBox(b.ur.getX - delta, b.ur.getX, b.ur.getY - delta, b.ur.getY)
     case 4 =>
       BoundingBox(b.ll.getX, b.ll.getX + delta, b.ur.getY - delta, b.ur.getY)
     case _ => b
    }
  }

  private def getDelta(level: Int): Double = 45.0 / Math.pow(2, level)

}
