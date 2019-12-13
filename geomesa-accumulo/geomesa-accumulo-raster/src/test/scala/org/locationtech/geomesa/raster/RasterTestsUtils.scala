/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.raster

import java.awt.image.{BufferedImage, RenderedImage, WritableRaster}
import java.util.{Date, UUID}

import org.geotools.coverage.grid.{GridCoverage2D, GridCoverageFactory}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.raster.data.{AccumuloRasterStore, Raster, RasterQuery}
import org.locationtech.geomesa.raster.index.RasterEntryDecoder.DecodedIndexValue
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

  val wholeworld = BoundingBox(-180, 180, -90, 90)

  val defaultGridCoverageFactory = new GridCoverageFactory

  def createMockRasterStore(tableName: String) = {
    AccumuloRasterStore("user", "pass", "testInstance", "zk", tableName, "", "", useMock = true)
  }

  def generateQuery(minX: Double, maxX: Double, minY: Double, maxY: Double, res: Double = 10.0) = {
    val bb = BoundingBox(new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84))
    new RasterQuery(bb, res)
  }

  implicit def bboxToRefEnv(b: BoundingBox): ReferencedEnvelope =
    new ReferencedEnvelope(b.minLon, b.maxLon, b.minLat, b.maxLat, DefaultGeographicCRS.WGS84)

  def generateRaster(bbox: BoundingBox, buf: BufferedImage, id: String = Raster.getRasterId("testRaster")): Raster = {
    val metadata = DecodedIndexValue(id, bbox.geom, Some(new Date()), null)
    val coverage = imageToCoverage(buf.getRaster, bbox, defaultGridCoverageFactory)
    Raster(coverage.getRenderedImage, metadata, 10.0)
  }

  def generateTestRaster(minX: Double, maxX: Double, minY: Double, maxY: Double,
                         w: Int = 256, h: Int = 256, res: Double = 10.0,
                         color: Array[Int] = white): Raster = {
    val env = new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84)
    val bbox = BoundingBox(env)
    val metadata = DecodedIndexValue(Raster.getRasterId("testRaster"), bbox.geom, Some(new Date()), null)
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

  def compareIntBufferedImages(act: BufferedImage, exp: BufferedImage): Boolean = {
    //compare basic info
    if (act.getWidth != exp.getWidth) false
    else if (act.getHeight != exp.getHeight) false
    else {
      val actWR = act.getRaster
      val expWR = exp.getRaster
      val actual = for(i <- (0 until act.getWidth).iterator;
                       j <- 0 until act.getHeight) yield actWR.getSample(i, j, 0)
      val expected = for(i <- (0 until act.getWidth).iterator;
                         j <- 0 until act.getHeight) yield expWR.getSample(i, j, 0)
      actual.sameElements(expected)
    }
  }

  def compareFloatBufferedImages(act: BufferedImage, exp: BufferedImage): Boolean = {
    //compare basic info
    if (act.getWidth != exp.getWidth) false
    else if (act.getHeight != exp.getHeight) false
    else {
      val actWR = act.getRaster
      val expWR = exp.getRaster
      val actual = for(i <- (0 until act.getWidth).iterator;
                       j <- 0 until act.getHeight) yield actWR.getSampleFloat(i, j, 0)
      val expected = for(i <- (0 until act.getWidth).iterator;
                         j <- 0 until act.getHeight) yield expWR.getSampleFloat(i, j, 0)
      actual.sameElements(expected)
    }
  }

  // check the setPixels to make sure we are not going out of bounds!
  val testRasterIntSolid: BufferedImage = {
    val image = new BufferedImage(16, 16, BufferedImage.TYPE_BYTE_GRAY)
    val wr = image.getRaster
    wr.setPixels(0,0,16,16, Array.fill[Int](16*16){1})
    image
  }

  val testRasterInt10x1: BufferedImage = {
    val image = new BufferedImage(10, 1, BufferedImage.TYPE_BYTE_GRAY)
    val wr = image.getRaster
    wr.setPixels(0,0,5,1, Array.fill[Int](5)(1))
    wr.setPixels(5,0,5,1, Array.fill[Int](5)(2))
    image
  }

  val redHerring: BufferedImage = {
    val image = new BufferedImage(16, 16, BufferedImage.TYPE_BYTE_GRAY)
    val wr = image.getRaster
    wr.setPixels(0,0,16,16, Array.fill[Int](16*16){42})
    image
  }

  val testRasterIntVSplit: BufferedImage = {
    val image = new BufferedImage(16, 16, BufferedImage.TYPE_BYTE_GRAY)
    val wr = image.getRaster
    wr.setPixels(0,0,8,16, Array.fill[Int](8*16){1})
    wr.setPixels(8,0,8,16, Array.fill[Int](8*16){2})
    image
  }

  val testRasterIntHSplit: BufferedImage = {
    val image = new BufferedImage(16, 16, BufferedImage.TYPE_BYTE_GRAY)
    val wr = image.getRaster
    wr.setPixels(0,0,16,8, Array.fill[Int](8*16){1})
    wr.setPixels(0,8,16,8, Array.fill[Int](8*16){2})
    image
  }

  val testRasterIntQuadrants: BufferedImage = {
    val image = new BufferedImage(16, 16, BufferedImage.TYPE_BYTE_GRAY)
    val wr = image.getRaster
    wr.setPixels(8,0,8,8, Array.fill[Int](8*8){1})
    wr.setPixels(0,0,8,8, Array.fill[Int](8*8){2})
    wr.setPixels(0,8,8,8, Array.fill[Int](8*8){3})
    wr.setPixels(8,8,8,8, Array.fill[Int](8*8){4})
    image
  }

  val testRasterIntQuadrants2x2: BufferedImage = {
    val image = new BufferedImage(2, 2, BufferedImage.TYPE_BYTE_GRAY)
    val wr = image.getRaster
    wr.setPixels(1,0,1,1, Array.fill[Int](1){1})
    wr.setPixels(0,0,1,1, Array.fill[Int](1){2})
    wr.setPixels(0,1,1,1, Array.fill[Int](1){3})
    wr.setPixels(1,1,1,1, Array.fill[Int](1){4})
    image
  }

  val testRasterIntQuadrants4x4: BufferedImage = {
    val image = new BufferedImage(4, 4, BufferedImage.TYPE_BYTE_GRAY)
    val wr = image.getRaster
    wr.setPixels(2,0,2,2, Array.fill[Int](4){1})
    wr.setPixels(0,0,2,2, Array.fill[Int](4){2})
    wr.setPixels(0,2,2,2, Array.fill[Int](4){3})
    wr.setPixels(2,2,2,2, Array.fill[Int](4){4})
    image
  }

  val testRasterIntQuadrants8x8: BufferedImage = {
    val image = new BufferedImage(8, 8, BufferedImage.TYPE_BYTE_GRAY)
    val wr = image.getRaster
    wr.setPixels(4,0,4,4, Array.fill[Int](16){1})
    wr.setPixels(0,0,4,4, Array.fill[Int](16){2})
    wr.setPixels(0,4,4,4, Array.fill[Int](16){3})
    wr.setPixels(4,4,4,4, Array.fill[Int](16){4})
    image
  }

  val testRasterFloat10x1: BufferedImage =
    Array(Array.fill[Float](5){1.6.toFloat} ++ Array.fill[Float](5){2.5.toFloat})

  val testRasterFloat1x10: BufferedImage =
    Array.fill(5)(Array.fill[Float](1){1.6.toFloat}) ++ Array.fill(5)(Array.fill[Float](1){2.5.toFloat})

  implicit def float2DArrayToBufferedImage(fa: Array[Array[Float]]): BufferedImage = {
    val gc = defaultGridCoverageFactory.create(UUID.randomUUID.toString, fa, quadrant1)
    RasterUtils.renderedImageToBufferedImage(gc.getRenderedImage)
  }

}
