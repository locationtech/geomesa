package org.locationtech.geomesa.raster.data

import java.awt.image._
import java.io.ByteArrayOutputStream
import javax.imageio.ImageIO

import breeze.util.ArrayUtil
import com.vividsolutions.jts.geom.Envelope
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.factory.Hints
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.index.DecodedIndex
import org.locationtech.geomesa.raster.feature.Raster
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.utils.geohash.BoundingBox
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable.ListBuffer
import scala.util.Random

//TODO: WCS: Improve this integration test by dealing with issues ID'd below
// GEOMESA-571
@RunWith(classOf[JUnitRunner])
class RasterStoreTest extends Specification {

  sequential

  val white = Array[Int] (255, 255, 255)
  val gray = Array[Int] (100, 100, 100)
  def getRasterStore = RasterStore("user",
                                   "pass",
                                   "testInstance",
                                   "zk",
                                   s"wcs_mosaic_test_${Random.alphanumeric.take(5).mkString}",
                                   "SUSA",
                                   "SUSA",
                                   useMock = true)

  //TODO: WCS: refactor to separate the creation of the RasterStore, creation of multiple images and insertion
  //           into separate functions.
  //TODO: WCS: need tests to show finding one image, no images and several images
  def createAndFillRasterStore = {
    val rs = getRasterStore
    val rasterName = "testRaster"
    val ingestTime = new DateTime()
    val coverageFactory = CoverageFactoryFinder.getGridCoverageFactory(new Hints())
    val env = new ReferencedEnvelope(0, 50, 0, 50, DefaultGeographicCRS.WGS84)
    val bbox = BoundingBox(env)
    val metadata = DecodedIndex(Raster.getRasterId(rasterName), bbox.geom, Option(ingestTime.getMillis))
    val image = getNewImage(500, 500, Array[Int](255, 255, 255))
    val coverage = coverageFactory.create("testRaster", image.getRaster, env)
    val raster = new Raster(coverage.getRenderedImage, metadata, 1.0)
    rs.putRaster(raster)
    rs
  }

  def generateQuery(envelope: Envelope) = {
    val bb = BoundingBox(envelope)
    new RasterQuery(bb, 1.0, None, None)
  }

  def getNewImage(width: Int, height: Int, color: Array[Int]): BufferedImage = {
    val image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val wr = image.getRaster
    for(h <- 1 until height) {
      for (w <- 1 until width) {
        wr.setPixel(w, h, color)
      }
    }
    image
  }

  def getRasterMetadata(env: ReferencedEnvelope, rasterName: String, ingestTime: DateTime): DecodedIndex = {
    val bbox = BoundingBox(env)
    DecodedIndex(Raster.getRasterId(rasterName), bbox.geom, Option(ingestTime.getMillis))
  }

  def getTestRasters(width: Int, height: Int, bbox: BoundingBox): List[Raster] = {
    val whiteImage = getNewImage(width, height, white)
    val grayImage = getNewImage(width, height, gray)
    val ingestTime = new DateTime()
    val midpoint = ((bbox.getMaxX + bbox.getMinX) / 2.0, (bbox.getMaxY + bbox.getMinY) / 2.0)
    val envTR = new ReferencedEnvelope(midpoint._1, bbox.getMaxX, midpoint._2, bbox.getMaxY, DefaultGeographicCRS.WGS84)
    val envBR = new ReferencedEnvelope(midpoint._1, bbox.getMaxX, bbox.getMinY, midpoint._2, DefaultGeographicCRS.WGS84)
    val envTL = new ReferencedEnvelope(bbox.getMinX, midpoint._1, midpoint._2, bbox.getMaxY, DefaultGeographicCRS.WGS84)
    val envBL = new ReferencedEnvelope(bbox.getMinX, midpoint._1, bbox.getMinY, midpoint._2, DefaultGeographicCRS.WGS84)
    val metadataTR = getRasterMetadata(envTR, "imageTR", ingestTime)
    val metadataBR = getRasterMetadata(envBR, "imageBR", ingestTime)
    val metadataTL = getRasterMetadata(envTL, "imageTL", ingestTime)
    val metadataBL = getRasterMetadata(envBL, "imageBL", ingestTime)
    val rasterList = new ListBuffer[Raster]()
    rasterList += Raster(grayImage, metadataTR, 1.0)
    rasterList += Raster(whiteImage, metadataTL, 1.0)
    rasterList += Raster(grayImage, metadataBL, 1.0)
    rasterList += Raster(whiteImage, metadataBR, 1.0)
    rasterList.toList
  }

  def getImageByteArray(rasterList: List[Raster],
                        width: Int,
                        height: Int,
                        envelope: ReferencedEnvelope,
                        resX: Double,
                        resY: Double): Array[Byte] = {
    val image = RasterUtils.mosaicRasters(rasterList, width * 2, height * 2, envelope, resX, resY)
    val baos = new ByteArrayOutputStream()
    ImageIO.write(image, "png", baos)
    baos.toByteArray
  }

  def runAccumuloMosaicTest(width: Int , height: Int, bbox: BoundingBox) = {
    val envelope = new ReferencedEnvelope(bbox.envelope, DefaultGeographicCRS.WGS84)
    val resX = (envelope.getMaximum(0) - envelope.getMinimum(0)) / (width * 2)
    val resY = (envelope.getMaximum(1) - envelope.getMinimum(1)) / (height * 2)
    val ingestRasters = getTestRasters(width, height, bbox)
    val rs = getRasterStore
    ingestRasters.foreach(rs.putRaster)
    val rq = generateQuery(envelope)
    val queryRasters = rs.getRasters(rq).toList
    val ingestImageByteArray = getImageByteArray(ingestRasters, width, height, envelope, resX, resY)
    val queryImageByteArray = getImageByteArray(queryRasters, width, height, envelope, resX, resY)
    ArrayUtil.equals(ingestImageByteArray, queryImageByteArray) must beTrue
  }

  "RasterStore" should {
    "create a Raster Store" in {
      val theStore = createAndFillRasterStore
      theStore must beAnInstanceOf[RasterStore]
      val theIterator = theStore.getRasters(
        generateQuery(new ReferencedEnvelope(0, 50, 0, 50, DefaultGeographicCRS.WGS84))
      )
      val theRaster = theIterator.next()
      theRaster must beAnInstanceOf[Raster]
    }
  }

  /**
   * For the below tests, the bounding box must be set .0001 outside of the requested area
   * to ensure all geohashes are enumerated and Accumulo is queried as expected.
   */
  "RasterStore" should {
    "mosaic Rasters appropriately for 4 10x10 Rasters" in {
      runAccumuloMosaicTest(10, 10, BoundingBox(-45.0001, 45.0001, -45.0001, 45.0001))
    }

    "mosaic Rasters appropriately for 4 100x100 Rasters" in {
      runAccumuloMosaicTest(100, 100, BoundingBox(-135.0001, -44.9999, -45.0001, 45.0001))
    }

    "mosaic Rasters appropriately for 4 1000x1000 Rasters" in {
      runAccumuloMosaicTest(1000, 1000, BoundingBox(-135.0001, -44.9999, -45.0001, 45.0001))
    }
  }
}
