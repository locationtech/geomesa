package org.locationtech.geomesa.plugin.wcs

import java.awt.image._
import java.io.{ByteArrayOutputStream, File}
import javax.imageio.ImageIO

import breeze.util.ArrayUtil
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.factory.Hints
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.junit.runner.RunWith
import org.locationtech.geomesa.raster.util.RasterUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable.ListBuffer
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class GeoMesaCoverageReaderTest extends Specification {
  val white = Array[Int] (255, 255, 255)
  val black = Array[Int] (0, 0, 0)

  def getNewImage(width: Int, height: Int, color: Array[Int]): BufferedImage = {
    val image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val wr = image.getRaster
    var h = 0
    var w = 0
    for(h <- 1 until height) {
      for (w <- 1 until width) {
        wr.setPixel(w, h, color)
      }
    }
    image
  }

  def getTestGridCoverages(width: Int, height: Int): Iterator[GridCoverage2D] = {
    val whiteImage = getNewImage(width, height, white)
    val blackImage = getNewImage(width, height, black)
    val coverageFactory = CoverageFactoryFinder.getGridCoverageFactory(new Hints())
    val envTR = new ReferencedEnvelope(0, width, 0, height, DefaultGeographicCRS.WGS84)
    val envBR = new ReferencedEnvelope(0, width, height * -1, 0, DefaultGeographicCRS.WGS84)
    val envTL = new ReferencedEnvelope(width * -1, 0, 0, height, DefaultGeographicCRS.WGS84)
    val envBL = new ReferencedEnvelope(width * -1, 0, height * -1, 0, DefaultGeographicCRS.WGS84)
    val coverageList = new ListBuffer[GridCoverage2D]()
    coverageList += coverageFactory.create("image1", whiteImage, envTL)
    coverageList += coverageFactory.create("image2", blackImage, envTR)
    coverageList += coverageFactory.create("image3", blackImage, envBL)
    coverageList += coverageFactory.create("image4", whiteImage, envBR)
    coverageList.toIterator
  }

  def getImageFromGridCoverages(gridCoverageIterator: Iterator[GridCoverage2D], width: Int, height: Int): BufferedImage = {
    val envTotal =  new ReferencedEnvelope(width * -1, width, height * -1, height, DefaultGeographicCRS.WGS84)
    RasterUtils.mosaicGridCoverages(gridCoverageIterator, width * 2, height * 2, envTotal)
  }

  def getImageByteArray(imageName: String): Array[Byte] = {
    val checkImage = ImageIO.read(new File(imageName))
    val baos = new ByteArrayOutputStream()
    ImageIO.write(checkImage, "png", baos)
    baos.toByteArray
  }

  def runMosaicTest(width: Int , height: Int) = {
    val gridCoverageIterator = getTestGridCoverages(width, height)
    val image = getImageFromGridCoverages(gridCoverageIterator, width, height)
    val randString = Random.alphanumeric.take(5).mkString
    val tempFile = File.createTempFile(s"test-$randString", ".png")
    ImageIO.write(image, "png", tempFile)
    val checkImage = getImageByteArray(this.getClass.getClassLoader.getResource(s"wcs/test-$width-$height.png").getPath)
    val newImage = getImageByteArray(tempFile.getAbsolutePath)
    ArrayUtil.equals(checkImage, newImage) must beTrue
  }

  "GeoMesaCoverageReader" should {
    "mosaic GridCoverages appropriately for 4 10x10 grid coverages" in {
      runMosaicTest(10, 10)
    }

    "mosaic GridCoverages appropriately for 4 100x100 grid coverages" in {
      runMosaicTest(100, 100)
    }

    "mosaic GridCoverages appropriately for 4 1000x1000 grid coverages" in {
      runMosaicTest(1000, 1000)
    }
  }
}
