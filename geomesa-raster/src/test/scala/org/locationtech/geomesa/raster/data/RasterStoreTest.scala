package org.locationtech.geomesa.raster.data

import java.awt._
import java.awt.color.ColorSpace
import java.awt.image._
import java.io._
import javax.imageio.ImageIO
import javax.media.jai.remote.SerializableRenderedImage

import com.vividsolutions.jts.geom.Geometry
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.{GridCoverageFactory, GridCoverage2D}
import org.geotools.factory.Hints
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
//import org.jaitools.imageutils.ImageUtils
import org.joda.time.DateTime
import org.junit.runner.RunWith
import org.locationtech.geomesa.core.index.DecodedIndex
//import org.locationtech.geomesa.plugin.ImageUtils
import org.locationtech.geomesa.raster.feature.Raster
import org.locationtech.geomesa.raster.ingest.SimpleRasterIngest._
import org.locationtech.geomesa.utils.geohash.{GeoHash, TwoGeoHashBoundingBox, BoundingBox}
import org.opengis.coverage.grid.GridCoverage
import org.opengis.geometry.Envelope
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.mutable.ListBuffer



@RunWith(classOf[JUnitRunner])
class RasterStoreTest extends Specification {

  def createAndFillRasterStore = {

    //val rs = RasterStore("user", "pass", getClass.toString, "zk", getClass.toString, "S,USA", "S,USA", true)
    val rs = RasterStore("user", "pass", getClass.toString, "zk", getClass.toString, "SUSA", "SUSA", true)

    val rasterName = "testRaster"

    val ingestTime = new DateTime()

    val coverageFactory = CoverageFactoryFinder.getGridCoverageFactory(new Hints())

    val env = new ReferencedEnvelope(0, 50, 0, 50, DefaultGeographicCRS.WGS84)

    val bbox = BoundingBox(env)

    val metadata = DecodedIndex(Raster.getRasterId(rasterName), bbox.geom, Option(ingestTime.getMillis))

    val image = getNewImage(500, 500, Array[Int](255, 255, 255))

    val coverage = imageToCoverage(500, 500, image.getRaster(), env, coverageFactory)

    val raster = new Raster(coverage.getRenderedImage, metadata)

    rs.ensureTableExists()
    rs.putRaster(raster)
    rs
  }
  def generateQuery = {
    ???
  }

  // stolen from elsewhere
  def getNewImage(width: Int, height: Int, color: Array[Int]): BufferedImage = {
    val image = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val wr = image.getRaster
    var h = 0
    var w = 0
    for (h <- 1 until height) {
      for (w <- 1 until width) {
        wr.setPixel(w, h, color)
      }
    }
    image
  }

  def imageToCoverage(width: Int, height: Int, img: WritableRaster, env: ReferencedEnvelope, cf: GridCoverageFactory) = {
    cf.create("testRaster", img, env)
  }

  "RasterStore" should {
    "create a Raster Store" in {
      val theStore = createAndFillRasterStore
      theStore must beAnInstanceOf[RasterStore]
      //val theIterator = theStore.getRasters()
    }
  }





}
