package org.locationtech.geomesa.raster.util

import java.awt.image.{BufferedImage, RenderedImage, WritableRaster}
import java.awt.{AlphaComposite, Color, Graphics2D}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import javax.media.jai.remote.SerializableRenderedImage

import org.geotools.coverage.grid.{GridCoverage2D, GridCoverageFactory}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.DateTime
import org.locationtech.geomesa.core.index.DecodedIndex
import org.locationtech.geomesa.raster.data.{RasterQuery, RasterStore}
import org.locationtech.geomesa.raster.feature.Raster
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeoHash}
import org.opengis.geometry.Envelope

import scala.reflect.runtime.universe._

object RasterUtils {

  object IngestRasterParams {
    val ACCUMULO_INSTANCE   = "geomesa-tools.ingestraster.instance"
    val ZOOKEEPERS          = "geomesa-tools.ingestraster.zookeepers"
    val ACCUMULO_MOCK       = "geomesa-tools.ingestraster.useMock"
    val ACCUMULO_USER       = "geomesa-tools.ingestraster.user"
    val ACCUMULO_PASSWORD   = "geomesa-tools.ingestraster.password"
    val AUTHORIZATIONS      = "geomesa-tools.ingestraster.authorizations"
    val VISIBILITIES        = "geomesa-tools.ingestraster.visibilities"
    val FILE_PATH           = "geomesa-tools.ingestraster.path"
    val FORMAT              = "geomesa-tools.ingestraster.format"
    val TIME                = "geomesa-tools.ingestraster.time"
    val GEOSERVER_REG       = "geomesa-tools.ingestraster.geoserver.reg"
    val RASTER_NAME         = "geomesa-tools.ingestraster.name"
    val TABLE               = "geomesa-tools.ingestraster.table"
  }

  def imageSerialize(image: RenderedImage): Array[Byte] = {
    val buffer: ByteArrayOutputStream = new ByteArrayOutputStream
    val out: ObjectOutputStream = new ObjectOutputStream(buffer)
    val serializableImage = new SerializableRenderedImage(image, true)
    try {
      out.writeObject(serializableImage)
    } finally {
      out.close
    }
    buffer.toByteArray
  }

  def imageDeserialize(imageBytes: Array[Byte]): RenderedImage = {
    val in: ObjectInputStream = new ObjectInputStream(new ByteArrayInputStream(imageBytes))
    var read: RenderedImage = null
    try {
      read = in.readObject.asInstanceOf[RenderedImage]
    } finally {
      in.close
    }
    read
  }

  val defaultGridCoverageFactory = new GridCoverageFactory

  def renderedImageToGridCoverage2d(name: String, image: RenderedImage, env: Envelope): GridCoverage2D =
    defaultGridCoverageFactory.create(name, image, env)

  def getEmptyImage(width: Int = 256, height: Int = 256) = {
    val emptyImage = new BufferedImage(width, height, BufferedImage.TYPE_BYTE_GRAY)
    val g2D = emptyImage.getGraphics.asInstanceOf[Graphics2D]
    val save = g2D.getColor
    g2D.setColor(Color.WHITE)
    g2D.setComposite(AlphaComposite.Clear)
    g2D.fillRect(0, 0, emptyImage.getWidth, emptyImage.getHeight)
    g2D.setColor(save)
    emptyImage
  }

  def mosaicRasters(rasters: Iterator[Raster], width: Int, height: Int, env: Envelope, resx: Double, resy: Double) = {
    val rescaleX: Double = resx / (env.getSpan(0) / width)
    val rescaleY: Double = resy / (env.getSpan(1) / height)
    val newWidth: Double = width / rescaleX
    val newHeight: Double = height / rescaleY
    val imageWidth = Math.max(Math.round(newWidth), 1).toInt
    val imageHeight = Math.max(Math.round(newHeight), 1).toInt
    val image = getEmptyImage(imageWidth, imageHeight)
    while(rasters.hasNext) {
      val raster = rasters.next()
      val coverageEnv = raster.envelope
      val coverageImage = raster.chunk
      val dx = ((coverageEnv.getMinimum(0) - env.getMinimum(0)) / resx).toInt
      val dy = ((env.getMaximum(1) - coverageEnv.getMaximum(1)) / resy).toInt
      image.getRaster.setRect(dx, dy, coverageImage.getData)
    }
    image
  }
  //TODO: WCS: Split off functions useful for just tests into a separate object, which includes classes from here on down
  val white = Array[Int] (255, 255, 255)
  val black = Array[Int] (0, 0, 0)

  def getNewImage[T: TypeTag](w: Int, h: Int, fill: Array[T], imageType: Int = BufferedImage.TYPE_BYTE_GRAY): BufferedImage = {
    val image = new BufferedImage(w, h, imageType)
    val wr = image.getRaster
    val setPixel: (Int, Int) => Unit = typeOf[T] match {
      case t if t =:= typeOf[Int]    =>
        (i, j) => wr.setPixel(j, i, fill.asInstanceOf[Array[Int]])
      case t if t =:= typeOf[Float]  =>
        (i, j) => wr.setPixel(j, i, fill.asInstanceOf[Array[Float]])
      case t if t =:= typeOf[Double] =>
        (i, j) => wr.setPixel(j, i, fill.asInstanceOf[Array[Double]])
      case _                         =>
        throw new IllegalArgumentException(s"Error, cannot handle Arrays of type: ${typeOf[T]}")
    }

    for (i <- 1 until h; j <- 1 until w) { setPixel(i, j) }
    image
  }

  def imageToCoverage(img: WritableRaster, env: ReferencedEnvelope, cf: GridCoverageFactory) = {
    cf.create("testRaster", img, env)
  }

  def createRasterStore(tableName: String) = {
    val rs = RasterStore("user", "pass", "testInstance", "zk", tableName, "SUSA", "SUSA", true)
    rs
  }

  def generateQuery(minX: Double, maxX: Double, minY: Double, maxY: Double, res: Double = 10.0) = {
    val bb = BoundingBox(new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84))
    new RasterQuery(bb, res, None, None)
  }

  def generateTestRaster(minX: Double, maxX: Double, minY: Double, maxY: Double, w: Int = 256, h: Int = 256, res: Double = 10.0): Raster = {
    val ingestTime = new DateTime()
    val env = new ReferencedEnvelope(minX, maxX, minY, maxY, DefaultGeographicCRS.WGS84)
    val bbox = BoundingBox(env)
    val metadata = DecodedIndex(Raster.getRasterId("testRaster"), bbox.geom, Option(ingestTime.getMillis))
    val image = getNewImage(w, h, Array[Int](255, 255, 255))
    val coverage = imageToCoverage(image.getRaster, env, defaultGridCoverageFactory)
    new Raster(coverage.getRenderedImage, metadata, res)
  }

  def generateTestRasterFromBoundingBox(bbox: BoundingBox, w: Int = 256, h: Int = 256, res: Double = 10.0): Raster = {
    generateTestRaster(bbox.minLon, bbox.maxLon, bbox.minLat, bbox.maxLat, w, h, res)
  }

  def generateTestRasterFromGeoHash(gh: GeoHash, w: Int = 256, h: Int = 256, res: Double = 10.0): Raster = {
    generateTestRasterFromBoundingBox(gh.bbox, w, h, res)
  }
}

