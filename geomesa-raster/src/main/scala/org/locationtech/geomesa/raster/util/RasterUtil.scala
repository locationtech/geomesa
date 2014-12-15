package org.locationtech.geomesa.raster.util

import java.awt.{AlphaComposite, Color, Graphics2D}
import java.awt.image.{BufferedImage, RenderedImage}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.ByteBuffer
import java.nio.charset.Charset
import javax.media.jai.remote.SerializableRenderedImage
import org.geotools.coverage.grid.{GridCoverage2D, GridCoverageFactory}
import org.opengis.coverage.grid.GridCoverage
import org.opengis.geometry.Envelope

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

  def mosaicGridCoverages(coverageList: Iterator[GridCoverage], width: Int = 256, height: Int = 256, env: Envelope, startImage: BufferedImage = null) = {
    val image = if (startImage == null) { getEmptyImage(width, height) } else { startImage }
    while (coverageList.hasNext) {
      val coverage = coverageList.next()
      val coverageEnv = coverage.getEnvelope
      val coverageImage = coverage.getRenderedImage
      val posx = ((coverageEnv.getMinimum(0) - env.getMinimum(0)) / 1.0).asInstanceOf[Int]
      val posy = ((env.getMaximum(1) - coverageEnv.getMaximum(1)) / 1.0).asInstanceOf[Int]
      image.getRaster.setDataElements(posx, posy, coverageImage.getData)
    }
    image
  }
}

