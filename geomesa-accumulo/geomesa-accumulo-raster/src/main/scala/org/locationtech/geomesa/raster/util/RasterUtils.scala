/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.util

import java.awt.RenderingHints
import java.awt.image.{BufferedImage, Raster => JRaster, RenderedImage}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.{Hashtable => JHashtable}
import javax.media.jai.remote.SerializableRenderedImage

import org.geotools.coverage.grid.GridGeometry2D
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.raster.data.Raster
import org.opengis.geometry.Envelope

import scala.reflect.runtime.universe._

object RasterUtils {

  val defaultBufferedImage = new BufferedImage(5, 5, BufferedImage.TYPE_INT_RGB)

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
    val TABLE               = "geomesa-tools.ingestraster.table"
    val WRITE_MEMORY        = "geomesa-tools.ingestraster.write.memory"
    val WRITE_THREADS       = "geomesa-tools.ingestraster.write.threads"
    val QUERY_THREADS       = "geomesa-tools.ingestraster.query.threads"
    val PARLEVEL            = "geomesa-tools.ingestraster.parallel.level"
    val IS_TEST_INGEST      = "geomesa.tools.ingestraster.is-test-ingest"
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

  def allocateBufferedImage(width: Int, height: Int, chunk: RenderedImage): BufferedImage = {
    val properties = new JHashtable[String, Object]
    if (chunk.getPropertyNames != null) {
      chunk.getPropertyNames.foreach(name => properties.put(name, chunk.getProperty(name)))
    }
    val colorModel = chunk.getColorModel
    val alphaPremultiplied = colorModel.isAlphaPremultiplied
    val sampleModel = chunk.getSampleModel.createCompatibleSampleModel(width, height)
    val emptyRaster = JRaster.createWritableRaster(sampleModel, null)
    new BufferedImage(colorModel, emptyRaster, alphaPremultiplied, properties)
  }

  def allocateBufferedImage(width: Int, height: Int, buf: BufferedImage): BufferedImage = {
    val colorModel = buf.getColorModel
    val alphaPremultiplied = colorModel.isAlphaPremultiplied
    val emptyRaster = colorModel.createCompatibleWritableRaster(width, height)
    new BufferedImage(colorModel, emptyRaster, alphaPremultiplied, null)
  }

  def renderedImageToBufferedImage(r: RenderedImage): BufferedImage = {
    val properties = new JHashtable[String, Object]
    if (r.getPropertyNames != null) {
      r.getPropertyNames.foreach(name => properties.put(name, r.getProperty(name)))
    }
    val colorModel = r.getColorModel
    val alphaPremultiplied = colorModel.isAlphaPremultiplied
    val sampleModel = r.getSampleModel
    new BufferedImage(colorModel, r.copyData(null), alphaPremultiplied, properties)
  }

  def writeToMosaic(mosaic: BufferedImage, raster: Raster, env: Envelope, resX: Double, resY: Double) = {
    val croppedRaster = cropRaster(raster, env)
    croppedRaster.foreach{ cropped =>
        val rasterEnv = raster.referencedEnvelope.intersection(envelopeToReferencedEnvelope(env))
        val originX = Math.floor((rasterEnv.getMinX - env.getMinimum(0)) / resX).toInt
        val originY = Math.floor((env.getMaximum(1) - rasterEnv.getMaxY) / resY).toInt
        mosaic.getRaster.setRect(originX, originY, cropped.getData)
    }
  }

  // TODO: refactor https://geomesa.atlassian.net/browse/GEOMESA-869. Probably move this to a new object also...
  def mosaicChunks(chunks: Iterator[Raster], queryWidth: Int, queryHeight: Int, queryEnv: Envelope): (BufferedImage, Int) = {
    if (chunks.isEmpty) {
      (null, 0)
    } else {
      val firstRaster = chunks.next()
      if (!chunks.hasNext) {
        val croppedRaster = cropRaster(firstRaster, queryEnv)
        croppedRaster match {
          case None      => (null, 1)
          case Some(buf) => (scaleBufferedImage(queryWidth, queryHeight, buf), 1)
        }
      } else {
        val accumuloRasterXRes = firstRaster.referencedEnvelope.getSpan(0) / firstRaster.chunk.getWidth
        val accumuloRasterYRes = firstRaster.referencedEnvelope.getSpan(1) / firstRaster.chunk.getHeight
        //TODO: check for corner cases: https://geomesa.atlassian.net/browse/GEOMESA-758
        val mosaicX = Math.round(queryEnv.getSpan(0) / accumuloRasterXRes).toInt
        val mosaicY = Math.round(queryEnv.getSpan(1) / accumuloRasterYRes).toInt
        if (mosaicX <= 0 || mosaicY <= 0) {
          (null, 1)
        } else {
          var count = 1
          val mosaic = allocateBufferedImage(mosaicX, mosaicY, firstRaster.chunk)
          writeToMosaic(mosaic, firstRaster, queryEnv, accumuloRasterXRes, accumuloRasterYRes)
          while (chunks.hasNext) {
            writeToMosaic(mosaic, chunks.next(), queryEnv, accumuloRasterXRes, accumuloRasterYRes)
            count += 1
          }
          (scaleBufferedImage(queryWidth, queryHeight, mosaic), count)
        }
      }
    }
  }

  def scaleBufferedImage(newWidth: Int, newHeight: Int, image: BufferedImage): BufferedImage = {
    if (image.getWidth == newWidth && image.getHeight == newHeight) {
      image
    } else {
      if (newWidth < 1 || newHeight < 1) null
      else {
        val result = allocateBufferedImage(newWidth, newHeight, image)
        val resGraphics = result.createGraphics()
        //TODO scaling can fail with floating points, very small rasters, etc: https://geomesa.atlassian.net/browse/GEOMESA-869
        resGraphics.setRenderingHint(RenderingHints.KEY_INTERPOLATION, RenderingHints.VALUE_INTERPOLATION_NEAREST_NEIGHBOR)
        resGraphics.drawImage(image, 0, 0, newWidth, newHeight, null)
        resGraphics.dispose()
        result
      }
    }
  }

  def cropRaster(raster: Raster, cropEnv: Envelope): Option[BufferedImage] = {
    val rasterEnv = raster.referencedEnvelope
    val intersection = rasterEnv.intersection(envelopeToReferencedEnvelope(cropEnv))
    if (intersection.equals(rasterEnv)) {
      // If the intersection of the crop envelope and tile is equal to the tile envelope we are done.
      Some(renderedImageToBufferedImage(raster.chunk))
    } else {
      // Check the area of the intersection to ensure it is at least 1x1 pixels
      val chunkWidth   = raster.chunk.getWidth
      val chunkHeight  = raster.chunk.getHeight
      val chunkXRes    = rasterEnv.getWidth / chunkWidth
      val chunkYRes    = rasterEnv.getHeight / chunkHeight
      //TODO: check for corner cases: https://geomesa.atlassian.net/browse/GEOMESA-758
      val widthPixels  = Math.round(intersection.getWidth / chunkXRes)
      val heightPixels = Math.round(intersection.getHeight / chunkYRes)
      if (widthPixels > 0 && heightPixels > 0) {
        // Now that we know the area is at least 1x1 perform the cropping operation
        val uLX = Math.max(Math.floor((intersection.getMinX - rasterEnv.getMinimum(0)) / chunkXRes).toInt, 0)
        val uLY = Math.max(Math.floor((rasterEnv.getMaximum(1) - intersection.getMaxY) / chunkYRes).toInt, 0)
        val tempWidth     = Math.max(Math.ceil(intersection.getWidth / chunkXRes).toInt, 0)
        val finalWidth    = if (tempWidth + uLX > chunkWidth) chunkWidth - uLX else tempWidth
        val tempHeight    = Math.max(Math.ceil(intersection.getHeight / chunkYRes).toInt, 0)
        val finalHeight   = if (tempHeight + uLY > chunkHeight) chunkHeight - uLY else tempHeight
        val bufferedChunk = renderedImageToBufferedImage(raster.chunk)
        Some(bufferedChunk.getSubimage(uLX, uLY, finalWidth, finalHeight))
      } else None
    }
  }

  def envelopeToReferencedEnvelope(e: Envelope): ReferencedEnvelope = {
    new ReferencedEnvelope(e.getMinimum(0),
      e.getMaximum(0),
      e.getMinimum(1),
      e.getMaximum(1),
      DefaultGeographicCRS.WGS84)
  }

  def getNewImage[T: TypeTag](w: Int, h: Int, fill: Array[T],
                              imageType: Int = BufferedImage.TYPE_BYTE_GRAY): BufferedImage = {
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

    for (i <- 0 until h; j <- 0 until w) { setPixel(i, j) }
    image
  }

  case class sharedRasterParams(gg: GridGeometry2D, envelope: Envelope) {
    val width = gg.getGridRange2D.getWidth
    val height = gg.getGridRange2D.getHeight
    val resX = (envelope.getMaximum(0) - envelope.getMinimum(0)) / width
    val resY = (envelope.getMaximum(1) - envelope.getMinimum(1)) / height
    val suggestedQueryResolution = math.min(resX, resY)
  }

}

