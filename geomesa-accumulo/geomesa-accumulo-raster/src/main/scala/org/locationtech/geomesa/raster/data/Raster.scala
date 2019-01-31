/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.data

import java.awt.image.RenderedImage
import java.util.{Date, UUID}

import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.raster.index.RasterEntryDecoder.DecodedIndexValue
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.utils.geohash.GeohashUtils

trait Raster {
  def metadata: DecodedIndexValue
  def resolution: Double
  def serializedChunk: Array[Byte]
  def chunk: RenderedImage
  def id = metadata.id

  lazy val time: Date = metadata.date.getOrElse(new Date(0L))

  // The minimum bounding geohash is the 32bit representable GeoHash that
  // most closely matches the extent of the Raster. The permits fuzzy indexing of Rasters.
  lazy val minimumBoundingGeoHash = GeohashUtils.getClosestAcceptableGeoHash(metadata.geom.getEnvelopeInternal)

  lazy val referencedEnvelope = new ReferencedEnvelope(metadata.geom.getEnvelopeInternal, DefaultGeographicCRS.WGS84)

  def encodeValue = RasterUtils.imageSerialize(chunk)

  override def toString = id
}

case class RenderedImageRaster(chunk: RenderedImage, metadata: DecodedIndexValue, resolution: Double) extends Raster {
  lazy val serializedChunk: Array[Byte] = RasterUtils.imageSerialize(chunk)
}

case class ArrayBytesRaster(serializedChunk: Array[Byte], metadata: DecodedIndexValue, resolution: Double) extends Raster {
  lazy val chunk: RenderedImage = RasterUtils.imageDeserialize(serializedChunk)
}

object Raster {
  def apply(chunk: RenderedImage, metadata: DecodedIndexValue, resolution: Double) =
    RenderedImageRaster(chunk, metadata, resolution)

  def apply(bytes: Array[Byte], metadata: DecodedIndexValue, resolution: Double) =
    ArrayBytesRaster(bytes, metadata, resolution)

  def getRasterId(rasterName: String): String =
    s"${rasterName}_${UUID.randomUUID.toString}"

}
