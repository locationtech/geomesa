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

package org.locationtech.geomesa.raster.data

import java.awt.image.RenderedImage
import java.util.UUID

import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.accumulo.index.DecodedIndexValue
import org.locationtech.geomesa.raster.index.RasterEntry
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.utils.geohash.GeohashUtils

trait Raster {
  def metadata: DecodedIndexValue
  def resolution: Double
  def serializedChunk: Array[Byte]
  def chunk: RenderedImage

  def id = metadata.id

  lazy val time = new DateTime(metadata.date.map(new DateTime(_, DateTimeZone.UTC)).getOrElse(0L), DateTimeZone.forID("UTC"))

  lazy val minimumBoundingGeoHash = GeohashUtils.getClosestAcceptableGeoHash(metadata.geom.getEnvelopeInternal)

  lazy val referencedEnvelope = new ReferencedEnvelope(metadata.geom.getEnvelopeInternal, DefaultGeographicCRS.WGS84 )

  def encodeValue = RasterUtils.imageSerialize(chunk)
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

  def apply(bytes: Array[Byte]): Raster = {
    //Todo: Fix this, we should not be taking just bytes
    val byteArrays = RasterUtils.decodeByteArrays(bytes, 3)
    val metaData = RasterEntry.decodeIndexCQMetadata(byteArrays(1)) // TODO: reimplement or fix
    val resolution = RasterUtils.bytesToDouble(byteArrays(2))
    Raster(byteArrays(0), metaData, resolution)
  }

  def encodeToBytes(raster: Raster): Array[Byte] = {
    val chunkBytes = RasterUtils.imageSerialize(raster.chunk)
    val metaDataBytes = RasterEntry.encodeIndexCQMetadata(raster.metadata)  // TODO: reimplement or fix
    val resolutionBytes = RasterUtils.doubleToBytes(raster.resolution)

    RasterUtils.encodeByteArrays(List(chunkBytes, metaDataBytes, resolutionBytes))
  }
}
