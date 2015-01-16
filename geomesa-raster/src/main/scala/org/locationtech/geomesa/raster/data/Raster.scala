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
import java.nio.ByteBuffer
import java.util.UUID

import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.CRS
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.core.index.{DecodedIndex, IndexEntry}
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.utils.geohash.GeohashUtils

case class Raster(chunk: RenderedImage, metadata: DecodedIndex, resolution: Double) {

  def id = metadata.id

  lazy val time = new DateTime(metadata.dtgMillis.getOrElse(0L), DateTimeZone.forID("UTC"))

  lazy val minimumBoundingGeoHash = GeohashUtils.getClosestAcceptableGeoHash(metadata.geom.getEnvelopeInternal)

  lazy val referencedEnvelope = new ReferencedEnvelope(metadata.geom.getEnvelopeInternal, CRS.decode("EPSG:4326"))

  lazy val serializedChunk = RasterUtils.imageSerialize(chunk)

  def encodeValue = RasterUtils.imageSerialize(chunk)

  def encodeToBytes(): Array[Byte] = {
    val chunkBytes = RasterUtils.imageSerialize(chunk)
    val metaDataBytes = metadata.toBytes

    ByteBuffer.allocate(4).putInt(chunkBytes.length).array() ++ chunkBytes ++
      ByteBuffer.allocate(4).putInt(metaDataBytes.length).array() ++ metaDataBytes ++
      ByteBuffer.allocate(8).putDouble(resolution).array()
  }
}

object Raster {
  def getRasterId(rasterName: String): String =
    s"${rasterName}_${UUID.randomUUID.toString}"

  def apply(bytes: Array[Byte]): Raster = {
    val chunkLength = ByteBuffer.wrap(bytes, 0, 4).getInt
    val (chunkPortion, metaResPortion) = bytes.drop(4).splitAt(chunkLength)
    val chunk = RasterUtils.imageDeserialize(chunkPortion)
    val metaLength = ByteBuffer.wrap(metaResPortion, 0, 4).getInt
    val (metaPortion, resPortion) = metaResPortion.drop(4).splitAt(metaLength)
    val metaData = IndexEntry.byteArrayToDecodedIndex(metaPortion)
    val resolution = ByteBuffer.wrap(resPortion).getDouble

    Raster(chunk, metaData, resolution)
  }
}
