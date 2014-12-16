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

package org.locationtech.geomesa.raster.feature

import java.awt.image.RenderedImage
import java.util.UUID

import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.CRS
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.core.index.DecodedIndex
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.utils.geohash.GeohashUtils

//case class Raster(id: String,
//                  name: String,
//                  chunk: RenderedImage,
//                  bbox: BoundingBox,
//                  resolution: Double,
//                  mbgh: GeoHash,
//                  unit: String,
//                  time: DateTime,
//                  dataType: Option[String],
//                  band: Option[Int]) {

// TODO: WCS: clean up this class and document
// GEOMESA-569
case class Raster(chunk: RenderedImage, metadata: DecodedIndex, resolution: Double) {
  def id = metadata.id
  def name = metadata.id
  def time = new DateTime(metadata.dtgMillis.getOrElse(0L), DateTimeZone.forID("UTC"))

  def mbgh = GeohashUtils.getMBGH(metadata.geom.getEnvelopeInternal)

  def envelope = new ReferencedEnvelope(metadata.geom.getEnvelopeInternal, CRS.decode("EPSG:4326"))

  def encodeValue = RasterUtils.imageSerialize(chunk)
}

object Raster {
  def getRasterId(rasterName: String): String =
    s"${rasterName}_${UUID.randomUUID.toString}"
}
