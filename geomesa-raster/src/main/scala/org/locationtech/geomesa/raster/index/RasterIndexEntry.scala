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

package org.locationtech.geomesa.raster.index

import java.nio.ByteBuffer

import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.Key
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.joda.time.DateTime
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.utils.text.WKBUtils

import scala.collection.JavaConversions._

object RasterIndexEntry extends IndexHelpers {

  // the metadata CQ consists of the raster feature's:
  // 1.  Minimum Bounding GeoHash of the Raster
  // 2.  Raster ID
  // 3.  WKB-encoded footprint geometry of the Raster (true envelope)
  // 4.  start-date/time
  def encodeIndexCQMetadata(mbGH: Geometry, uniqId: String, geometry: Geometry, dtg: DateTime) = {
    val encodedId = uniqId.getBytes
    val encodedGH = WKBUtils.write(mbGH)
    val encodedFootprint = WKBUtils.write(geometry)
    val encodedDtg = ByteBuffer.allocate(8).putLong(dtg.getMillis).array()
    
    val cqByteArray = ByteBuffer.allocate(4).putInt(encodedGH.length).array() ++
                      encodedGH ++
                      ByteBuffer.allocate(4).putInt(encodedId.length).array() ++
                      encodedId ++
                      ByteBuffer.allocate(4).putInt(encodedFootprint.length).array() ++
                      encodedFootprint ++
                      encodedDtg
    cqByteArray
  }

  def byteArrayToDecodedCQMetadata(b: Array[Byte]): DecodedCQMetadata = {
    // This will need to conform to some decided CQ format ie: gh(x,y) ~ metadata
    // meta data being: id, geom, and time.
    val ghLength = ByteBuffer.wrap(b, 0, 4).getInt
    val (ghPortion, idGeomDatePortion) = b.drop(4).splitAt(ghLength)
    val mbgh = WKBUtils.read(ghPortion)
    val idLength = ByteBuffer.wrap(idGeomDatePortion, 0, 4).getInt
    val (idPortion, geomDatePortion) = b.drop(4).splitAt(idLength)
    val id = new String(idPortion)
    val geomLength = ByteBuffer.wrap(geomDatePortion, 0, 4).getInt
    val (l,r) = geomDatePortion.drop(4).splitAt(geomLength)
    DecodedCQMetadata(id, mbgh, WKBUtils.read(l), ByteBuffer.wrap(r).getLong)
  }

  def decodeIndexCQMetadata(k: Key): DecodedCQMetadata = {
    val cqd = k.getColumnQualifierData.toArray
    byteArrayToDecodedCQMetadata(cqd)
  }

  case class DecodedCQMetadata(id: String, mbgh: Geometry, footprint: Geometry, dtgMillis: Long)
}

object RasterIndexEntryCQMetadataDecoder {
  val metaBuilder = new ThreadLocal[SimpleFeatureBuilder] {
    override def initialValue(): SimpleFeatureBuilder = new SimpleFeatureBuilder(rasterIndexSFT)
  }
}

import org.locationtech.geomesa.raster.index.RasterIndexEntryCQMetadataDecoder._

case class RasterIndexEntryCQMetadataDecoder(ghDecoder: GeohashDecoder, dtDecoder: Option[DateDecoder]) {
  def decode(key: Key) = {
    val cq = key.getColumnQualifier // we are not using this, todo: make ghDecoder and dtDecoder take the CQ!
    val builder = metaBuilder.get
    builder.reset()
    builder.addAll(List(ghDecoder.decode(key).geom, dtDecoder.map(_.decode(key))))
    builder.buildFeature("")
  }
}
