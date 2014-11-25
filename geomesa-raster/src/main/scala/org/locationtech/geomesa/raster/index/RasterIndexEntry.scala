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
  // 1.  Raster ID
  // 2.  WKB-encoded footprint geometry of the Raster (true envelope)
  // 3.  start-date/time
  def encodeIndexCQMetadata(uniqId: String, geometry: Geometry, dtg: Option[DateTime]) = {
    val encodedId = uniqId.getBytes
    val encodedFootprint = WKBUtils.write(geometry)
    val encodedDtg = dtg.map(d => ByteBuffer.allocate(8).putLong(d.getMillis).array()).getOrElse(Array[Byte]())
    
    val cqByteArray = ByteBuffer.allocate(4).putInt(encodedId.length).array() ++
                      encodedId ++
                      ByteBuffer.allocate(4).putInt(encodedFootprint.length).array() ++
                      encodedFootprint ++
                      encodedDtg
    cqByteArray
  }

  def decodeIndexCQMetadata(k: Key): DecodedIndex = {
    decodeIndexCQMetadata(k.getColumnQualifierData.toArray)
  }

  def decodeIndexCQMetadata(cq: Array[Byte]): DecodedIndex = {
    byteArrayToDecodedIndex(cq)
  }

}

object RasterIndexEntryCQMetadataDecoder {
  val metaBuilder = new ThreadLocal[SimpleFeatureBuilder] {
    override def initialValue(): SimpleFeatureBuilder = new SimpleFeatureBuilder(rasterIndexSFT)
  }
}

import org.locationtech.geomesa.raster.index.RasterIndexEntryCQMetadataDecoder._

case class RasterIndexEntryCQMetadataDecoder(geomDecoder: GeometryDecoder[ColumnQualifierExtractor],
                                             dtDecoder: Option[DateDecoder[ColumnQualifierExtractor]]) {
  def decode(key: Key) = {
    val builder = metaBuilder.get
    builder.reset()
    builder.addAll(List(geomDecoder.decode(key), dtDecoder.map(_.decode(key))))
    builder.buildFeature("")
  }
}
