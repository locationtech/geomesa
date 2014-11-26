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
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object RasterIndexEntry extends IndexHelpers {

  // the metadata CQ consists of the feature's:
  // 1.  ID
  // 2.  WKB-encoded geometry
  // 3.  start-date/time
  def encodeIndexCQMetadataKey(entry: SimpleFeature): Key = {
    val encodedId = entry.sid.getBytes
    val encodedGeom = WKBUtils.write(entry.geometry)
    val encodedDtg = entry.dt.map(dtg => ByteBuffer.allocate(8).putLong(dtg.getMillis).array()).getOrElse(Array[Byte]())
    val EMPTY_BYTES = Array.emptyByteArray
    val cqByteArray = ByteBuffer.allocate(4).putInt(encodedId.length).array() ++ encodedId ++
      ByteBuffer.allocate(4).putInt(encodedGeom.length).array() ++ encodedGeom ++
      encodedDtg

    new Key(EMPTY_BYTES, EMPTY_BYTES, cqByteArray, EMPTY_BYTES, Long.MaxValue)
  }

  def byteArrayToDecodedMetadata(b: Array[Byte]): DecodedCQMetadata = {
    // This will need to conform to some decided CQ format ie: gh(x,y) ~ metadata
    // meta data being: id, geom, and time. below time is optional, may want to eliminate this choice
    val idLength = ByteBuffer.wrap(b, 0, 4).getInt
    val (idPortion, geomDatePortion) = b.drop(4).splitAt(idLength)
    val id = new String(idPortion)
    val geomLength = ByteBuffer.wrap(geomDatePortion, 0, 4).getInt
    if(geomLength < (geomDatePortion.length - 4)) {
      val (l,r) = geomDatePortion.drop(4).splitAt(geomLength)
      DecodedCQMetadata(id, WKBUtils.read(l), Some(ByteBuffer.wrap(r).getLong))
    } else {
      DecodedCQMetadata(id, WKBUtils.read(geomDatePortion.drop(4)), None)
    }
  }

  def decodeIndexCQMetadata(k: Key): DecodedCQMetadata = {
    val cqd = k.getColumnQualifierData.toArray
    byteArrayToDecodedMetadata(cqd)
  }

  case class DecodedCQMetadata(id: String, geom: Geometry, dtgMillis: Option[Long])
}

object RasterIndexEntryCQMetadataDecoder {
  val metaBuilder = new ThreadLocal[SimpleFeatureBuilder] {
    override def initialValue(): SimpleFeatureBuilder = new SimpleFeatureBuilder(indexSFT)
  }
}

import org.locationtech.geomesa.raster.index.RasterIndexEntryCQMetadataDecoder._

case class RasterIndexEntryCQMetadataDecoder(ghDecoder: GeohashDecoder, dtDecoder: Option[DateDecoder]) {
  // are we grabbing the right stuff from the key?
  def decode(key: Key) = {
    val builder = metaBuilder.get
    builder.reset()
    builder.addAll(List(ghDecoder.decode(key).geom, dtDecoder.map(_.decode(key))))
    builder.buildFeature("")
  }
}
