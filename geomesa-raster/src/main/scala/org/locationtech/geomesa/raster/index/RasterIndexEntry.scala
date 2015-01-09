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

import java.awt.image.RenderedImage
import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.nio.ByteBuffer

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.io.Text
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.joda.time.DateTime
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.raster
import org.locationtech.geomesa.raster.data.Raster
import org.locationtech.geomesa.utils.text.WKBUtils

import scala.collection.JavaConversions._

object RasterIndexEntry extends IndexHelpers {

  // the metadata CQ consists of the raster feature's:
  // 1.  Raster ID
  // 2.  WKB-encoded footprint geometry of the Raster (true envelope)
  // 3.  start-date/time
  def encodeIndexCQMetadata(uniqId: String, geometry: Geometry, dtg: Option[DateTime]) = {
    val encodedId   = uniqId.getBytes
    val encodedGeom = WKBUtils.write(geometry)
    val encodedDtg  = dtg.map { d => ByteBuffer.allocate(8).putLong(d.getMillis).array() } .getOrElse(Array[Byte]())
    
    val cqByteArray = ByteBuffer.allocate(4).putInt(encodedId.length).array() ++
                      encodedId ++
                      ByteBuffer.allocate(4).putInt(encodedGeom.length).array() ++
                      encodedGeom ++
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
    override def initialValue(): SimpleFeatureBuilder = new SimpleFeatureBuilder(indexSFT)
  }
}

import org.locationtech.geomesa.raster.index.RasterIndexEntryCQMetadataDecoder._

case class RasterIndexEntryCQMetadataDecoder(geomDecoder: GeometryDecoder,
                                             dtDecoder: Option[DateDecoder[ColumnQualifierExtractor]]) {
  def decode(key: Key) = {
    val builder = metaBuilder.get
    builder.reset()
    builder.addAll(List(geomDecoder.decode(key), dtDecoder.map { _.decode(key) } ))
    builder.buildFeature("")
  }
}

case class RasterIndexEntryEncoder(rowf: TextFormatter,
                                   cff: TextFormatter,
                                   cqf: TextFormatter)
  extends Logging {

  def encode(raster: Raster, visibility: String = ""): KeyValuePair = {

    logger.trace(s"encoding raster: $raster")
    val vis = new Text(visibility)
    val key = new Key(getRow(raster), getCF(raster), getCQ(raster), vis)
    val encodedRaster = encodeValue(raster)

    (key, encodedRaster)
  }

  private def getRow(ras: Raster) = {
    val resEncoder = DoubleTextFormatter(ras.resolution)
    val geohash = ras.minimumBoundingGeoHash.map ( _.hash ) .getOrElse("")
    new Text(s"~${resEncoder.fmtdStr}~$geohash")
  }

  //TODO: WCS: add band value to Raster and insert it into the CF here
  // GEOMESA-561
  private def getCF(raster: Raster): Text = new Text("")
  
  private def getCQ(raster: Raster): Text = {
    new Text(RasterIndexEntry.encodeIndexCQMetadata(raster.id, raster.metadata.geom, Some(raster.time)))
  }

  private def encodeValue(raster: Raster): Value =
    new Value(raster.serializedChunk)

}

object RasterIndexEntryDecoder {
  def rasterImageDeserialize(imageBytes: Array[Byte]): RenderedImage = {
    val in: ObjectInputStream = new ObjectInputStream(new ByteArrayInputStream(imageBytes))
    var read: RenderedImage = null
    try {
      read = in.readObject().asInstanceOf[RenderedImage]
    } finally {
      in.close()
    }
    read
  }
}

import org.locationtech.geomesa.raster.index.RasterIndexEntryDecoder._

case class RasterIndexEntryDecoder() {
  // this should not really need any parameters for the case class, it should simply
  // construct a Raster from a Key (don't we need the value for this....)
  // maybe this is not needed at all?
  def decode(entry: KeyValuePair) = {
    val renderedImage: RenderedImage = rasterImageDeserialize(entry._2.get)
    val metadata: DecodedIndex = RasterIndexEntry.decodeIndexCQMetadata(entry._1)
    //TODO: move this to RasterIndexSchema
    val res = raster.lexiDecodeStringToDouble(new String(entry._1.getRowData.toArray).split("~")
      .toList.get(1))
    Raster(renderedImage, metadata, res)
  }
}
