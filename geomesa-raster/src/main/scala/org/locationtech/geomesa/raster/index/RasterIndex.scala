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

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.io.Text
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data.{DATA_CQ, SimpleFeatureEncoder}
import org.locationtech.geomesa.core.index._
import org.locationtech.geomesa.utils.geohash.{GeoHash, GeohashUtils}
import org.locationtech.geomesa.utils.text.WKBUtils
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object RasterIndex  {

  val timeZone = DateTimeZone.forID("UTC")

  implicit class IndexEntrySFT(sf: SimpleFeature) {
    lazy val userData = sf.getFeatureType.getUserData
    lazy val dtgStartField = userData.getOrElse(SF_PROPERTY_START_TIME, DEFAULT_DTG_PROPERTY_NAME).asInstanceOf[String]
    lazy val dtgEndField = userData.getOrElse(SF_PROPERTY_END_TIME, DEFAULT_DTG_END_PROPERTY_NAME).asInstanceOf[String]

    lazy val sid = sf.getID
    lazy val gh: GeoHash = GeohashUtils.reconstructGeohashFromGeometry(geometry)
    def geometry = sf.getDefaultGeometry match {
      case geo: Geometry => geo
      case other =>
        throw new Exception(s"Default geometry must be Geometry: '$other' of type '${Option(other).map(_.getClass).orNull}'")
    }

    private def getTime(attr: String) = sf.getAttribute(attr).asInstanceOf[java.util.Date]
    def startTime = getTime(dtgStartField)
    def endTime   = getTime(dtgEndField)
    lazy val dt   = Option(startTime).map { d => new DateTime(d) }

    private def setTime(attr: String, time: DateTime) =
      sf.setAttribute(attr, Option(time).map(_.toDate).orNull)

    def setStartTime(time: DateTime) = setTime(dtgStartField, time)
    def setEndTime(time: DateTime)   = setTime(dtgEndField, time)
  }

  // the index value consists of the feature's:
  // 1.  ID
  // 2.  WKB-encoded geometry
  // 3.  start-date/time
  def encodeIndexValue(entry: SimpleFeature): Value = {
    val encodedId = entry.sid.getBytes
    val encodedGeom = WKBUtils.write(entry.geometry)
    val encodedDtg = entry.dt.map(dtg => ByteBuffer.allocate(8).putLong(dtg.getMillis).array()).getOrElse(Array[Byte]())

    new Value(
      ByteBuffer.allocate(4).putInt(encodedId.length).array() ++ encodedId ++
        ByteBuffer.allocate(4).putInt(encodedGeom.length).array() ++ encodedGeom ++
        encodedDtg)
  }

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

  def byteArrayToDecodedIndexValue(b: Array[Byte]): DecodedIndexValue = {
    val idLength = ByteBuffer.wrap(b, 0, 4).getInt
    val (idPortion, geomDatePortion) = b.drop(4).splitAt(idLength)
    val id = new String(idPortion)
    val geomLength = ByteBuffer.wrap(geomDatePortion, 0, 4).getInt
    if(geomLength < (geomDatePortion.length - 4)) {
      val (l,r) = geomDatePortion.drop(4).splitAt(geomLength)
      DecodedIndexValue(id, WKBUtils.read(l), Some(ByteBuffer.wrap(r).getLong))
    } else {
      DecodedIndexValue(id, WKBUtils.read(geomDatePortion.drop(4)), None)
    }
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

  def decodeIndexValue(v: Value): DecodedIndexValue = {
    val buf = v.get()
    byteArrayToDecodedIndexValue(buf)
  }

  case class DecodedIndexValue(id: String, geom: Geometry, dtgMillis: Option[Long])

  def decodeIndexCQMetadata(k: Key): DecodedCQMetadata = {
    val cqd = k.getColumnQualifierData.toArray
    byteArrayToDecodedMetadata(cqd)
  }

  case class DecodedCQMetadata(id: String, geom: Geometry, dtgMillis: Option[Long])
}

case class RasterIndexEncoder(rowf: TextFormatter,
                             cff: TextFormatter,
                             cqf: TextFormatter,
                             featureEncoder: SimpleFeatureEncoder)
  extends Logging {

  import org.locationtech.geomesa.core.index.IndexEntry._
  import org.locationtech.geomesa.utils.geohash.GeohashUtils._

  val formats = Array(rowf,cff,cqf)

  // the resolutions are valid for decomposed objects are all 5-bit boundaries
  // between 5-bits and 35-bits (inclusive)
  lazy val decomposableResolutions: ResolutionRange = new ResolutionRange(0, 35, 5)

  // the maximum number of sub-units into which a geometry may be decomposed
  lazy val maximumDecompositions: Int = 5

  def encode(featureToEncode: SimpleFeature, visibility: String = ""): List[KeyValuePair] = {

    logger.trace(s"encoding feature: $featureToEncode")

    // decompose non-point geometries into multiple index entries
    // (a point will return a single GeoHash at the maximum allowable resolution)
    val geohashes =
      decomposeGeometry(featureToEncode.geometry, maximumDecompositions, decomposableResolutions)

    logger.trace(s"decomposed ${featureToEncode.geometry} into geohashes: ${geohashes.map(_.hash).mkString(",")})}")

    val v = new Text(visibility)
    val dt = featureToEncode.dt.getOrElse(new DateTime()).withZone(timeZone)

    // remember the resulting index-entries
    val keys = geohashes.map { gh =>
      val Array(r, cf, cq) = formats.map { _.format(gh, dt, featureToEncode) }
      new Key(r, cf, cq, v)
    }
    val rowIDs = keys.map(_.getRow)
    val id = new Text(featureToEncode.sid)

    val indexValue = RasterIndex.encodeIndexValue(featureToEncode)
    val iv = new Value(indexValue)
    // the index entries are (key, FID) pairs
    val indexEntries = keys.map { k => (k, iv) }

    // the (single) data value is the encoded (serialized-to-string) SimpleFeature
    val dataValue = new Value(featureEncoder.encode(featureToEncode))

    // data entries are stored separately (and independently) from the index entries;
    // each attribute gets its own data row (though currently, we use only one attribute
    // that represents the entire, encoded feature)
    val dataEntries = rowIDs.map { rowID =>
      val key = new Key(rowID, id, DATA_CQ, v)
      (key, dataValue)
    }

    (indexEntries ++ dataEntries).toList
  }

}

object RasterIndexCQMetadataDecoder {
  val metaBuilder = new ThreadLocal[SimpleFeatureBuilder] {
    override def initialValue(): SimpleFeatureBuilder = new SimpleFeatureBuilder(indexSFT)
  }
}

import org.locationtech.geomesa.raster.index.RasterIndexCQMetadataDecoder._

case class RasterIndexCQMetadataDecoder[T <: TextExtractor](ghDecoder: GeohashDecoder, dtDecoder: Option[DateDecoder[T]]) {
  // are we grabbing the right stuff from the key?
  def decode(key: Key) = {
    val builder = metaBuilder.get
    builder.reset()
    builder.addAll(List(ghDecoder.decode(key).geom, dtDecoder.map(_.decode(key))))
    builder.buildFeature("")
  }
}

object RasterIndexDecoder {
  val localBuilder = new ThreadLocal[SimpleFeatureBuilder] {
    override def initialValue(): SimpleFeatureBuilder = new SimpleFeatureBuilder(indexSFT)
  }
}

import org.locationtech.geomesa.raster.index.RasterIndexDecoder._

case class RasterIndexDecoder[T <: TextExtractor](ghDecoder: GeohashDecoder, dtDecoder: Option[DateDecoder[T]]) {
  def decode(key: Key) = {
    val builder = localBuilder.get
    builder.reset()
    builder.addAll(List(ghDecoder.decode(key).geom, dtDecoder.map(_.decode(key))))
    builder.buildFeature("")
  }
}
