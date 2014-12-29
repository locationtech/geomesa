package org.locationtech.geomesa.core.index

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import org.apache.accumulo.core.data.{Key, Value}
import org.apache.hadoop.io.Text
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.core._
import org.locationtech.geomesa.core.data.tables.SpatioTemporalTable
import org.locationtech.geomesa.feature.SimpleFeatureEncoder
import org.locationtech.geomesa.utils.geohash.{GeoHash, GeohashUtils}
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._

object IndexEntry {

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
}

case class IndexEntryEncoder(rowf: TextFormatter,
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

    // the index value is the encoded date/time/fid
    val indexValue = new Value(IndexValueEncoder(featureToEncode.getFeatureType).encode(featureToEncode))
    // the data value is the encoded SimpleFeature
    val dataValue = new Value(featureEncoder.encode(featureToEncode))

    // data entries are stored separately (and independently) from the index entries
    // the entries are (key, value) pairs

    val indexEntries = geohashes.map { gh =>
      formats.map(_.format(gh, dt, featureToEncode, true)) match {
        case Array(row, cf, cq) => (new Key(row, cf, cq, v), indexValue)
      }
    }
    val dataEntries = geohashes.map { gh =>
      formats.map(_.format(gh, dt, featureToEncode, false)) match {
        case Array(row, cf, cq) => (new Key(row, cf, cq, v), dataValue)
      }
    }

    (indexEntries ++ dataEntries).toList
  }

}

object IndexEntryDecoder {
  val localBuilder = new ThreadLocal[SimpleFeatureBuilder] {
    override def initialValue(): SimpleFeatureBuilder = new SimpleFeatureBuilder(indexSFT)
  }
}

import org.locationtech.geomesa.core.index.IndexEntryDecoder._

case class IndexEntryDecoder(ghDecoder: GeohashDecoder, dtDecoder: Option[DateDecoder]) {
  def decode(key: Key) = {
    val builder = localBuilder.get
    builder.reset()
    builder.addAll(List(ghDecoder.decode(key).geom, dtDecoder.map(_.decode(key))))
    builder.buildFeature("")
  }
}
