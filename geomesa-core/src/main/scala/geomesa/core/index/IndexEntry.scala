package geomesa.core.index

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import geomesa.core._
import geomesa.core.data.DATA_CQ
import geomesa.core.data.SimpleFeatureEncoder
import geomesa.utils.geohash.{GeoHash, GeohashUtils}
import geomesa.utils.geotools.SimpleFeatureTypes
import org.apache.accumulo.core.data.{Value, Key}
import org.apache.hadoop.io.Text
import org.geotools.data.DataUtilities
import org.geotools.feature.simple.SimpleFeatureBuilder
import org.joda.time.DateTime
import org.opengis.feature.simple.SimpleFeature
import scala.collection.JavaConversions._

object IndexEntry {

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

case class IndexEncoder(rowf: TextFormatter[SimpleFeature],
                        cff: TextFormatter[SimpleFeature],
                        cqf: TextFormatter[SimpleFeature],
                        featureEncoder: SimpleFeatureEncoder) 
  extends Logging {

  import GeohashUtils._
  import IndexEntry._

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

    logger.trace(s"decomposed geohashes: ${geohashes.map(_.hash).mkString(",")})}")

    val origFeatureType = featureToEncode.getType
    val origFeatureTypeSpec = SimpleFeatureTypes.encodeType(origFeatureType)
    val decompFeatureTypeSpec = origFeatureTypeSpec.replaceAll(":(Point|MultiPoint|LineString|MultiLineString|MultiPolygon)",":Geometry")
    val decompFeatureType = SimpleFeatureTypes.createType(origFeatureType.getName.toString, decompFeatureTypeSpec)
    decompFeatureType.getUserData.putAll(origFeatureType.getUserData)  // for field annotations
    logger.trace(s"decomposed feature type geometry descriptor ${decompFeatureType.getGeometryDescriptor}")

    def setDefaultGeometry(sf: SimpleFeature, geom: Geometry) =
      sf.setAttribute(decompFeatureType.getGeometryDescriptor.getName, geom)

    val entries = geohashes.map { gh =>
      val copy = DataUtilities.reType(decompFeatureType, featureToEncode, true)
      setDefaultGeometry(copy, gh)
      copy
    }

    logger.trace(s"decomposed features: ${entries.map(e => (e, e.getType.getGeometryDescriptor)).mkString(",")})}")

    val v = new Text(visibility)

    // remember the resulting index-entries
    val keys = entries.map { entry =>
      val Array(r, cf, cq) = formats.map { _.format(entry) }
      new Key(r, cf, cq, v)
    }
    val rowIDs = keys.map(_.getRow)
    val id = new Text(featureToEncode.sid)

    val indexValue = IndexSchema.encodeIndexValue(featureToEncode)
    val iv = new Value(indexValue)
    // the index entries are (key, FID) pairs
    val indexEntries = keys.map { k => (k, iv) }

    // the (single) data value is the encoded (serialized-to-string) SimpleFeature
    val dataValue = featureEncoder.encode(featureToEncode)

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

case class IndexEntryDecoder(ghDecoder: GeohashDecoder,
                             dtDecoder: Option[DateDecoder]) {

  def decode(key: Key) =
    SimpleFeatureBuilder.build(indexSFT, List(ghDecoder.decode(key).geom, dtDecoder.map(_.decode(key))), "")
}