/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.locationtech.geomesa.plugin.wfs.output

import java.io.{BufferedOutputStream, OutputStream}
import java.util.Date
import javax.xml.namespace.QName

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.Geometry
import net.opengis.wfs.{GetFeatureType => GetFeatureTypeV1, QueryType => QueryTypeV1}
import net.opengis.wfs20.{GetFeatureType => GetFeatureTypeV2, QueryType => QueryTypeV2}
import org.geoserver.config.GeoServer
import org.geoserver.ows.Response
import org.geoserver.platform.Operation
import org.geoserver.wfs.WFSGetFeatureOutputFormat
import org.geoserver.wfs.request.{FeatureCollectionResponse, GetFeatureRequest}
import org.geotools.data.DataStore
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.util.Version
import org.locationtech.geomesa.filter.function.{BasicValues, Convert2ViewerFunction, ExtendedValues}
import org.locationtech.geomesa.utils.geotools.Conversions.toRichSimpleFeatureIterator
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConverters._

/**
 * Output format for wfs requests that encodes features into a binary format.
 * To trigger, use outputFormat=application/vnd.binary-viewer in your wfs request
 *
 * Required flags:
 * format_options=dtg:<dtg_attribute_name>
 *
 * Optional flags:
 * format_options=dtg:dtg;trackId:<track_attribute_name>;label:<label_attribute_name>
 *
 * Other useful wfs options:
 * sortBy=dtg
 * propertyName=dtg,geom,tweet_id
 *
 * @param gs
 */
class BinaryViewerOutputFormat(gs: GeoServer)
    extends WFSGetFeatureOutputFormat(gs, Set("bin", BinaryViewerOutputFormat.MIME_TYPE).asJava) {

  import org.locationtech.geomesa.core.index.getDtgFieldName
  import org.locationtech.geomesa.plugin.wfs.output.AxisOrder._
  import org.locationtech.geomesa.plugin.wfs.output.BinaryViewerOutputFormat._

  override def getMimeType(value: AnyRef, operation: Operation) = MIME_TYPE

  override def getPreferredDisposition(value: AnyRef, operation: Operation) = Response.DISPOSITION_INLINE

  override def getAttachmentFileName(value: AnyRef, operation: Operation) = {
    val gfr = GetFeatureRequest.adapt(operation.getParameters()(0))
    val name = Option(gfr.getHandle).getOrElse(gfr.getQueries.get(0).getTypeNames.get(0).getLocalPart)
    // if they have requested a label, then it will be 24 byte encoding (assuming the field exists...)
    val size = if (gfr.getFormatOptions.containsKey(LABEL_FIELD)) "24" else "16"
    s"${name}.${FILE_EXTENSION}$size"
  }

  override def write(featureCollections: FeatureCollectionResponse,
                     output: OutputStream,
                     getFeature: Operation): Unit = {

    // format_options flags for customizing the request
    val request = GetFeatureRequest.adapt(getFeature.getParameters()(0))
    val trackIdField = Option(request.getFormatOptions.get(TRACK_ID_FIELD).asInstanceOf[String])
    val labelField = Option(request.getFormatOptions.get(LABEL_FIELD).asInstanceOf[String])
    // check for explicit dtg field in request, or use schema default dtg
    val dtgField = Option(request.getFormatOptions.get(DATE_FIELD).asInstanceOf[String])
        .orElse(getDateField(getFeature))
    val sysTime = System.currentTimeMillis()

    // depending on srs requested and wfs versions, axis order can be flipped
    val axisOrder = checkAxisOrder(getFeature)

    val bos = new BufferedOutputStream(output)

    featureCollections.getFeatures.asScala.foreach { fc =>
      fc.features().asInstanceOf[SimpleFeatureIterator].foreach { f =>
        val geom = f.getDefaultGeometry.asInstanceOf[Geometry].getInteriorPoint
        val (lat, lon) = axisOrder match {
          case LatLon => (geom.getX, geom.getY)
          case LonLat => (geom.getY, geom.getX)
        }
        val dtg = dtgField.map(f.getAttribute(_))
            .filter(_.isInstanceOf[Date])
            .map(_.asInstanceOf[Date].getTime)
            .getOrElse(sysTime)
        val trackId = trackIdField.flatMap(getAttributeOrId(f, _))
        val values = labelField match {
          case Some(label) => ExtendedValues(lat.toFloat, lon.toFloat, dtg, trackId, getAttributeOrId(f, label))
          case None        => BasicValues(lat.toFloat, lon.toFloat, dtg, trackId)
        }
        bos.write(Convert2ViewerFunction.encode(values))
      }
      // implicit RichSimpleFeatureIterator calls close on the feature collection for us
      bos.flush()
    }
    // none of the implementations in geoserver call 'close' on the output stream
  }

  /**
   * Gets an optional attribute or id
   *
   * @param f
   * @param attribute
   * @return
   */
  private def getAttributeOrId(f: SimpleFeature, attribute: String): Option[String] =
    if (attribute == "id") Some(f.getID) else Option(f.getAttribute(attribute)).map(_.toString)

  /**
   * Try to pull out the default date field from the SimpleFeatureType associated with this request
   *
   * @param getFeature
   * @return
   */
  private def getDateField(getFeature: Operation): Option[String] =
    for {
      tn        <- getTypeName(getFeature)
      name      =  tn.getLocalPart
      layer     <- Option(gs.getCatalog.getLayerByName(name))
      storeId   =  layer.getResource.getStore.getId
      dataStore =  gs.getCatalog.getDataStore(storeId).getDataStore(null).asInstanceOf[DataStore]
      schema    <- Option(dataStore.getSchema(name))
      dtgField  <- getDtgFieldName(schema)
    } yield dtgField

}

object BinaryViewerOutputFormat extends Logging {

  import org.locationtech.geomesa.plugin.wfs.output.AxisOrder._

  val MIME_TYPE = "application/vnd.binary-viewer"
  val FILE_EXTENSION = "bin"
  val TRACK_ID_FIELD = "TRACKID"
  val LABEL_FIELD = "LABEL"
  val DATE_FIELD = "DTG"

  // constants used to determine axis order from geoserver
  val wfsVersion1 = new Version("1.0.0")
  val srsVersionOnePrefix = "http://www.opengis.net/gml/srs/epsg.xml#"
  val srsVersionOnePlusPrefix = "urn:x-ogc:def:crs:epsg:"
  val srsNonStandardPrefix = "epsg:"

  /**
   * Determines the order of lat/lon in simple features returned by this request.
   *
   * See http://docs.geoserver.org/2.5.x/en/user/services/wfs/basics.html#axis-ordering for details
   * on how geoserver handles axis order.
   *
   * @param getFeature
   * @return
   */
  def checkAxisOrder(getFeature: Operation): AxisOrder =
    getSrs(getFeature) match {
      // if an explicit SRS is requested, that takes priority
      // SRS format associated with WFS 1.1.0 and 2.0.0 - lat is first
      case Some(srs) if srs.toLowerCase.startsWith(srsVersionOnePlusPrefix) => LatLon
      // SRS format associated with WFS 1.0.0 - lon is first
      case Some(srs) if srs.toLowerCase.startsWith(srsVersionOnePrefix) => LonLat
      // non-standard SRS format - geoserver puts lon first
      case Some(srs) if srs.toLowerCase.startsWith(srsNonStandardPrefix) => LonLat
      case Some(srs) =>
        val valid = s"${srsVersionOnePrefix}xxxx, ${srsVersionOnePlusPrefix}xxxx, ${srsNonStandardPrefix}xxxx"
        throw new IllegalArgumentException(s"Invalid SRS format: '$srs'. Valid options are: $valid")
      // if no explicit SRS: wfs 1.0.0 stores x = lon y = lat, anything greater stores x = lat y = lon
      case None => if (getFeature.getService.getVersion.compareTo(wfsVersion1) > 0) LatLon else LonLat
    }

  def getTypeName(getFeature: Operation): Option[QName] = {
    val typeNamesV2 = getFeatureTypeV2(getFeature)
        .flatMap(getQueryType)
        .map(_.getTypeNames.asScala)
        .getOrElse(Seq.empty)
    val typeNamesV1 = getFeatureTypeV1(getFeature)
        .flatMap(getQueryType)
        .map(_.getTypeName.asScala)
        .getOrElse(Seq.empty)
    val typeNames = typeNamesV2 ++ typeNamesV1
    if (typeNames.size > 1) {
      logger.warn(s"Multiple TypeNames detected in binary format request (using first): $typeNames")
    }
    typeNames.headOption.map(_.asInstanceOf[QName])
  }

  /**
   * Function to pull requested SRS out of a WFS request
   *
   * @param getFeature
   * @return
   */
  def getSrs(getFeature: Operation): Option[String] =
    getFeatureTypeV2(getFeature).flatMap(getSrs)
        .orElse(getFeatureTypeV1(getFeature).flatMap(getSrs))

  /**
   * Function to pull requested SRS out of WFS 1.0.0/1.1.0 request
   *
   * @param getFeatureType
   * @return
   */
  def getSrs(getFeatureType: GetFeatureTypeV1): Option[String] =
    getQueryType(getFeatureType).flatMap(qt => Option(qt.getSrsName)).map(_.toString)

  /**
   * Function to pull requested SRS out of WFS 2 request
   *
   * @param getFeatureType
   * @return
   */
  def getSrs(getFeatureType: GetFeatureTypeV2): Option[String] =
    getQueryType(getFeatureType).flatMap(qt => Option(qt.getSrsName)).map(_.toString)

  /**
   *
   * @param getFeature
   * @return
   */
  def getFeatureTypeV2(getFeature: Operation): Option[GetFeatureTypeV2] =
    getFeature.getParameters.find(_.isInstanceOf[GetFeatureTypeV2])
        .map(_.asInstanceOf[GetFeatureTypeV2])

  /**
   *
   * @param getFeature
   * @return
   */
  def getFeatureTypeV1(getFeature: Operation): Option[GetFeatureTypeV1] =
    getFeature.getParameters.find(_.isInstanceOf[GetFeatureTypeV1])
        .map(_.asInstanceOf[GetFeatureTypeV1])

  /**
   * Pull out query object from request
   *
   * @param getFeatureType
   * @return
   */
  def getQueryType(getFeatureType: GetFeatureTypeV1): Option[QueryTypeV1] =
    getFeatureType.getQuery.iterator().asScala
        .find(_.isInstanceOf[QueryTypeV1])
        .map(_.asInstanceOf[QueryTypeV1])

  /**
   * Pull out query object from request
   *
   * @param getFeatureType
   * @return
   */
  def getQueryType(getFeatureType: GetFeatureTypeV2): Option[QueryTypeV2] =
    getFeatureType.getAbstractQueryExpressionGroup.iterator().asScala
        .find(_.getValue.isInstanceOf[QueryTypeV2])
        .map(_.getValue.asInstanceOf[QueryTypeV2])
}

object AxisOrder extends Enumeration {
  type AxisOrder = Value
  val LatLon, LonLat = Value
}
