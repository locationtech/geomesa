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

import com.vividsolutions.jts.geom.Geometry
import org.geoserver.config.GeoServer
import org.geoserver.ows.Response
import org.geoserver.platform.Operation
import org.geoserver.wfs.WFSGetFeatureOutputFormat
import org.geoserver.wfs.request.{FeatureCollectionResponse, GetFeatureRequest}
import org.geotools.data.simple.SimpleFeatureIterator
import org.geotools.util.Version
import org.locationtech.geomesa.filter.function.{Convert2ViewerFunction, EncodedValues}
import org.locationtech.geomesa.plugin.wfs.output.BinaryViewerOutputFormat._
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
class BinaryViewerOutputFormat(gs: GeoServer) extends WFSGetFeatureOutputFormat(gs, Set("bin", MIME_TYPE).asJava) {

  val wfsVersion1 = new Version("1.0.0")

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
    val gfr = GetFeatureRequest.adapt(getFeature.getParameters()(0))
    val trackIdField = Option(gfr.getFormatOptions.get(TRACK_ID_FIELD).asInstanceOf[String])
    val labelField = Option(gfr.getFormatOptions.get(LABEL_FIELD).asInstanceOf[String])
    // dtg isn't present in the feature or type hints, so we have to pass it in
    val dtgField = Option(gfr.getFormatOptions.get(DATE_FIELD).asInstanceOf[String])

    // wfs 1.0.0 stores x = lon y = lat, anything greater stores x = lat y = lon
    val xIsLat = getFeature.getService.getVersion.compareTo(wfsVersion1) > 0

    val bos = new BufferedOutputStream(output)

    featureCollections.getFeatures.asScala.foreach { fc =>
      fc.features().asInstanceOf[SimpleFeatureIterator].foreach { f =>
        val geom = f.getDefaultGeometry.asInstanceOf[Geometry].getInteriorPoint
        val (lat, lon) = if (xIsLat) (geom.getX, geom.getY) else (geom.getY, geom.getX)
        val dtg = dtgField.map(f.getAttribute(_))
            .filter(_.isInstanceOf[Date])
            .map(_.asInstanceOf[Date].getTime)
            .getOrElse(System.currentTimeMillis())
        val label = labelField.flatMap(getAttributeOrId(f, _))
        val trackId = trackIdField.flatMap(getAttributeOrId(f, _))
        val values = EncodedValues(lat.toFloat, lon.toFloat, dtg, trackId, label)
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

}

object BinaryViewerOutputFormat {
  val MIME_TYPE = "application/vnd.binary-viewer"
  val FILE_EXTENSION = "bin"
  val TRACK_ID_FIELD = "TRACKID"
  val LABEL_FIELD = "LABEL"
  val DATE_FIELD = "DTG"
}