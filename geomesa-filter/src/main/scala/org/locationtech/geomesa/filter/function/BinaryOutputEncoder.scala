/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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

package org.locationtech.geomesa.filter.function

import java.io.OutputStream
import java.util.Date

import com.typesafe.scalalogging.slf4j.Logging
import com.vividsolutions.jts.geom.{Geometry, LineString, Point}
import org.geotools.data.simple.SimpleFeatureCollection
import org.locationtech.geomesa.utils.geotools.Conversions.RichSimpleFeatureIterator
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConversions._


object BinaryOutputEncoder extends Logging {

  import org.locationtech.geomesa.filter.function.AxisOrder._

  case class ValuesToEncode(lat: Float, lon: Float, dtg: Long, track: Option[String], label: Option[String])

  def encodeFeatureCollection(
      fc: SimpleFeatureCollection,
      output: OutputStream,
      dtgField: Option[String],
      trackIdField: Option[String],
      labelField: Option[String],
      axisOrder: AxisOrder,
      sorted: Boolean) = {

    val sysTime = System.currentTimeMillis
    val isLineString = fc.getSchema.getGeometryDescriptor.getType.getBinding == classOf[LineString]

    // depending on srs requested and wfs versions, axis order can be flipped
    val getLatLon: (Point) => (Float, Float) = axisOrder match {
      case LatLon => (p) => (p.getX.toFloat, p.getY.toFloat)
      case LonLat => (p) => (p.getY.toFloat, p.getX.toFloat)
    }
    val getDtg: (SimpleFeature) => Long = dtgField match {
      case Some(dtg) =>
        (f) => Option(f.getAttribute(dtg).asInstanceOf[Date]).map(_.getTime).getOrElse(sysTime)
      case None =>
        (_) => sysTime
    }
    val getLineDtg: (SimpleFeature) => Option[Array[Long]] = dtgField match {
      case Some(dtg) =>
        (f) => Option(f.getAttribute(dtg)).map { list =>
          if (classOf[java.util.List[_]].isAssignableFrom(list.getClass) &&
              !list.asInstanceOf[java.util.List[_]].isEmpty &&
              list.asInstanceOf[java.util.List[_]].get(0).getClass == classOf[Date]) {
            list.asInstanceOf[java.util.List[Date]].map(_.getTime).toArray
          } else {
            logger.warn(s"Expected date attribute of type 'List[Date]', instead got '${list.getClass}'")
            Array.empty[Long]
          }
        }
      case None =>
        (_) => None
    }
    val getTrackId: (SimpleFeature) => Option[String] = trackIdField match {
      case Some(trackId) => (f) => getAttributeOrId(f, trackId)
      case None => (_) => None
    }
    val getLabel: (SimpleFeature) => Option[String] = labelField match {
      case Some(label) => (f) => getAttributeOrId(f, label)
      case None => (_) => None
    }

    val encode: (ValuesToEncode) => Unit = labelField match {
      case Some(label) => (v) => {
        val toEncode = ExtendedValues(v.lat, v.lon, v.dtg, v.track, v.label)
        Convert2ViewerFunction.encode(toEncode, output)
      }
      case None => (v) => {
        val toEncode = BasicValues(v.lat, v.lon, v.dtg, v.track)
        Convert2ViewerFunction.encode(toEncode, output)
      }
    }

    if (isLineString) {
      // expand the line string into individual points to encode
      val closeableIterator = new RichSimpleFeatureIterator(fc.features())
      val iter = closeableIterator.flatMap { sf =>
        val geom = sf.getDefaultGeometry.asInstanceOf[LineString]
        val indices = (0 until geom.getNumPoints)
        val points = indices.map(geom.getPointN(_)).toArray
        val dates = getLineDtg(sf).filter { d =>
          val matches = d.size == points.size
          if (!matches) {
            logger.warn(s"Mismatched geometries and dates for simple feature ${sf.getID} - " +
                "defaulting to sys date")
          }
          matches
        }.getOrElse(Array.fill(points.size)(sysTime))
        val trackId = getTrackId(sf)
        val label = getLabel(sf)

        indices.map { case i =>
          val (lat, lon) = getLatLon(points(i))
          ValuesToEncode(lat, lon, dates(i), trackId, label)
        }
      }
      if (dtgField.isDefined) {
        // have to re-sort, since lines will have a variety of dates
        iter.toList.sortBy(_.dtg).foreach(encode)
      } else {
        // no point in sorting since all will have same time anyway
        iter.foreach(encode)
      }
      closeableIterator.close() // have to close explicitly since we are flatMapping
    } else {
      val iter = new RichSimpleFeatureIterator(fc.features()).map { f =>
        val geom = f.getDefaultGeometry.asInstanceOf[Geometry].getInteriorPoint
        val (lat, lon) = getLatLon(geom)
        val dtg = getDtg(f)
        val trackId = getTrackId(f)
        val label = getLabel(f)
        ValuesToEncode(lat, lon, dtg, trackId, label)
      }
      if (dtgField.isDefined && sorted == false) {
        logger.info("No query sort detected - sorting bin output")
        iter.toList.sortBy(_.dtg).foreach(encode)
      } else {
        iter.foreach(encode)
      }
    }
    // Feature collection has already been closed by SelfClosingIterator
  }

  /**
   * Gets an optional attribute or id
   *
   * @param f
   * @param attribute
   * @return
   */
  def getAttributeOrId(f: SimpleFeature, attribute: String): Option[String] =
    if (attribute == "id") Some(f.getID) else Option(f.getAttribute(attribute)).map(_.toString)
}

object AxisOrder extends Enumeration {
  type AxisOrder = Value
  val LatLon, LonLat = Value
}
