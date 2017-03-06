/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.filter.function

import java.io.OutputStream
import java.util.{Collections, Date}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, LineString, Point}
import org.geotools.data.simple.SimpleFeatureCollection
import org.locationtech.geomesa.utils.geotools.AttributeSpec.ListAttributeSpec
import org.locationtech.geomesa.utils.geotools.{SimpleFeatureSpecParser, SimpleFeatureTypes}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._


object BinaryOutputEncoder extends LazyLogging {

  import org.locationtech.geomesa.filter.function.AxisOrder._
  import org.locationtech.geomesa.utils.geotools.Conversions._
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  sealed trait GeometrySelector

  case class GeometryAttribute(geom: String, axisOrder: AxisOrder = LatLon) extends GeometrySelector
  case class LatLonAttributes(lat: String, lon: String) extends GeometrySelector

  case class ValuesToEncode(lat: Float, lon: Float, dtg: Long, track: Int, label: Long)

  case class EncodingOptions(geomField: Option[GeometrySelector],
                             dtgField: Option[String],
                             trackIdField: Option[String],
                             labelField: Option[String] = None)

  val CollectionEncodingOptions = Collections.synchronizedMap(new java.util.HashMap[String, EncodingOptions])

  implicit val ordering = new Ordering[ValuesToEncode]() {
    override def compare(x: ValuesToEncode, y: ValuesToEncode): Int = x.dtg.compareTo(y.dtg)
  }

  /**
    * Encodes a feature collection to bin format. Features are written to the output stream.
    *
    * @param fc feature collection to encode
    * @param output output stream to write to
    * @param options fields, etc to encode
    * @param sort sort results before returning
    */
  def encodeFeatureCollection(
      fc: SimpleFeatureCollection,
      output: OutputStream,
      options: EncodingOptions,
      sort: Boolean = false): Unit = {

    val sft = fc.getSchema

    val geomField = options.geomField.getOrElse(GeometryAttribute(sft.getGeometryDescriptor.getLocalName))
    val dtgField = options.dtgField.orElse(sft.getDtgField).orNull

    val (isPoint, isLineString) = geomField match {
      case _: LatLonAttributes        => (true, false)
      case GeometryAttribute(geom, _) =>
        val binding = Option(sft.getDescriptor(geom)).map(_.getType.getBinding).orNull
        (binding == classOf[Point], binding == classOf[LineString])
    }

    // validate our input data
    validateGeometryAttribute(sft, geomField)
    validateDateAttribute(sft, isLineString, dtgField)
    validateLabels(sft, options.trackIdField, options.labelField)

    val dtgIndex = sft.indexOf(dtgField)

    // get date function
    val getDtg: (SimpleFeature) => Long = (f) => {
      val date = f.getAttribute(dtgIndex).asInstanceOf[Date]
      if (date == null) { 0L } else { date.getTime }
    }
    // for line strings, we need an array of dates corresponding to the points in the line
    val getLineDtg: (SimpleFeature) => Array[Long] = (f) => {
      val dates = f.getAttribute(dtgIndex).asInstanceOf[java.util.List[Date]]
      if (dates == null) { Array.empty } else { dates.map(_.getTime).toArray }
    }

    // get lat/lon as floats
    val getLatLon: (SimpleFeature) => (Float, Float) = geomField match {
      case LatLonAttributes(lat, lon) =>
        // if requested, export arbitrary fields as lat/lon
        val latIndex = sft.indexOf(lat)
        val lonIndex = sft.indexOf(lon)
        val binding  = sft.getType(latIndex).getBinding
        if (binding == classOf[java.lang.Float]) {
          (f) => (f.getAttribute(latIndex).asInstanceOf[Float], f.getAttribute(lonIndex).asInstanceOf[Float])
        } else if (binding == classOf[java.lang.Double]) {
          (f) => (f.getAttribute(latIndex).asInstanceOf[Double].toFloat, f.getAttribute(lonIndex).asInstanceOf[Double].toFloat)
        } else {
          (f) => (f.getAttribute(latIndex).toString.toFloat, f.getAttribute(lonIndex).toString.toFloat)
        }

      case GeometryAttribute(geom, axisOrder) =>
        val geomIndex = sft.indexOf(geom)
        // depending on srs requested and wfs versions, axis order can be flipped
        (axisOrder, isPoint) match {
          case (LatLon, true)  => (f) => pointToXY(f.getAttribute(geomIndex).asInstanceOf[Point])
          case (LatLon, false) => (f) => pointToXY(f.getAttribute(geomIndex).asInstanceOf[Geometry].safeCentroid())
          case (LonLat, true)  => (f) => pointToXY(f.getAttribute(geomIndex).asInstanceOf[Point]).swap
          case (LonLat, false) => (f) => pointToXY(f.getAttribute(geomIndex).asInstanceOf[Geometry].safeCentroid()).swap
        }
    }

    // for linestrings, we return each point - use an array so we get constant-time lookup
    // depending on srs requested and wfs versions, axis order can be flipped
    lazy val getLineLatLon: (SimpleFeature) => Array[(Float, Float)] = geomField match {
      case GeometryAttribute(geom, LatLon) =>
        val geomIndex = sft.indexOf(geom)
        (f) => {
          val geom = f.getAttribute(geomIndex).asInstanceOf[LineString]
          val points = (0 until geom.getNumPoints).map(geom.getPointN)
          points.map(pointToXY).toArray
        }

      case GeometryAttribute(geom, LonLat) =>
        val geomIndex = sft.indexOf(geom)
        (f) => {
          val geom = f.getAttribute(geomIndex).asInstanceOf[LineString]
          val points = (0 until geom.getNumPoints).map(geom.getPointN)
          points.map(pointToXY(_).swap).toArray
        }
    }

    // gets the track id from a feature
    val getTrackId: (SimpleFeature) => Int = options.trackIdField match {
      case Some(trackId) if trackId == "id" => (f) => f.getID.hashCode

      case Some(trackId) =>
        val trackIndex  = sft.indexOf(trackId)
        (f) => {
          val track = f.getAttribute(trackIndex)
          if (track == null) { 0 } else { track.hashCode }
        }

      case None => (_) => 0
    }

    // gets the label from a feature
    val getLabel: (SimpleFeature) => Long = options.labelField match {
      case Some(label) =>
        val labelIndex = sft.indexOf(label)
        (f) => {
          val label = f.getAttribute(labelIndex)
          if (label == null) { 0L } else { Convert2ViewerFunction.convertToLabel(label.toString) }
        }

      case None => (_) => 0L
    }

    // encodes the values in either 16 or 24 bytes
    val encode: (ValuesToEncode) => Unit = options.labelField match {
      case Some(_) => (v) => {
        val toEncode = ExtendedValues(v.lat, v.lon, v.dtg, v.track, v.label)
        Convert2ViewerFunction.encode(toEncode, output)
      }
      case None => (v) => {
        val toEncode = BasicValues(v.lat, v.lon, v.dtg, v.track)
        Convert2ViewerFunction.encode(toEncode, output)
      }
    }

    val iter = if (isLineString) {
      // expand the line string into individual points to encode
      fc.features().flatMap { sf =>
        val points = getLineLatLon(sf)
        val dates = getLineDtg(sf)
        if (points.length != dates.length) {
          logger.warn(s"Mismatched geometries and dates for simple feature ${sf.getID} - skipping")
          Seq.empty
        } else {
          val trackId = getTrackId(sf)
          val label = getLabel(sf)
          points.indices.map { i =>
            val (lat, lon) = points(i)
            ValuesToEncode(lat, lon, dates(i), trackId, label)
          }
        }
      }
    } else {
      fc.features().map { sf =>
        val (lat, lon) = getLatLon(sf)
        val dtg = getDtg(sf)
        val trackId = getTrackId(sf)
        val label = getLabel(sf)
        ValuesToEncode(lat, lon, dtg, trackId, label)
      }
    }
    if (sort) {
      iter.toList.sorted.foreach(encode)
    } else {
      iter.foreach(encode)
    }
    // Feature collection has already been closed by SelfClosingIterator
  }

  private def pointToXY(p: Point) = (p.getX.toFloat, p.getY.toFloat)

  private def validateGeometryAttribute(sft: SimpleFeatureType, selector: GeometrySelector) = {
    val ok = selector match {
      case LatLonAttributes(lat, lon) => sft.indexOf(lat) != -1 && sft.indexOf(lon) != -1
      case GeometryAttribute(geom, _) => sft.indexOf(geom) != -1
    }

    if (!ok) {
      throw new RuntimeException(s"Invalid geometry fields $selector requested for feature type $sft")
    }
  }

  private def validateDateAttribute(sft: SimpleFeatureType, isLineString: Boolean, dtgField: String) = {
    val sftAttributes = SimpleFeatureSpecParser.parse(SimpleFeatureTypes.encodeType(sft)).attributes
    val dateAttribute = sftAttributes.find(_.name == dtgField)
    val ok = dateAttribute.exists { spec =>
      if (isLineString) {
        spec.isInstanceOf[ListAttributeSpec] && spec.asInstanceOf[ListAttributeSpec].subClass == classOf[Date]
      } else {
        spec.clazz == classOf[Date]
      }
    }
    if (!ok) {
      throw new RuntimeException(s"Invalid date field $dtgField requested for feature type $sft")
    }
  }

  private def validateLabels(sft: SimpleFeatureType, trackId: Option[String], label: Option[String]) = {
    val ok = Seq(trackId, label).flatten.forall(attr => attr == "id" || sft.indexOf(attr) != -1)
    if (!ok) {
      throw new RuntimeException(s"Invalid fields $trackId and/or $label requested for feature type $sft")
    }
  }
}

object AxisOrder extends Enumeration {
  type AxisOrder = Value
  val LatLon, LonLat = Value
}
