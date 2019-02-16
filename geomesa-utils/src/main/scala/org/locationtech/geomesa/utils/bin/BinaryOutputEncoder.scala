/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.bin

import java.io.{ByteArrayOutputStream, OutputStream}
import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.utils.bin.BinaryEncodeCallback.{ByteArrayCallback, ByteStreamCallback}
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.ToValues
import org.locationtech.geomesa.utils.collection.CloseableIterator
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpec.ListAttributeSpec
import org.locationtech.geomesa.utils.geotools.sft.SimpleFeatureSpecParser
import org.locationtech.jts.geom.{Geometry, LineString, Point}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.JavaConversions._

class BinaryOutputEncoder private (toValues: ToValues) {

  def encode(f: SimpleFeature): Array[Byte] = {
    toValues(f, ByteArrayCallback)
    ByteArrayCallback.result
  }

  def encode(f: SimpleFeature, callback: BinaryOutputCallback): Unit = toValues(f, callback)

  def encode(f: CloseableIterator[SimpleFeature], os: OutputStream, sort: Boolean = false): Long = {
    if (sort) {
      val byteStream = new ByteArrayOutputStream
      val callback = new ByteStreamCallback(byteStream)
      try { f.foreach(toValues(_, callback)) } finally {
        f.close()
      }
      val count = callback.result
      val bytes = byteStream.toByteArray
      val size = (bytes.length / count).toInt
      bytes.grouped(size).toSeq.sorted(BinaryOutputEncoder.DateOrdering).foreach(os.write)
      count
    } else {
      val callback = new ByteStreamCallback(os)
      try { f.foreach(toValues(_, callback)) } finally {
        f.close()
      }
      callback.result
    }
  }
}

object BinaryOutputEncoder extends LazyLogging {

  import AxisOrder._
  import org.locationtech.geomesa.utils.geotools.Conversions._
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  val BinEncodedSft: SimpleFeatureType = SimpleFeatureTypes.createType("bin", "bin:Bytes,*geom:Point:srid=4326")
  val BIN_ATTRIBUTE_INDEX = 0 // index of 'bin' attribute in BinEncodedSft

  // compares the 4 bytes representing the date in a bin array
  private val DateOrdering = new Ordering[Array[Byte]] {
    override def compare(x: Array[Byte], y: Array[Byte]): Int = {
      val compare1 = Ordering.Byte.compare(x(4), y(4))
      if (compare1 != 0) { return compare1 }
      val compare2 = Ordering.Byte.compare(x(5), y(5))
      if (compare2 != 0) { return compare2 }
      val compare3 = Ordering.Byte.compare(x(6), y(6))
      if (compare3 != 0) { return compare3 }
      Ordering.Byte.compare(x(7), y(7))
    }
  }

  case class EncodingOptions(geomField: Option[Int],
                             dtgField: Option[Int],
                             trackIdField: Option[Int],
                             labelField: Option[Int] = None,
                             axisOrder: Option[AxisOrder] = None)

  case class EncodedValues(trackId: Int, lat: Float, lon: Float, dtg: Long, label: Long)

  def apply(sft: SimpleFeatureType, options: EncodingOptions): BinaryOutputEncoder =
    new BinaryOutputEncoder(toValues(sft, options))

  def convertToTrack(f: SimpleFeature, i: Int): Int = convertToTrack(f.getAttribute(i))
  def convertToTrack(track: AnyRef): Int = if (track == null) { 0 } else { track.hashCode }

  // TODO could use `.getDateAsLong` if we know we have a KryoBufferSimpleFeature
  def convertToDate(f: SimpleFeature, i: Int): Long = convertToDate(f.getAttribute(i).asInstanceOf[Date])
  def convertToDate(date: Date): Long = if (date == null) { 0L } else { date.getTime }

  def convertToLabel(f: SimpleFeature, i: Int): Long = convertToLabel(f.getAttribute(i))
  def convertToLabel(label: AnyRef): Long = label match {
    case null => 0L
    case n: Number => n.longValue()
    case _ =>
      import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichTraversableOnce
      var sum = 0L
      label.toString.getBytes(StandardCharsets.UTF_8).iterator.take(8).foreachIndex {
        case (b, i) => sum += (b & 0xffL) << (8 * i)
      }
      sum
  }

  /**
    * Decodes a byte array
    *
    * @param encoded encoded byte array
    * @param callback callback for results
    */
  def decode(encoded: Array[Byte], callback: BinaryOutputCallback): Unit = {
    val buf = ByteBuffer.wrap(encoded).order(ByteOrder.LITTLE_ENDIAN)
    val trackId = buf.getInt
    val time = buf.getInt * 1000L
    val lat = buf.getFloat
    val lon = buf.getFloat
    if (encoded.length > 16) {
      val label = buf.getLong
      callback(trackId, lat, lon, time, label)
    } else {
      callback(trackId, lat, lon, time)
    }
  }

  def decode(encoded: Array[Byte]): EncodedValues = {
    var values: EncodedValues = null
    decode(encoded, new BinaryOutputCallback() {
      override def apply(trackId: Int, lat: Float, lon: Float, dtg: Long): Unit =
        values = EncodedValues(trackId, lat, lon, dtg, -1L)
      override def apply(trackId: Int, lat: Float, lon: Float, dtg: Long, label: Long): Unit =
        values = EncodedValues(trackId, lat, lon, dtg, label)
    })
    values
  }

  /**
    * Creates the function to map a simple feature to a bin-encoded buffer
    *
    * @param sft simple feature type
    * @param options encoding options
    * @return
    */
  private def toValues(sft: SimpleFeatureType, options: EncodingOptions): ToValues = {

    val geomIndex = options.geomField.getOrElse(sft.getGeomIndex)
    if (geomIndex == -1) {
      throw new IllegalArgumentException(s"Invalid geometry field requested for feature type ${sft.getTypeName}")
    }
    val dtgIndex = options.dtgField.orElse(sft.getDtgIndex).getOrElse(-1)
    if (dtgIndex == -1) {
      throw new RuntimeException(s"Invalid date field requested for feature type ${sft.getTypeName}")
    }
    val isSingleDate = classOf[Date].isAssignableFrom(sft.getDescriptor(dtgIndex).getType.getBinding)
    val axisOrder = options.axisOrder.getOrElse(AxisOrder.LonLat)

    val (isPoint, isLineString) = {
      val binding = sft.getDescriptor(geomIndex).getType.getBinding
      (binding == classOf[Point], binding == classOf[LineString])
    }

    // noinspection ExistsEquals
    if (options.trackIdField.exists(_ == -1)) {
      throw new IllegalArgumentException(s"Invalid track field requested for feature type ${sft.getTypeName}")
    } else if (options.labelField.exists(_ == -1)) {
      throw new IllegalArgumentException(s"Invalid label field requested for feature type ${sft.getTypeName}")
    } else if (!isSingleDate) {
      if (isLineString) {
        val dtgField = sft.getDescriptor(dtgIndex).getLocalName
        val sftAttributes = SimpleFeatureSpecParser.parse(SimpleFeatureTypes.encodeType(sft)).attributes
        sftAttributes.find(_.name == dtgField).foreach { spec =>
          if (!spec.isInstanceOf[ListAttributeSpec] ||
                !classOf[Date].isAssignableFrom(spec.asInstanceOf[ListAttributeSpec].subClass)) {
            throw new RuntimeException(s"Invalid date field requested for feature type ${sft.getTypeName}")
          }
        }
      } else {
        throw new RuntimeException(s"Invalid date field requested for feature type ${sft.getTypeName}")
      }
    }

    // gets the track id from a feature
    val getTrackId: (SimpleFeature) => Int = options.trackIdField match {
      case None => (f) => f.getID.hashCode
      case Some(trackId) => convertToTrack(_, trackId)
    }

    // gets the label from a feature
    val getLabelOption: Option[(SimpleFeature) => Long] = options.labelField.map { labelIndex =>
      convertToLabel(_, labelIndex)
    }

    if (isLineString) {
      // for linestrings, we return each point - use an array so we get constant-time lookup
      // depending on srs requested and wfs versions, axis order can be flipped
      val getLineLatLon: (SimpleFeature) => Array[(Float, Float)] = axisOrder match {
        case LatLon => lineToXY(_, geomIndex)
        case LonLat => lineToYX(_, geomIndex)
      }

      if (isSingleDate) {
        getLabelOption match {
          case None => new ToValuesLines(getTrackId, getLineLatLon, dtgIndex)
          case Some(getLabel) => new ToValuesLinesLabels(getTrackId, getLineLatLon, getLabel, dtgIndex)
        }
      } else {
        // for line strings, we need an array of dates corresponding to the points in the line
        val getLineDtg: (SimpleFeature) => Array[Long] = dateArray(_, dtgIndex)
        getLabelOption match {
          case None => new ToValuesLinesDates(getTrackId, getLineLatLon, getLineDtg)
          case Some(getLabel) => new ToValuesLinesDatesLabels(getTrackId, getLineLatLon, getLineDtg, getLabel)
        }
      }
    } else {
      // get lat/lon as floats
      // depending on srs requested and wfs versions, axis order can be flipped
      val getLatLon: (SimpleFeature) => (Float, Float) = (isPoint, axisOrder) match {
        case (true,  LatLon) => pointToXY(_, geomIndex)
        case (true,  LonLat) => pointToYX(_, geomIndex)
        case (false, LatLon) => geomToXY(_, geomIndex)
        case (false, LonLat) => geomToYX(_, geomIndex)
      }

      getLabelOption match {
        case None => new ToValuesPoints(getTrackId, getLatLon, dtgIndex)
        case Some(getLabel) => new ToValuesPointsLabels(getTrackId, getLatLon, getLabel, dtgIndex)
      }
    }
  }

  private def pointToXY(p: Point): (Float, Float) = (p.getX.toFloat, p.getY.toFloat)
  private def pointToYX(p: Point): (Float, Float) = (p.getY.toFloat, p.getX.toFloat)

  private def pointToXY(f: SimpleFeature, i: Int): (Float, Float) =
    pointToXY(f.getAttribute(i).asInstanceOf[Point])
  private def pointToYX(f: SimpleFeature, i: Int): (Float, Float) =
    pointToYX(f.getAttribute(i).asInstanceOf[Point])

  private def geomToXY(f: SimpleFeature, i: Int): (Float, Float) =
    pointToXY(f.getAttribute(i).asInstanceOf[Geometry].safeCentroid())
  private def geomToYX(f: SimpleFeature, i: Int): (Float, Float) =
    pointToYX(f.getAttribute(i).asInstanceOf[Geometry].safeCentroid())

  private def lineToXY(f: SimpleFeature, i: Int): Array[(Float, Float)] = {
    val line = f.getAttribute(i).asInstanceOf[LineString]
    Array.tabulate(line.getNumPoints)(i => pointToXY(line.getPointN(i)))
  }
  private def lineToYX(f: SimpleFeature, i: Int): Array[(Float, Float)] = {
    val line = f.getAttribute(i).asInstanceOf[LineString]
    Array.tabulate(line.getNumPoints)(i => pointToYX(line.getPointN(i)))
  }

  private def dateArray(f: SimpleFeature, i: Int): Array[Long] = {
    val dates = f.getAttribute(i).asInstanceOf[java.util.List[Date]]
    if (dates == null) { Array.empty } else { dates.map(_.getTime).toArray }
  }

  private trait ToValues {
    def apply(f: SimpleFeature, callback: BinaryOutputCallback): Unit
  }

  private class ToValuesPoints(getTrackId: (SimpleFeature) => Int,
                               getLatLon: (SimpleFeature) => (Float, Float),
                               dtgIndex: Int) extends ToValues {
    override def apply(f: SimpleFeature, callback: BinaryOutputCallback): Unit = {
      val (lat, lon) = getLatLon(f)
      callback(getTrackId(f), lat, lon, convertToDate(f, dtgIndex))
    }
  }

  private class ToValuesPointsLabels(getTrackId: (SimpleFeature) => Int,
                                     getLatLon: (SimpleFeature) => (Float, Float),
                                     getLabel: (SimpleFeature) => Long,
                                     dtgIndex: Int) extends ToValues {
    override def apply(f: SimpleFeature, callback: BinaryOutputCallback): Unit = {
      val (lat, lon) = getLatLon(f)
      callback(getTrackId(f), lat, lon, convertToDate(f, dtgIndex), getLabel(f))
    }
  }

  private class ToValuesLines(getTrackId: (SimpleFeature) => Int,
                              getLatLon: (SimpleFeature) => Array[(Float, Float)],
                              dtgIndex: Int) extends ToValues {
    override def apply(f: SimpleFeature, callback: BinaryOutputCallback): Unit = {
      val trackId = getTrackId(f)
      val points = getLatLon(f)
      val date = convertToDate(f, dtgIndex)
      var i = 0
      while (i < points.length) {
        val (lat, lon) = points(i)
        callback(trackId, lat, lon, date)
        i += 1
      }
    }
  }

  private class ToValuesLinesLabels(getTrackId: (SimpleFeature) => Int,
                                    getLatLon: (SimpleFeature) => Array[(Float, Float)],
                                    getLabel: (SimpleFeature) => Long,
                                    dtgIndex: Int) extends ToValues {
    override def apply(f: SimpleFeature, callback: BinaryOutputCallback): Unit = {
      val trackId = getTrackId(f)
      val points = getLatLon(f)
      val date = convertToDate(f, dtgIndex)
      val label = getLabel(f)
      var i = 0
      while (i < points.length) {
        val (lat, lon) = points(i)
        callback(trackId, lat, lon, date, label)
        i += 1
      }
    }
  }

  private class ToValuesLinesDates(getTrackId: (SimpleFeature) => Int,
                                   getLatLon: (SimpleFeature) => Array[(Float, Float)],
                                   getLineDtg: (SimpleFeature) => Array[Long]) extends ToValues {
    override def apply(f: SimpleFeature, callback: BinaryOutputCallback): Unit = {
      val trackId = getTrackId(f)
      val points = getLatLon(f)
      val dates = getLineDtg(f)
      val size = if (points.length == dates.length) { points.length } else {
        logger.warn(s"Mismatched geometries and dates for simple feature $f: ${points.toList} ${dates.toList}")
        math.min(points.length, dates.length)
      }
      var i = 0
      while (i < size) {
        val (lat, lon) = points(i)
        callback(trackId, lat, lon, dates(i))
        i += 1
      }
    }
  }

  private class ToValuesLinesDatesLabels(getTrackId: (SimpleFeature) => Int,
                                         getLatLon: (SimpleFeature) => Array[(Float, Float)],
                                         getLineDtg: (SimpleFeature) => Array[Long],
                                         getLabel: (SimpleFeature) => Long) extends ToValues {
    override def apply(f: SimpleFeature, callback: BinaryOutputCallback): Unit = {
      val trackId = getTrackId(f)
      val points = getLatLon(f)
      val dates = getLineDtg(f)
      val size = if (points.length == dates.length) { points.length } else {
        logger.warn(s"Mismatched geometries and dates for simple feature $f: ${points.toList} ${dates.toList}")
        math.min(points.length, dates.length)
      }
      val label = getLabel(f)
      var i = 0
      while (i < size) {
        val (lat, lon) = points(i)
        callback(trackId, lat, lon, dates(i), label)
        i += 1
      }
    }
  }
}
