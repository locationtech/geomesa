/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.utils.bin

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Date

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{Geometry, LineString, Point}
import org.geotools.factory.Hints
import org.locationtech.geomesa.filter.function.Convert2ViewerFunction
import org.locationtech.geomesa.index.conf.QueryHints.RichHints
import org.locationtech.geomesa.index.utils.SamplingIterator
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

trait BinAggregatingUtils extends SamplingIterator {

  import org.locationtech.geomesa.index.utils.bin.BinAggregatingUtils._

  var trackIndex: Int = -1
  var geomIndex: Int = -1
  var dtgIndex: Int = -1
  var labelIndex: Int = -1

  var getTrackId: (SimpleFeature) => Int = _
  var getDtg: (SimpleFeature) => Long = _
  var isDtgArray: Boolean = false
  var linePointIndex: Int = -1

  var binSize: Int = 16
  var sort: Boolean = false

  var sampling: Option[(SimpleFeature) => Boolean] = _

  var writeBin: (SimpleFeature, ByteBufferResult) => Unit = _

  def isPoints: Boolean
  def isLines: Boolean

  def initBinAggregation(options: Map[String, String]): ByteBufferResult = {
    geomIndex = options(GEOM_OPT).toInt
    labelIndex = options.get(LABEL_OPT).map(_.toInt).getOrElse(-1)

    trackIndex = options(TRACK_OPT).toInt
    getTrackId = if (trackIndex == -1) {
      (sf) => sf.getID.hashCode
    } else {
      (sf) => {
        val track = sf.getAttribute(trackIndex)
        if (track == null) { 0 } else { track.hashCode() }
      }
    }
    dtgIndex = options(DATE_OPT).toInt
    isDtgArray = options.get(DATE_ARRAY_OPT).exists(_.toBoolean)
    getDtg = if (dtgIndex == -1) {
      (_) => 0L
    } else if (isDtgArray) {
      (sf) => {
        try {
          sf.getAttribute(dtgIndex).asInstanceOf[java.util.List[Date]].get(linePointIndex).getTime
        } catch {
          case _: IndexOutOfBoundsException => 0L
        }
      }
    } else {
      (sf) => sf.getAttribute(dtgIndex).asInstanceOf[Date].getTime
      //JNH: Not sure about this!
      // Code was KryoBufferSimpleFeature.getDateAsLong(dtgIndex)
    }

    binSize = if (labelIndex == -1) 16 else 24
    sort = options(SORT_OPT).toBoolean

    sampling = sample(options)

    // derive the bin values from the features
    val writeGeom: (SimpleFeature, ByteBufferResult) => Unit = if (isPoints) {
      if (labelIndex == -1) writePoint else writePointWithLabel
    } else if (isLines) {
      if (labelIndex == -1) writeLineString else writeLineStringWithLabel
    } else {
      if (labelIndex == -1) writeGeometry else writeGeometryWithLabel
    }

    writeBin = sampling match {
      case None => writeGeom
      case Some(samp) => (sf, bb) => if (samp(sf)) { writeGeom(sf, bb) }
    }

    val batchSize = options(BATCH_SIZE_OPT).toInt * binSize

    val buffer = ByteBuffer.wrap(Array.ofDim(batchSize)).order(ByteOrder.LITTLE_ENDIAN)
    val overflow = ByteBuffer.wrap(Array.ofDim(binSize * 16)).order(ByteOrder.LITTLE_ENDIAN)

    new ByteBufferResult(buffer, overflow)
  }

  def encodeBinAggregationResult(result: ByteBufferResult): Array[Byte] = {
    val bytes = if (result.overflow.position() > 0) {
      // overflow bytes - copy the two buffers into one
      val copy = Array.ofDim[Byte](result.buffer.position + result.overflow.position)
      System.arraycopy(result.buffer.array, 0, copy, 0, result.buffer.position)
      System.arraycopy(result.overflow.array, 0, copy, result.buffer.position, result.overflow.position)
      copy
    } else if (result.buffer.position == result.buffer.limit) {
      // use the existing buffer if possible
      result.buffer.array
    } else {
      // if not, we have to copy it - values do not allow you to specify a valid range
      val copy = Array.ofDim[Byte](result.buffer.position)
      System.arraycopy(result.buffer.array, 0, copy, 0, result.buffer.position)
      copy
    }
    if (sort) {
      BinSorter.quickSort(bytes, 0, bytes.length - binSize, binSize)
    }
    bytes
  }

  /**
    * Writes a point to our buffer in the bin format
    */
  private def writeBinToBuffer(sf: SimpleFeature, pt: Point, result: ByteBufferResult): Unit = {
    val buffer = result.ensureCapacity(16)
    buffer.putInt(getTrackId(sf))
    buffer.putInt((getDtg(sf) / 1000).toInt)
    buffer.putFloat(pt.getY.toFloat) // y is lat
    buffer.putFloat(pt.getX.toFloat) // x is lon
  }

  /**
    * Writes a label to the buffer in the bin format
    */
  private def writeLabelToBuffer(sf: SimpleFeature, result: ByteBufferResult): Unit = {
    val label = sf.getAttribute(labelIndex)
    val labelAsLong = if (label == null) { 0L } else { Convert2ViewerFunction.convertToLabel(label.toString) }
    val buffer = result.ensureCapacity(8)
    buffer.putLong(labelAsLong)
  }

  /**
    * Writes a bin record from a feature that has a point geometry
    */
  def writePoint(sf: SimpleFeature, result: ByteBufferResult): Unit =
    writeBinToBuffer(sf, sf.getAttribute(geomIndex).asInstanceOf[Point], result)

  /**
    * Writes point + label
    */
  def writePointWithLabel(sf: SimpleFeature, result: ByteBufferResult): Unit = {
    writePoint(sf, result)
    writeLabelToBuffer(sf, result)
  }

  /**
    * Writes bins record from a feature that has a line string geometry.
    * The feature will be multiple bin records.
    */
  def writeLineString(sf: SimpleFeature, result: ByteBufferResult): Unit = {
    val geom = sf.getAttribute(geomIndex).asInstanceOf[LineString]
    linePointIndex = 0
    while (linePointIndex < geom.getNumPoints) {
      writeBinToBuffer(sf, geom.getPointN(linePointIndex), result)
      linePointIndex += 1
    }
  }

  /**
    * Writes line string + label
    */
  def writeLineStringWithLabel(sf: SimpleFeature, result: ByteBufferResult): Unit = {
    val geom = sf.getAttribute(geomIndex).asInstanceOf[LineString]
    linePointIndex = 0
    while (linePointIndex < geom.getNumPoints) {
      writeBinToBuffer(sf, geom.getPointN(linePointIndex), result)
      writeLabelToBuffer(sf, result)
      linePointIndex += 1
    }
  }

  /**
    * Writes a bin record from a feature that has a arbitrary geometry.
    * A single internal point will be written.
    */
  def writeGeometry(sf: SimpleFeature, result: ByteBufferResult): Unit = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    writeBinToBuffer(sf, sf.getAttribute(geomIndex).asInstanceOf[Geometry].safeCentroid(), result)
  }

  /**
    * Writes geom + label
    */
  def writeGeometryWithLabel(sf: SimpleFeature, result: ByteBufferResult): Unit = {
    writeGeometry(sf, result)
    writeLabelToBuffer(sf, result)
  }
}

object BinAggregatingUtils extends LazyLogging {
  private val zeroPoint = WKTUtils.read("POINT(0 0)")

  val DEFAULT_PRIORITY = 25

  // configuration keys
  private val BATCH_SIZE_OPT = "batch"
  private val SORT_OPT       = "sort"

  private val TRACK_OPT      = "track"
  private val GEOM_OPT       = "geom"
  private val DATE_OPT       = "dtg"
  private val LABEL_OPT      = "label"

  private val DATE_ARRAY_OPT = "dtg-array"

  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType


  /**
    * Determines if the requested fields match the precomputed bin data
    */
  def canUsePrecomputedBins(sft: SimpleFeatureType, hints: Hints): Boolean = {
    // noinspection ExistsEquals
    sft.getBinTrackId.exists(_ == hints.getBinTrackIdField) &&
      hints.getBinGeomField.forall(_ == sft.getGeomField) &&
      hints.getBinDtgField == sft.getDtgField &&
      hints.getBinLabelField.isEmpty &&
      hints.getSampleByField.forall(_ == hints.getBinTrackIdField)
  }

  /**
    * Adapts the iterator to create simple features.
    * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
    */
  //  def kvsToFeatures(): (Entry[Key, Value]) => SimpleFeature = {
  //    val sf = new ScalaSimpleFeature("", BinaryOutputEncoder.BinEncodedSft)
  //    sf.setAttribute(1, zeroPoint)
  //    (e: Entry[Key, Value]) => {
  //      sf.setAttribute(BIN_ATTRIBUTE_INDEX, e.getValue.get())
  //      sf
  //    }
  //  }

  /**
    * Fallback for when we can't use the aggregating iterator (for example, if the features are avro encoded).
    * Instead, do bin conversion in client.
    *
    * Only encodes one bin (or one bin line) per feature
    */
  //  def nonAggregatedKvsToFeatures(sft: SimpleFeatureType,
  //                                 index: AccumuloFeatureIndex,
  //                                 hints: Hints,
  //                                 serializationType: SerializationType): (Entry[Key, Value]) => SimpleFeature = {
  //
  //    // don't use return sft from query hints, as it will be bin_sft
  //    val returnSft = hints.getTransformSchema.getOrElse(sft)
  //
  //    val trackIdIndex = returnSft.indexOf(hints.getBinTrackIdField)
  //    val geomIndex = hints.getBinGeomField.map(returnSft.indexOf).getOrElse(returnSft.getGeomIndex)
  //    val dtgIndex= hints.getBinDtgField.map(returnSft.indexOf).getOrElse(returnSft.getDtgIndex.get)
  //    val labelIndexOpt= hints.getBinLabelField.map(returnSft.indexOf)
  //
  //    val isPoint = returnSft.isPoints
  //    val isLineString = !isPoint && returnSft.isLines
  //
  //    val encode: (SimpleFeature) => Array[Byte] = labelIndexOpt match {
  //      case None if isPoint =>
  //        (sf) => {
  //          val trackId = getTrack(sf, trackIdIndex)
  //          val (lat, lon) = getPointGeom(sf, geomIndex)
  //          val dtg = getDtg(sf, dtgIndex)
  //          Convert2ViewerFunction.encodeToByteArray(BasicValues(lat, lon, dtg, trackId))
  //        }
  //
  //      case None if isLineString =>
  //        val buf = new ByteArrayOutputStream()
  //        (sf) => {
  //          buf.reset()
  //          val trackId = getTrack(sf, trackIdIndex)
  //          val points = getLineGeom(sf, geomIndex)
  //          val dtgs = getLineDtg(sf, dtgIndex)
  //          if (points.length != dtgs.length) {
  //            logger.warn(s"Mismatched geometries and dates for simple feature ${sf.getID} - skipping")
  //          } else {
  //            var i = 0
  //            while (i < points.length) {
  //              val (lat, lon) = points(i)
  //              Convert2ViewerFunction.encode(BasicValues(lat, lon, dtgs(i), trackId), buf)
  //              i += 1
  //            }
  //          }
  //          buf.toByteArray
  //        }
  //
  //      case None =>
  //        (sf) => {
  //          val trackId = getTrack(sf, trackIdIndex)
  //          val (lat, lon) = getGenericGeom(sf, geomIndex)
  //          val dtg = getDtg(sf, dtgIndex)
  //          Convert2ViewerFunction.encodeToByteArray(BasicValues(lat, lon, dtg, trackId))
  //        }
  //
  //      case Some(lblIndex) if isPoint =>
  //        (sf) => {
  //          val trackId = getTrack(sf, trackIdIndex)
  //          val (lat, lon) = getPointGeom(sf, geomIndex)
  //          val dtg = getDtg(sf, dtgIndex)
  //          val label = getLabel(sf, lblIndex)
  //          Convert2ViewerFunction.encodeToByteArray(ExtendedValues(lat, lon, dtg, trackId, label))
  //        }
  //
  //      case Some(lblIndex) if isLineString =>
  //        val buf = new ByteArrayOutputStream()
  //        (sf) => {
  //          buf.reset()
  //          val trackId = getTrack(sf, trackIdIndex)
  //          val points = getLineGeom(sf, geomIndex)
  //          val dtgs = getLineDtg(sf, dtgIndex)
  //          val label = getLabel(sf, lblIndex)
  //          if (points.length != dtgs.length) {
  //            logger.warn(s"Mismatched geometries and dates for simple feature ${sf.getID} - skipping")
  //          } else {
  //            var i = 0
  //            while (i < points.length) {
  //              val (lat, lon) = points(i)
  //              Convert2ViewerFunction.encode(ExtendedValues(lat, lon, dtgs(i), trackId, label), buf)
  //              i += 1
  //            }
  //          }
  //          buf.toByteArray
  //        }
  //
  //      case Some(lblIndex) =>
  //        (sf) => {
  //          val trackId = getTrack(sf, trackIdIndex)
  //          val (lat, lon) = getGenericGeom(sf, geomIndex)
  //          val dtg = getDtg(sf, dtgIndex)
  //          val label = getLabel(sf, lblIndex)
  //          Convert2ViewerFunction.encodeToByteArray(ExtendedValues(lat, lon, dtg, trackId, label))
  //        }
  //    }
  //
  //    if (index.serializedWithId) {
  //      val deserializer = SimpleFeatureDeserializers(returnSft, serializationType)
  //      (e: Entry[Key, Value]) => {
  //        val deserialized = deserializer.deserialize(e.getValue.get())
  //        // set the value directly in the array, as we don't support byte arrays as properties
  //        new ScalaSimpleFeature(deserialized.getID, BinaryOutputEncoder.BinEncodedSft, Array(encode(deserialized), zeroPoint))
  //      }
  //    } else {
  //      val getId = index.getIdFromRow(sft)
  //      val deserializer = SimpleFeatureDeserializers(returnSft, serializationType, SerializationOptions.withoutId)
  //      (e: Entry[Key, Value]) => {
  //        val deserialized = deserializer.deserialize(e.getValue.get())
  //        val row = e.getKey.getRow
  //        deserialized.getIdentifier.asInstanceOf[FeatureIdImpl].setID(getId(row.getBytes, 0, row.getLength))
  //        // set the value directly in the array, as we don't support byte arrays as properties
  //        new ScalaSimpleFeature(deserialized.getID, BinaryOutputEncoder.BinEncodedSft, Array(encode(deserialized), zeroPoint))
  //      }
  //    }
  //
  //  }

  private def getTrack(sf: SimpleFeature, i: Int): Int = {
    val t = sf.getAttribute(i)
    if (t == null) { 0 } else { t.hashCode }
  }

  // get a single geom
  private def getPointGeom(sf: SimpleFeature, i: Int): (Float, Float) = {
    val p = sf.getAttribute(i).asInstanceOf[Point]
    (p.getY.toFloat, p.getX.toFloat)
  }

  // get a line geometry as an array of points
  private def getLineGeom(sf: SimpleFeature, i: Int): Array[(Float, Float)] = {
    val geom = sf.getAttribute(i).asInstanceOf[LineString]
    (0 until geom.getNumPoints).map(geom.getPointN).map(p => (p.getY.toFloat, p.getX.toFloat)).toArray
  }

  // get a single geom
  private def getGenericGeom(sf: SimpleFeature, i: Int): (Float, Float) = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    val p = sf.getAttribute(i).asInstanceOf[Geometry].safeCentroid()
    (p.getY.toFloat, p.getX.toFloat)
  }

  // get a single date
  private def getDtg(sf: SimpleFeature, i: Int): Long = {
    val date = sf.getAttribute(i).asInstanceOf[Date]
    if (date == null) System.currentTimeMillis else date.getTime
  }

  // for line strings, we need an array of dates corresponding to the points in the line
  private def getLineDtg(sf: SimpleFeature, i: Int): Array[Long] = {
    import scala.collection.JavaConversions._
    val dates = sf.getAttribute(i).asInstanceOf[java.util.List[Date]]
    if (dates == null) Array.empty else dates.map(_.getTime).toArray
  }

  // get a label as a long
  private def getLabel(sf: SimpleFeature, i: Int): Long = {
    val lbl = sf.getAttribute(i)
    if (lbl == null) 0L else Convert2ViewerFunction.convertToLabel(lbl.toString)
  }
}

// wrapper for java's byte buffer that adds scala methods for the aggregating iterator
class ByteBufferResult(val buffer: ByteBuffer, var overflow: ByteBuffer) {
  def ensureCapacity(size: Int): ByteBuffer = {
    if (buffer.position < buffer.limit - size) {
      buffer
    } else if (overflow.position < overflow.limit - size) {
      overflow
    } else {
      val expanded = Array.ofDim[Byte](overflow.limit * 2)
      System.arraycopy(overflow.array, 0, expanded, 0, overflow.limit)
      val order = overflow.order
      val position = overflow.position
      overflow = ByteBuffer.wrap(expanded).order(order).position(position).asInstanceOf[ByteBuffer]
      overflow
    }
  }

  def isEmpty: Boolean = buffer.position == 0
  def clear(): Unit = {
    buffer.clear()
    overflow.clear()
  }
}


