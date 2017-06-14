/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Date

import com.vividsolutions.jts.geom.{Geometry, LineString, Point}
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.kryo.KryoBufferSimpleFeature
import org.locationtech.geomesa.filter.function.Convert2ViewerFunction
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.utils.bin.BinSorter
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait BinAggregatingScan extends AggregatingScan[ByteBufferResult] {
  import BinAggregatingScan.Configuration._

  var trackIndex: Int = -1
  var geomIndex: Int = -1
  var dtgIndex: Int = -1
  var labelIndex: Int = -1

  var getTrackId: (KryoBufferSimpleFeature) => Int = _
  var getDtg: (KryoBufferSimpleFeature) => Long = _
  var isDtgArray: Boolean = false
  var linePointIndex: Int = -1

  var binSize: Int = 16
  var sort: Boolean = false

  var writeBin: (KryoBufferSimpleFeature, ByteBufferResult) => Unit = _

  // create the result object for the current scan
  override protected def initResult(sft: SimpleFeatureType,
                                    transform: Option[SimpleFeatureType],
                                    options: Map[String, String]): ByteBufferResult = {
    geomIndex = options(GeomOpt).toInt
    labelIndex = options.get(LabelOpt).map(_.toInt).getOrElse(-1)

    trackIndex = options(TrackOpt).toInt
    getTrackId = if (trackIndex == -1) {
      (sf) => sf.getID.hashCode
    } else {
      (sf) => {
        val track = sf.getAttribute(trackIndex)
        if (track == null) { 0 } else { track.hashCode() }
      }
    }
    dtgIndex = options(DateOpt).toInt
    isDtgArray = options.get(DateArrayOpt).exists(_.toBoolean)
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
      (sf) => sf.getDateAsLong(dtgIndex)
    }

    binSize = if (labelIndex == -1) 16 else 24
    sort = options(SortOpt).toBoolean

    // derive the bin values from the features
    writeBin = if (sft.isPoints) {
      if (labelIndex == -1) writePoint else writePointWithLabel
    } else if (sft.isLines) {
      if (labelIndex == -1) writeLineString else writeLineStringWithLabel
    } else {
      if (labelIndex == -1) writeGeometry else writeGeometryWithLabel
    }

    val batchSize = options(BatchSizeOpt).toInt * binSize

    val buffer = ByteBuffer.wrap(Array.ofDim(batchSize)).order(ByteOrder.LITTLE_ENDIAN)
    val overflow = ByteBuffer.wrap(Array.ofDim(binSize * 16)).order(ByteOrder.LITTLE_ENDIAN)

    new ByteBufferResult(buffer, overflow)
  }

  // add the feature to the current aggregated result
  override def aggregateResult(sf: SimpleFeature, result: ByteBufferResult): Unit =
    writeBin(sf.asInstanceOf[KryoBufferSimpleFeature], result)

  // encode the result as a byte array
  override def encodeResult(result: ByteBufferResult): Array[Byte] = {
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
  private def writeBinToBuffer(sf: KryoBufferSimpleFeature, pt: Point, result: ByteBufferResult): Unit = {
    val buffer = result.ensureCapacity(16)
    buffer.putInt(getTrackId(sf))
    buffer.putInt((getDtg(sf) / 1000).toInt)
    buffer.putFloat(pt.getY.toFloat) // y is lat
    buffer.putFloat(pt.getX.toFloat) // x is lon
  }

  /**
    * Writes a label to the buffer in the bin format
    */
  private def writeLabelToBuffer(sf: KryoBufferSimpleFeature, result: ByteBufferResult): Unit = {
    val label = sf.getAttribute(labelIndex)
    val labelAsLong = if (label == null) { 0L } else { Convert2ViewerFunction.convertToLabel(label.toString) }
    val buffer = result.ensureCapacity(8)
    buffer.putLong(labelAsLong)
  }

  /**
    * Writes a bin record from a feature that has a point geometry
    */
  def writePoint(sf: KryoBufferSimpleFeature, result: ByteBufferResult): Unit =
    writeBinToBuffer(sf, sf.getAttribute(geomIndex).asInstanceOf[Point], result)

  /**
    * Writes point + label
    */
  def writePointWithLabel(sf: KryoBufferSimpleFeature, result: ByteBufferResult): Unit = {
    writePoint(sf, result)
    writeLabelToBuffer(sf, result)
  }

  /**
    * Writes bins record from a feature that has a line string geometry.
    * The feature will be multiple bin records.
    */
  def writeLineString(sf: KryoBufferSimpleFeature, result: ByteBufferResult): Unit = {
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
  def writeLineStringWithLabel(sf: KryoBufferSimpleFeature, result: ByteBufferResult): Unit = {
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
  def writeGeometry(sf: KryoBufferSimpleFeature, result: ByteBufferResult): Unit = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    writeBinToBuffer(sf, sf.getAttribute(geomIndex).asInstanceOf[Geometry].safeCentroid(), result)
  }

  /**
    * Writes geom + label
    */
  def writeGeometryWithLabel(sf: KryoBufferSimpleFeature, result: ByteBufferResult): Unit = {
    writeGeometry(sf, result)
    writeLabelToBuffer(sf, result)
  }
}

object BinAggregatingScan {
  object Configuration {
    // configuration keys
    val BatchSizeOpt  = "batch"
    val SortOpt       = "sort"
    val TrackOpt      = "track"
    val GeomOpt       = "geom"
    val DateOpt       = "dtg"
    val LabelOpt      = "label"
    val DateArrayOpt  = "dtg-array"
  }

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _, _],
                filter: Option[Filter],
                trackId: String,
                geom: String,
                dtg: Option[String],
                label: Option[String],
                batchSize: Int,
                sort: Boolean,
                hints: Hints): Map[String, String] = {
    import AggregatingScan.{OptionToConfig, StringToConfig}
    import Configuration._

    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor


    val dtgIndex = dtg.map(sft.indexOf).getOrElse(-1)
    val setDateArrayOpt: Option[String] =
      if (sft.isLines && dtgIndex != -1 && sft.getDescriptor(dtgIndex).isList &&
        classOf[Date].isAssignableFrom(sft.getDescriptor(dtgIndex).getListType())) {
        Some("true")
      } else {
        None
      }

    val base = AggregatingScan.configure(sft, index, filter, None, hints.getSampling) // note: don't pass transforms
    base ++ AggregatingScan.optionalMap(
      BatchSizeOpt -> batchSize.toString,
      TrackOpt     -> sft.indexOf(trackId).toString,
      GeomOpt      -> sft.indexOf(geom).toString,
      DateOpt      -> dtg.map(sft.indexOf).getOrElse(-1).toString,
      DateArrayOpt -> setDateArrayOpt,
      LabelOpt     -> label.map(sft.indexOf(_).toString),
      SortOpt      -> sort.toString
    )
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

