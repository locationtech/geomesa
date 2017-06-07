/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.hbase.coprocessor.aggregators
import java.nio.{ByteBuffer, ByteOrder}
import java.util.Date

import com.vividsolutions.jts.geom.Point
import org.geotools.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.filter.function.BinaryOutputEncoder.BIN_ATTRIBUTE_INDEX
import org.locationtech.geomesa.filter.function.{BinaryOutputEncoder, Convert2ViewerFunction}
import org.locationtech.geomesa.hbase.coprocessor.aggregators.HBaseBinAggregator._
import org.locationtech.geomesa.index.iterators.IteratorCache
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.text.WKTUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.mutable

class HBaseBinAggregator extends GeoMesaHBaseAggregator {
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
  var sft: SimpleFeatureType = _

  //var sampling: Option[(SimpleFeature) => Boolean] = _

  var writeBin: (SimpleFeature, ByteBufferResult) => Unit = _

  var internalResult: ByteBufferResult = _

  override def init(options: Map[String, String]): Unit = {
    val spec = options(SFT_OPT)
    sft = IteratorCache.sft(spec)

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
      (sf) => sf.getAttribute(dtgIndex).asInstanceOf[Date].getTime // JNH?!??!?!?
    }

    binSize = if (labelIndex == -1) 16 else 24
    sort = options(SORT_OPT).toBoolean

    //sampling = sample(options)

    // derive the bin values from the features
    val writeGeom: (SimpleFeature, ByteBufferResult) => Unit = if (sft.isPoints) {
      if (labelIndex == -1) writePoint else writePointWithLabel
    } else ???
//    else if (sft.isLines) {
//      if (labelIndex == -1) writeLineString else writeLineStringWithLabel
//    } else {
//      if (labelIndex == -1) writeGeometry else writeGeometryWithLabel
//    }

    writeBin = writeGeom
//    sampling match {
//      case None => writeGeom
//      case Some(samp) => (sf, bb) => if (samp(sf)) { writeGeom(sf, bb) }
//    }

    val batchSize = options(BATCH_SIZE_OPT).toInt * binSize

    val buffer = ByteBuffer.wrap(Array.ofDim(batchSize)).order(ByteOrder.LITTLE_ENDIAN)
    val overflow = ByteBuffer.wrap(Array.ofDim(binSize * 16)).order(ByteOrder.LITTLE_ENDIAN)

    internalResult = new ByteBufferResult(buffer, overflow)
  }

  // TODO:  Move this to a logical shared place
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
    * Writes a label to the buffer in the bin format
    */
  private def writeLabelToBuffer(sf: SimpleFeature, result: ByteBufferResult): Unit = {
    val label = sf.getAttribute(labelIndex)
    val labelAsLong = if (label == null) { 0L } else { Convert2ViewerFunction.convertToLabel(label.toString) }
    val buffer = result.ensureCapacity(8)
    buffer.putLong(labelAsLong)
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


  override def aggregate(sf: SimpleFeature): Unit = writeBin(sf, internalResult)

  override def encodeResult(): Array[Byte] = encodeResult(internalResult)

  // TODD: Refactor - copied code
  def encodeResult(result: ByteBufferResult): Array[Byte] = {
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
//    if (sort) {
//      BinSorter.quickSort(bytes, 0, bytes.length - binSize, binSize)
//    }
    bytes
  }
}

object HBaseBinAggregator {
  // TODO Factor out!
  val SFT_OPT      = "sft"
  private val zeroPoint = WKTUtils.read("POINT(0 0)")
  private val BATCH_SIZE_OPT = "batch"
  private val SORT_OPT       = "sort"

  private val TRACK_OPT      = "track"
  private val GEOM_OPT       = "geom"
  private val DATE_OPT       = "dtg"
  private val LABEL_OPT      = "label"

  private val DATE_ARRAY_OPT = "dtg-array"

  def bytesToFeatures(bytes : Array[Byte]): SimpleFeature = {
    val sf = new ScalaSimpleFeature("", BinaryOutputEncoder.BinEncodedSft)
    sf.setAttribute(1, zeroPoint)
    sf.setAttribute(BIN_ATTRIBUTE_INDEX, bytes)
    sf
  }

  def configure(sft: SimpleFeatureType,
                hints: Hints): Map[String, String] = {

    val is = mutable.Map.empty[String, String]
    is.put(GeoMesaHBaseAggregator.AGGREGATOR_CLASS, classOf[HBaseBinAggregator].getName)
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    
    val trackId = hints.getBinTrackIdField
    val geom = hints.getBinGeomField.getOrElse(sft.getGeomField)
    val dtg = hints.getBinDtgField.orElse(sft.getDtgField)
    val label = hints.getBinLabelField
    val batchSize = hints.getBinBatchSize
    val sort = hints.isBinSorting
    val sampling = hints.getSampling
    
    is.put(BATCH_SIZE_OPT, batchSize.toString)
    is.put(TRACK_OPT, sft.indexOf(trackId).toString)
    is.put(GEOM_OPT, sft.indexOf(geom).toString)
    val dtgIndex = dtg.map(sft.indexOf).getOrElse(-1)
    is.put(DATE_OPT, dtgIndex.toString)
//    if (sft.isLines && dtgIndex != -1 && sft.getDescriptor(dtgIndex).isList &&
//      classOf[Date].isAssignableFrom(sft.getDescriptor(dtgIndex).getListType())) {
//      is.put(DATE_ARRAY_OPT, "true")
//    }
    //    sampling.foreach(SamplingIterator.configure(is, sft, _))
    label.foreach(l => is.put(LABEL_OPT, sft.indexOf(l).toString))
    is.put(SORT_OPT, sort.toString)
    is.put(SFT_OPT, SimpleFeatureTypes.encodeType(sft, includeUserData = true))

    is.toMap
  }
}

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