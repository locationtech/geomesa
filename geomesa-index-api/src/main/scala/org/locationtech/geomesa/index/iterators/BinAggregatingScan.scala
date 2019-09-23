/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Date

import org.geotools.factory.Hints
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.BinAggregatingScan.{ByteBufferResult, ResultCallback}
import org.locationtech.geomesa.index.utils.bin.BinSorter
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.bin.{BinaryOutputCallback, BinaryOutputEncoder}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait BinAggregatingScan extends AggregatingScan[ByteBufferResult] {

  import BinAggregatingScan.Configuration._

  var encoding: EncodingOptions = _
  var encoder: BinaryOutputEncoder = _
  var callback: ResultCallback = _

  var binSize: Int = 16
  var sort: Boolean = false

  // create the result object for the current scan
  override protected def initResult(
      sft: SimpleFeatureType,
      transform: Option[SimpleFeatureType],
      options: Map[String, String]): ByteBufferResult = {
    val geom = options.get(GeomOpt).map(_.toInt).filter(_ != -1)
    val dtg = options.get(DateOpt).map(_.toInt).filter(_ != -1)
    val track = options.get(TrackOpt).map(_.toInt).filter(_ != -1)
    val label = options.get(LabelOpt).map(_.toInt).filter(_ != -1)

    encoding = EncodingOptions(geom, dtg, track, label)
    encoder = BinaryOutputEncoder(sft, encoding)

    binSize = if (label.isEmpty) { 16 } else { 24 }
    sort = options(SortOpt).toBoolean

    val batchSize = options(BatchSizeOpt).toInt * binSize

    val buffer = ByteBuffer.wrap(Array.ofDim(batchSize)).order(ByteOrder.LITTLE_ENDIAN)
    val overflow = ByteBuffer.wrap(Array.ofDim(binSize * 16)).order(ByteOrder.LITTLE_ENDIAN)

    callback = new ResultCallback(new ByteBufferResult(buffer, overflow))

    callback.result
  }

  // add the feature to the current aggregated result
  override protected def aggregateResult(sf: SimpleFeature, result: ByteBufferResult): Unit =
    encoder.encode(sf, callback)

  override protected def notFull(result: ByteBufferResult): Boolean =
    result.buffer.position < result.buffer.limit

  // encode the result as a byte array
  override protected def encodeResult(result: ByteBufferResult): Array[Byte] = {
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
}

object BinAggregatingScan {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

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
                index: GeoMesaFeatureIndex[_, _],
                filter: Option[Filter],
                trackId: String,
                geom: String,
                dtg: Option[String],
                label: Option[String],
                batchSize: Int,
                sort: Boolean,
                sampling: Option[(Float, Option[String])]): Map[String, String] = {
    import AggregatingScan.{OptionToConfig, StringToConfig}
    import Configuration._
    import org.locationtech.geomesa.utils.geotools.RichAttributeDescriptors.RichAttributeDescriptor

    val dtgIndex = dtg.map(sft.indexOf).getOrElse(-1)
    val setDateArrayOpt: Option[String] =
      if (sft.isLines && dtgIndex != -1 && sft.getDescriptor(dtgIndex).isList &&
        classOf[Date].isAssignableFrom(sft.getDescriptor(dtgIndex).getListType())) {
        Some("true")
      } else {
        None
      }

    val base = AggregatingScan.configure(sft, index, filter, None, sampling) // note: don't pass transforms
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

  /**
    * Get the attributes used by a BIN query
    *
    * @param hints query hints
    * @param sft simple feature type
    * @return
    */
  def propertyNames(hints: Hints, sft: SimpleFeatureType): Seq[String] = {
    val geom = hints.getBinGeomField.orElse(Option(sft.getGeomField))
    val dtg = hints.getBinDtgField.orElse(sft.getDtgField)
    (Seq(hints.getBinTrackIdField) ++ geom ++ dtg ++ hints.getBinLabelField).distinct.filter(_ != "id")
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

  class ResultCallback(val result: ByteBufferResult) extends BinaryOutputCallback {
    override def apply(trackId: Int, lat: Float, lon: Float, dtg: Long): Unit = {
      val buffer = result.ensureCapacity(16)
      put(buffer, trackId, lat, lon, dtg)
    }

    override def apply(trackId: Int, lat: Float, lon: Float, dtg: Long, label: Long): Unit = {
      val buffer = result.ensureCapacity(24)
      put(buffer, trackId, lat, lon, dtg, label)
    }
  }
}
