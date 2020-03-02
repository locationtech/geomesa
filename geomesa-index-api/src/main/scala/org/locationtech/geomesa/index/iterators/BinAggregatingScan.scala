/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators

import java.nio.{ByteBuffer, ByteOrder}
import java.util.Date

import org.geotools.util.factory.Hints
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.api.QueryPlan.ResultsToFeatures
import org.locationtech.geomesa.index.iterators.BinAggregatingScan.ResultCallback
import org.locationtech.geomesa.index.utils.bin.BinSorter
import org.locationtech.geomesa.utils.bin.BinaryOutputEncoder.EncodingOptions
import org.locationtech.geomesa.utils.bin.{BinaryOutputCallback, BinaryOutputEncoder}
import org.locationtech.geomesa.utils.geotools.GeometryUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter

trait BinAggregatingScan extends AggregatingScan[ResultCallback] {

  import BinAggregatingScan.Configuration._

  // create the result object for the current scan
  override protected def createResult(
      sft: SimpleFeatureType,
      transform: Option[SimpleFeatureType],
      batchSize: Int,
      options: Map[String, String]): ResultCallback = {
    val geom = options.get(GeomOpt).map(_.toInt).filter(_ != -1)
    val dtg = options.get(DateOpt).map(_.toInt).filter(_ != -1)
    val track = options.get(TrackOpt).map(_.toInt).filter(_ != -1)
    val label = options.get(LabelOpt).map(_.toInt).filter(_ != -1)

    val encoder = BinaryOutputEncoder(sft, EncodingOptions(geom, dtg, track, label))

    val binSize = if (label.isEmpty) { 16 } else { 24 }
    val sort = options(SortOpt).toBoolean

    val buffer = ByteBuffer.wrap(Array.ofDim(batchSize * binSize)).order(ByteOrder.LITTLE_ENDIAN)
    val overflow = ByteBuffer.wrap(Array.ofDim(binSize * 16)).order(ByteOrder.LITTLE_ENDIAN)

    new ResultCallback(buffer, overflow, encoder, binSize, sort)
  }

  override protected def defaultBatchSize: Int =
    throw new IllegalArgumentException("Batch scan is specified per scan")
}

object BinAggregatingScan {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  // configuration keys
  object Configuration {
    val SortOpt       = "sort"
    val TrackOpt      = "track"
    val GeomOpt       = "geom"
    val DateOpt       = "dtg"
    val LabelOpt      = "label"
    val DateArrayOpt  = "dtg-array"

    @deprecated("AggregatingScan.Configuration.BatchSizeOpt")
    val BatchSizeOpt = "batch"
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

    val base = AggregatingScan.configure(sft, index, filter, None, sampling, batchSize) // note: don't pass transforms
    base ++ AggregatingScan.optionalMap(
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

  class ResultCallback(
      buffer: ByteBuffer,
      private var overflow: ByteBuffer,
      encoder: BinaryOutputEncoder,
      binSize: Int,
      sort: Boolean
    ) extends AggregatingScan.Result with BinaryOutputCallback {

    override def apply(trackId: Int, lat: Float, lon: Float, dtg: Long): Unit =
      put(ensureCapacity(16), trackId, lat, lon, dtg)

    override def apply(trackId: Int, lat: Float, lon: Float, dtg: Long, label: Long): Unit =
      put(ensureCapacity(24), trackId, lat, lon, dtg, label)

    override def init(): Unit = {}

    override def aggregate(sf: SimpleFeature): Int = {
      val pos = buffer.position + overflow.position
      encoder.encode(sf, this)
      (buffer.position + overflow.position - pos) / binSize
    }

    override def encode(): Array[Byte] = {
      val bytes = try {
        if (overflow.position() > 0) {
          // overflow bytes - copy the two buffers into one
          val copy = Array.ofDim[Byte](buffer.position + overflow.position)
          System.arraycopy(buffer.array, 0, copy, 0, buffer.position)
          System.arraycopy(overflow.array, 0, copy, buffer.position, overflow.position)
          copy
        } else if (buffer.position == buffer.limit) {
          // use the existing buffer if possible
          buffer.array
        } else {
          // if not, we have to copy it - values do not allow you to specify a valid range
          val copy = Array.ofDim[Byte](buffer.position)
          System.arraycopy(buffer.array, 0, copy, 0, buffer.position)
          copy
        }
      } finally {
        buffer.clear()
        overflow.clear()
      }
      if (sort) {
        BinSorter.quickSort(bytes, 0, bytes.length - binSize, binSize)
      }
      bytes
    }

    override def cleanup(): Unit = {}

    private def ensureCapacity(size: Int): ByteBuffer = {
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
  }

  /**
    * Converts bin results to features
    *
    * @tparam T result type
    */
  abstract class BinResultsToFeatures[T] extends ResultsToFeatures[T] {

    override def init(state: Map[String, String]): Unit = {}

    override def state: Map[String, String] = Map.empty

    override def schema: SimpleFeatureType = BinaryOutputEncoder.BinEncodedSft

    override def apply(result: T): SimpleFeature =
      new ScalaSimpleFeature(BinaryOutputEncoder.BinEncodedSft, "", Array(bytes(result), GeometryUtils.zeroPoint))

    protected def bytes(result: T): Array[Byte]

    def canEqual(other: Any): Boolean = other.isInstanceOf[BinResultsToFeatures[T]]

    override def equals(other: Any): Boolean = other match {
      case that: BinResultsToFeatures[T] if that.canEqual(this) => true
      case _ => false
    }

    override def hashCode(): Int = schema.hashCode()
  }
}
