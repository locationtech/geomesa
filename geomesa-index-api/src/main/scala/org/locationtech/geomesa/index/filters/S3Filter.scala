/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.filters

import java.nio.ByteBuffer

import com.google.common.geometry.S2CellId
import org.locationtech.geomesa.index.filters.RowFilter.RowFilterFactory
import org.locationtech.geomesa.index.index.s3.S3IndexValues
import org.locationtech.geomesa.utils.index.ByteArrays

/**
  * @author sunyabo 2019年08月01日 09:26
  * @version V1.0
  */
class S3Filter(
    val xy: Array[Array[Double]],
    val t: Array[Array[Array[Int]]],
    val minEpoch: Short,
    val maxEpoch: Short
  ) extends RowFilter {

  /**
    * Determine if the point is inside the rectangle
    * @param buf row
    * @param offset offset
    * @return
    */
  override def inBounds(buf: Array[Byte], offset: Int): Boolean = {
    // account for epoch - first 2 bytes
    val keyS = ByteArrays.readLong(buf, offset + 2)
    val timeOffset = ByteArrays.readInt(buf, offset + 10)
    pointInBounds(keyS) && timeInBounds(ByteArrays.readShort(buf, offset), timeOffset)
  }

  private def pointInBounds(s3Values: Long): Boolean = {
    val s2CellId = new S2CellId(s3Values)
    val x = s2CellId.toLatLng.lngDegrees()
    val y = s2CellId.toLatLng.latDegrees()
    var i = 0
    while (i < xy.length) {
      val xyi = xy(i)
      if (x >= xyi(0) && x <= xyi(2) && y >= xyi(1) && y <= xyi(3)) {
        return true
      }
      i += 1
    }
    false
  }

  private def timeInBounds(epoch: Short, offset: Int): Boolean = {
    // we know we're only going to scan appropriate epochs, so leave out whole epochs
    if (epoch > maxEpoch || epoch < minEpoch) { false } else {
      val tEpoch = t(epoch - minEpoch)
      if (tEpoch == null) { true } else {
        val time = offset
        var i = 0
        while (i < tEpoch.length) {
          val ti = tEpoch(i)
          if (time >= ti(0) && time <= ti(1)) {
            return true
          }
          i += 1
        }
        false
      }
    }
  }

  override def toString: String = S3Filter.serializeToStrings(this).toSeq.sortBy(_._1).mkString(",")
}

object S3Filter extends RowFilterFactory[S3Filter] {

  private val RangeSeparator = ":"
  private val TermSeparator  = ";"
  private val EpochSeparator = ","

  val XYKey     = "sxy"
  val TKey      = "st"
  val EpochKey  = "epoch"

  def apply(values: S3IndexValues): S3Filter = {
    val S3IndexValues(_, maxTime, _, spatialBounds, _, temporalBounds, _) = values

    val xy: Array[Array[Double]] = spatialBounds.map { case (xmin, ymin, xmax, ymax) =>
      Array(xmin, ymin, xmax, ymax)
    }.toArray

    // we know we're only going to scan appropriate periods, so leave out whole ones
    val wholePeriod = Seq((0L, maxTime))
    var minEpoch: Short = Short.MaxValue
    var maxEpoch: Short = Short.MinValue
    val epochsAndTimes = temporalBounds.toSeq.filter(_._2 != wholePeriod).sortBy(_._1).map { case (epoch, times) =>
      // set min/max epochs - note: side effect in map
      if (epoch < minEpoch) {
        minEpoch = epoch
      }
      if (epoch > maxEpoch) {
        maxEpoch = epoch
      }
      (epoch, times.map { case (t1, t2) => Array(t1, t2)}.toArray)
    }

    val t: Array[Array[Array[Int]]] =
      if (minEpoch == Short.MaxValue && maxEpoch == Short.MinValue) {
        Array.empty
      } else {
        Array.ofDim(maxEpoch - minEpoch + 1)
      }

    epochsAndTimes.foreach { case (w, times) => t(w - minEpoch) = times }

    new S3Filter(xy, t, minEpoch, maxEpoch)
  }

  override def serializeToBytes(filter: S3Filter): Array[Byte] = {
    // 4 bytes for length plus 32 bytes for each xy val (4 doubles)
    val xyLength = 4 + filter.xy.length * 32
    // 4 bytes for length, then per-epoch 4 bytes for length plus 8 bytes for each t val (2 ints)
    val tLength = 4 + filter.t.map(bounds => if (bounds == null) { 4 } else { 4 + bounds.length * 8 } ).sum
    // additional 4 bytes for min/max epoch
    val buffer = ByteBuffer.allocate(xyLength + tLength + 4)

    buffer.putInt(filter.xy.length)
    filter.xy.foreach(bounds => bounds.foreach(buffer.putDouble))
    buffer.putInt(filter.t.length)
    filter.t.foreach { bounds =>
      if (bounds == null) {
        buffer.putInt(-1)
      } else {
        buffer.putInt(bounds.length)
        bounds.foreach(inner => inner.foreach(buffer.putInt))
      }
    }

    buffer.putShort(filter.minEpoch)
    buffer.putShort(filter.maxEpoch)

    buffer.array()
  }

  override def deserializeFromBytes(serialized: Array[Byte]): S3Filter = {
    val buffer = ByteBuffer.wrap(serialized)

    val xy = Array.fill(buffer.getInt())(Array.fill(4)(buffer.getDouble()))
    val t = Array.fill(buffer.getInt) {
      val length = buffer.getInt
      if (length == -1) { null } else {
        Array.fill(length)(Array.fill(2)(buffer.getInt()))
      }
    }
    val minEpoch = buffer.getShort
    val maxEpoch = buffer.getShort

    new S3Filter(xy, t, minEpoch, maxEpoch)
  }

  override def serializeToStrings(filter: S3Filter): Map[String, String] = {
    val xy = filter.xy.map(bounds => bounds.mkString(RangeSeparator)).mkString(TermSeparator)
    val t = filter.t.map { bounds =>
      if (bounds == null) { "" } else {
        bounds.map(_.mkString(RangeSeparator)).mkString(TermSeparator)
      }
    }.mkString(EpochSeparator)
    val epoch = s"${filter.minEpoch}$RangeSeparator${filter.maxEpoch}"

    Map(
      XYKey    -> xy,
      TKey     -> t,
      EpochKey -> epoch
    )
  }

  override def deserializeFromStrings(serialized: scala.collection.Map[String, String]): S3Filter = {
    val xy = serialized(XYKey).split(TermSeparator).map(_.split(RangeSeparator).map(_.toDouble))
    val t = serialized(TKey).split(EpochSeparator).map { bounds =>
      if (bounds.isEmpty) { null } else {
        bounds.split(TermSeparator).map(_.split(RangeSeparator).map(_.toInt))
      }
    }
    val Array(minEpoch, maxEpoch) = serialized(EpochKey).split(RangeSeparator).map(_.toShort)

    new S3Filter(xy, t, minEpoch, maxEpoch)
  }
}
