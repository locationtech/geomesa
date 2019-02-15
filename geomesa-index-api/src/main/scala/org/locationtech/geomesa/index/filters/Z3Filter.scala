/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.filters

import java.nio.ByteBuffer

import org.locationtech.geomesa.index.index.z3.Z3IndexValues
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.sfcurve.zorder.Z3

class Z3Filter(val xy: Array[Array[Int]], val t: Array[Array[Array[Int]]], val minEpoch: Short, val maxEpoch: Short) {

  def inBounds(buf: Array[Byte], offset: Int): Boolean = {
    val keyZ = ByteArrays.readLong(buf, offset + 2) // account for epoch - first 2 bytes
    pointInBounds(keyZ) && timeInBounds(ByteArrays.readShort(buf, offset), keyZ)
  }

  private def pointInBounds(z: Long): Boolean = {
    val x = Z3(z).d0
    val y = Z3(z).d1
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

  private def timeInBounds(epoch: Short, z: Long): Boolean = {
    // we know we're only going to scan appropriate epochs, so leave out whole epochs
    if (epoch > maxEpoch || epoch < minEpoch) { true } else {
      val tEpoch = t(epoch - minEpoch)
      if (tEpoch == null) { true } else {
        val time = Z3(z).d2
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

  override def toString: String = Z3Filter.serializeToStrings(this).toSeq.sortBy(_._1).mkString(",")
}

object Z3Filter {

  private val RangeSeparator = ":"
  private val TermSeparator  = ";"
  private val EpochSeparator = ","

  val XYKey     = "zxy"
  val TKey      = "zt"
  val EpochKey  = "epoch"

  def apply(values: Z3IndexValues): Z3Filter = {
    val Z3IndexValues(sfc, _, spatialBounds, _, temporalBounds, _) = values

    val xy: Array[Array[Int]] = spatialBounds.map { case (xmin, ymin, xmax, ymax) =>
      Array(sfc.lon.normalize(xmin), sfc.lat.normalize(ymin), sfc.lon.normalize(xmax), sfc.lat.normalize(ymax))
    }.toArray

    // we know we're only going to scan appropriate periods, so leave out whole ones
    val wholePeriod = Seq((sfc.time.min.toLong, sfc.time.max.toLong))
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
      (epoch, times.map { case (t1, t2) => Array(sfc.time.normalize(t1), sfc.time.normalize(t2)) }.toArray)
    }

    val t: Array[Array[Array[Int]]] =
      if (minEpoch == Short.MaxValue && maxEpoch == Short.MinValue) {
        Array.empty
      } else {
        Array.ofDim(maxEpoch - minEpoch + 1)
      }

    epochsAndTimes.foreach { case (w, times) => t(w - minEpoch) = times }

    new Z3Filter(xy, t, minEpoch, maxEpoch)
  }

  def serializeToBytes(filter: Z3Filter): Array[Byte] = {
    // 4 bytes for length plus 16 bytes for each xy val (4 ints)
    val xyLength = 4 + filter.xy.length * 16
    // 4 bytes for length, then per-epoch 4 bytes for length plus 8 bytes for each t val (2 ints)
    val tLength = 4 + filter.t.map(bounds => if (bounds == null) { 4 } else { 4 + bounds.length * 8 } ).sum
    // additional 4 bytes for min/max epoch
    val buffer = ByteBuffer.allocate(xyLength + tLength + 4)

    buffer.putInt(filter.xy.length)
    filter.xy.foreach(bounds => bounds.foreach(buffer.putInt))

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

  def deserializeFromBytes(serialized: Array[Byte]): Z3Filter = {
    val buffer = ByteBuffer.wrap(serialized)

    val xy = Array.fill(buffer.getInt())(Array.fill(4)(buffer.getInt))
    val t = Array.fill(buffer.getInt) {
      val length = buffer.getInt
      if (length == -1) { null } else {
        Array.fill(length)(Array.fill(2)(buffer.getInt))
      }
    }
    val minEpoch = buffer.getShort
    val maxEpoch = buffer.getShort

    new Z3Filter(xy, t, minEpoch, maxEpoch)
  }

  def serializeToStrings(filter: Z3Filter): Map[String, String] = {
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

  def deserializeFromStrings(serialized: scala.collection.Map[String, String]): Z3Filter = {
    val xy = serialized(XYKey).split(TermSeparator).map(_.split(RangeSeparator).map(_.toInt))
    val t = serialized(TKey).split(EpochSeparator).map { bounds =>
      if (bounds.isEmpty) { null } else {
        bounds.split(TermSeparator).map(_.split(RangeSeparator).map(_.toInt))
      }
    }
    val Array(minEpoch, maxEpoch) = serialized(EpochKey).split(RangeSeparator).map(_.toShort)

    new Z3Filter(xy, t, minEpoch, maxEpoch)
  }
}