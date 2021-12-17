/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.filters

import com.typesafe.scalalogging.StrictLogging
import org.geotools.util.factory.Hints
import org.locationtech.geomesa.index.filters.RowFilter.{FilterResult, RowFilterFactory}
import org.locationtech.geomesa.index.filters.Z3Filter.ZBounds
import org.locationtech.geomesa.index.index.z3.Z3IndexValues
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.sfcurve.zorder.Z3

import java.nio.ByteBuffer

/**
 * Z3 filter implementation
 *
 * @param xy spatial bounds
 *           <ul><li>each element of the outer array is a bounding box</li>
 *           <li>the inner arrays wil always have 4 elements, consisting of (xmin,ymin,xmax,ymax)</li></ul>
 *           for example, `bbox(0,1,2,3) OR bbox(4,5,6,7)` would be `Array(Array(0,1,2,3), Array(4,5,6,7))`
 *
 * @param t temporal bounds
 *          <ul><li>each element of the outer array is the times for a particular epoch, indexed by (epoch - epochOffset)
 *            if the epoch is < epochOffset or > t.length + epochOffset, or the element corresponding to the epoch is null, then
 *            the entire epoch is included in the time filter</li>
 *          <li>each element of the second array is a time bound within the given epoch</li>
 *          <li>the inner arrays will always have 2 elements, consisting of (min time, max time)</li>
 *          <li>the outer array may be empty, which indicates all epochs are fully included in the time filter</li></ul>
 *          for example, `during t1/t2 OR during t3/t4` would be `Array(Array(Array(tz1,tz2), Array(tz3,tz4)))`
 *            (assuming a single epoch). tz refers to the time offset within the epoch
 *
 * @param epochOffset offset into the temporal bounds array
 * @param maxTime max time for our space-filling curve
 * @param seek whether to seek or not
 */
class Z3Filter(
    val xy: Array[Array[Int]],
    val t: Array[Array[Array[Int]]],
    val epochOffset: Short,
    val maxTime: Int,
    val seek: Boolean
  ) extends RowFilter with StrictLogging {

  private val zBoundsNoTime: ZBounds = if (!seek) { null } else {
    val bounds = xy.map { case Array(xmin, ymin, xmax, ymax) =>
      Z3Filter.zBound(xmin, ymin, 0, xmax, ymax, maxTime)
    }
    java.util.Arrays.sort(bounds, Z3Filter.ZBoundsOrdering)
    ZBounds(bounds, bounds.head.head)
  }

  private val zBounds: Array[ZBounds] = if (!seek) { null } else {
    t.map {
      case null => zBoundsNoTime
      case ti =>
        val bounds = ti.flatMap { case Array(tmin, tmax) =>
          xy.map { case Array(xmin, ymin, xmax, ymax) =>
            Z3Filter.zBound(xmin, ymin, tmin, xmax, ymax, tmax)
          }
        }
        java.util.Arrays.sort(bounds, Z3Filter.ZBoundsOrdering)
        ZBounds(bounds, bounds.head.head)
    }
  }

  override def filter(row: Array[Byte], offset: Int): FilterResult = {
    // row consists of 2-byte epoch + 8 byte z value
    val epoch = ByteArrays.readShort(row, offset)
    val keyZ = ByteArrays.readLong(row, offset + 2)
    if (pointInBounds(keyZ) && timeInBounds(epoch, keyZ)) {
      FilterResult.InBounds
    } else if (seek) {
      nextJumpIn(epoch, keyZ)
    } else {
      FilterResult.OutOfBounds
    }
  }

  // noinspection ScalaDeprecation
  override def inBounds(buf: Array[Byte], offset: Int): Boolean = {
    filter(buf, offset) match {
      case FilterResult.InBounds => true
      case _ => false
    }
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
    val iEpoch = epoch - epochOffset
    // we know we're only going to scan appropriate epochs, so leave out whole epochs
    if (iEpoch < 0 || iEpoch >= t.length) { true } else {
      val tEpoch = t(iEpoch)
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

  private def nextJumpIn(epoch: Short, z: Long): FilterResult = {
    val epochIndex = epoch - epochOffset
    val zb = if (epochIndex < 0 || epochIndex >= zBounds.length) { zBoundsNoTime } else { zBounds(epochIndex) }

    var nextZ = Long.MaxValue

    var i = 0
    while (i < zb.bounds.length) {
      val Array(zmin, zmax) = zb.bounds(i)
      if (z < zmin) {
        if (zmin < nextZ) {
          nextZ = zmin
        }
      } else if (z < zmax) {
        val next = Z3.zdivide(z, zmin, zmax)._2
        if (next < nextZ) {
          nextZ = next
        }
      }
      i += 1
    }

    var nextEpoch = epoch

    if (nextZ == Long.MaxValue && epoch != Short.MaxValue) {
      val nextEpochIndex = epochIndex + 1
      if (nextEpochIndex < 0 || nextEpochIndex >= zBounds.length) {
        nextZ = zBoundsNoTime.min
      } else {
        nextZ = zBounds(nextEpochIndex).min
      }
      nextEpoch = (epoch + 1).toShort
    }

    logger.trace(s"Seeking ahead from $epoch:$z to $nextEpoch:$nextZ")
    FilterResult.SkipAhead(ByteArrays.toBytes(nextEpoch, nextZ))
  }

  override def toString: String = Z3Filter.serializeToStrings(this).toSeq.sortBy(_._1).mkString(",")
}

object Z3Filter extends RowFilterFactory[Z3Filter] {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  private val ZBoundsOrdering = Ordering.by[Array[Long], Long](_.head)

  private val RangeSeparator = ":"
  private val TermSeparator  = ";"
  private val EpochSeparator = ","

  val VersionKey = "v"
  val XYKey      = "zxy"
  val TKey       = "zt"
  val EpochKey   = "epoch"
  val MaxTimeKey = "mt"
  val SeekKey    = "s"


  // note: we use a negative version number to ensure that we error out with a negative length if an older version
  // of the filter tries to deserialize a newer serialized buffer. This was verified with the code snippet below
  val Version: Byte = -1

  //  val buf = ByteBuffer.allocate(5)
  //  val read = ByteBuffer.wrap(buf.array())
  //  (-9 to -1).map(_.toByte).foreach { version =>
  //    buf.clear()
  //    buf.put(version).mark()
  //    (0 to 10000).foreach { i =>
  //      buf.reset()
  //      buf.putInt(i)
  //      read.clear()
  //      read.getInt must beLessThan(0)
  //      read.position(1)
  //      read.get() must beGreaterThanOrEqualTo(0.toByte)
  //    }
  //  }

  def apply(values: Z3IndexValues): Z3Filter = apply(values, seek = true)

  def apply(values: Z3IndexValues, hints: Hints): Z3Filter = apply(values, hints.isScanSeeking)

  private def apply(values: Z3IndexValues, seek: Boolean): Z3Filter = {
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

    // note: disable seek if we have no valid dimensions (degenerate scan case)
    new Z3Filter(xy, t, minEpoch, sfc.time.normalize(wholePeriod.head._2), seek && xy.nonEmpty)
  }

  override def serializeToBytes(filter: Z3Filter): Array[Byte] = {
    // 4 bytes for length plus 16 bytes for each xy val (4 ints)
    val xyLength = 4 + filter.xy.length * 16
    // 4 bytes for length, then per-epoch 4 bytes for length plus 8 bytes for each t val (2 ints)
    val tLength = 4 + filter.t.map(bounds => if (bounds == null) { 4 } else { 4 + bounds.length * 8 } ).sum
    // additional 1 byte for version, 2 bytes for epoch offset, 4 bytes for max time, 1 byte for seek
    val buffer = ByteBuffer.allocate(xyLength + tLength + 8)

    buffer.put(Version)
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

    buffer.putShort(filter.epochOffset)
    buffer.putInt(filter.maxTime)
    buffer.put(if (filter.seek) { 1.toByte } else { 0.toByte })

    buffer.array()
  }

  override def deserializeFromBytes(serialized: Array[Byte]): Z3Filter = {
    val buffer = ByteBuffer.wrap(serialized)

    val version = buffer.get()
    val maxTimeOpt = version match {
      case -1 =>
        None

      case v if v > 0 =>
        // back compatibility with older clients
        // reset to start of buffer and set the max time, which isn't in the buffer
        buffer.clear()
        Some(0)

      case _ =>
        throw new IllegalArgumentException("Unexpected serialized filter")
    }

    val xy = Array.fill(buffer.getInt())(Array.fill(4)(buffer.getInt))
    val t = Array.fill(buffer.getInt) {
      val length = buffer.getInt
      if (length == -1) { null } else {
        Array.fill(length)(Array.fill(2)(buffer.getInt))
      }
    }
    val epochOffset = buffer.getShort
    val maxTime = maxTimeOpt.getOrElse(buffer.getInt)
    val seek = if (maxTimeOpt.isEmpty) { buffer.get > 0 } else { false }

    new Z3Filter(xy, t, epochOffset, maxTime, seek)
  }

  override def serializeToStrings(filter: Z3Filter): Map[String, String] = {
    val xy = filter.xy.map(bounds => bounds.mkString(RangeSeparator)).mkString(TermSeparator)
    val t = filter.t.map { bounds =>
      if (bounds == null) { "" } else {
        bounds.map(_.mkString(RangeSeparator)).mkString(TermSeparator)
      }
    }.mkString(EpochSeparator)

    Map(
      VersionKey -> java.lang.Byte.toString(Version),
      XYKey      -> xy,
      TKey       -> t,
      EpochKey   -> java.lang.Short.toString(filter.epochOffset),
      MaxTimeKey -> Integer.toString(filter.maxTime),
      SeekKey    -> java.lang.Boolean.toString(filter.seek)
    )
  }

  override def deserializeFromStrings(serialized: scala.collection.Map[String, String]): Z3Filter = {
    val (maxTime, epochOffset, seek) = serialized.get(VersionKey) match {
      case Some("-1") => (serialized(MaxTimeKey).toInt, serialized(EpochKey).toShort, serialized(SeekKey).toBoolean)
      case None       => (0, serialized(EpochKey).split(RangeSeparator).head.toShort, false)
      case Some(v)    => throw new IllegalArgumentException(s"Unexpected serialized filter version $v")
    }
    val xy = serialized(XYKey).split(TermSeparator).map(_.split(RangeSeparator).map(_.toInt))
    val t = serialized(TKey).split(EpochSeparator).map { bounds =>
      if (bounds.isEmpty) { null } else {
        bounds.split(TermSeparator).map(_.split(RangeSeparator).map(_.toInt))
      }
    }

    new Z3Filter(xy, t, epochOffset, maxTime, seek)
  }

  /**
   * Gets z-low and z-hi for a bounding box in normalized space
   */
  private def zBound(xmin: Int, ymin: Int, tmin: Int, xmax: Int, ymax: Int, tmax: Int): Array[Long] =
    Array(Z3(xmin, ymin, tmin).z, Z3(xmax, ymax, tmax).z)

  /**
   * Holds z values for seeking in a given epoch
   *
   * @param bounds the inner array is tuples of min/max z values for valid scan ranges,
   *               the outer array is to hold all the valid tuples
   * @param min the absolute min z value in the bounds, used when seeking from a previous epoch
   */
  private case class ZBounds(bounds: Array[Array[Long]], min: Long)
}