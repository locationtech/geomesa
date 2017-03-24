/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.google.common.primitives.{Longs, Shorts}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Value, Range => AccRange}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.index.legacy.z3.Z3IndexV2
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.sfcurve.zorder.Z3
import org.opengis.feature.simple.SimpleFeatureType

class Z3Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z3Iterator._

  private var source: SortedKeyValueIterator[Key, Value] = null

  private var keyXY: String = null
  private var keyT: String = null

  private var xyvals: Array[Array[Int]] = null
  private var tvals: Array[Array[Array[Int]]] = null

  private var minWeek: Short = Short.MinValue
  private var maxWeek: Short = Short.MinValue

  private var zOffset: Int = -1
  private var zLength: Int = -1
  private var rowToWeek: Array[Byte] => Short = null
  private var rowToZ: Array[Byte] => Long = null

  private var topKey: Key = null
  private var topValue: Value = null
  private val row = new Text()

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    this.source = source

    zOffset = options.get(ZOffsetKey).toInt
    zLength = options.get(ZLengthKey).toInt

    rowToWeek = getRowToWeek(zOffset)
    rowToZ    = getRowToZ(zOffset, zLength)

    keyXY = options.get(ZKeyXY)
    keyT  = options.get(ZKeyT)

    xyvals = keyXY.split(TermSeparator).map(_.split(RangeSeparator).map(_.toInt))

    minWeek = Short.MinValue
    maxWeek = Short.MinValue

    val weeksAndTimes = keyT.split(WeekSeparator).filterNot(_.isEmpty).map { times =>
      val parts = times.split(TermSeparator)
      val week = parts(0).toShort
      // set min/max weeks - note: side effect in map
      if (minWeek == Short.MinValue) {
        minWeek = week
        maxWeek = week
      } else if (week < minWeek) {
        minWeek = week
      } else if (week > maxWeek) {
        maxWeek = week
      }
      (week, parts.drop(1).map(_.split(RangeSeparator).map(_.toInt)))
    }

    tvals = if (minWeek == Short.MinValue) Array.empty else Array.ofDim(maxWeek - minWeek + 1)
    weeksAndTimes.foreach { case (w, times) => tvals(w - minWeek) = times }
  }

  override def next(): Unit = {
    source.next()
    findTop()
  }

  private def findTop(): Unit = {
    topKey = null
    topValue = null
    while (source.hasTop) {
      if (inBounds(source.getTopKey)) {
        topKey = source.getTopKey
        topValue = source.getTopValue
        return
      } else {
        source.next()
      }
    }
  }

  private def inBounds(k: Key): Boolean = {
    k.getRow(row)
    val keyZ = rowToZ(row.getBytes)
    pointInBounds(keyZ) && timeInBounds(rowToWeek(row.getBytes), keyZ)
  }

  private def pointInBounds(z: Long): Boolean = {
    val x = Z3(z).d0
    val y = Z3(z).d1
    var i = 0
    while (i < xyvals.length) {
      val xy = xyvals(i)
      if (x >= xy(0) && x <= xy(2) && y >= xy(1) && y <= xy(3)) {
        return true
      }
      i += 1
    }
    false
  }

  private def timeInBounds(week: Short, z: Long): Boolean = {
    // we know we're only going to scan appropriate weeks, so leave out whole weeks
    if (week > maxWeek || week < minWeek) { true } else {
      val times = tvals(week - minWeek)
      if (times == null) { true } else {
        val t = Z3(z).d2
        var i = 0
        while (i < times.length) {
          val time = times(i)
          if (t >= time(0) && t <= time(1)) {
            return true
          }
          i += 1
        }
        false
      }
    }
  }

  override def seek(range: AccRange, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean): Unit = {
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def getTopValue: Value = topValue
  override def getTopKey: Key = topKey
  override def hasTop: Boolean = topKey != null

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    import scala.collection.JavaConversions._
    val opts = Map(
      ZKeyXY     -> keyXY,
      ZKeyT      -> keyT,
      ZOffsetKey -> zOffset.toString,
      ZLengthKey -> zLength.toString
    )
    val iter = new Z3Iterator
    iter.init(source.deepCopy(env), opts, env)
    iter
  }
}

object Z3Iterator {

  val ZKeyXY = "zxy"
  val ZKeyT  = "zt"

  val ZOffsetKey = "zo"
  val ZLengthKey = "zl"

  val RangeSeparator = ":"
  val TermSeparator  = ";"
  val WeekSeparator  = ","

  def configure(sfc: Z3SFC,
                bounds: Seq[(Double, Double, Double, Double)],
                timesByBin: Map[Short, Seq[(Long, Long)]],
                isPoints: Boolean,
                hasSplits: Boolean,
                isSharing: Boolean,
                priority: Int): IteratorSetting = {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val is = new IteratorSetting(priority, "z3", classOf[Z3Iterator])

    // index space values for comparing in the iterator
    val (xyOpts, tOpts) = if (isPoints) {
      val xyOpts = bounds.map { case (xmin, ymin, xmax, ymax) =>
        s"${sfc.lon.normalize(xmin)}$RangeSeparator${sfc.lat.normalize(ymin)}$RangeSeparator" +
            s"${sfc.lon.normalize(xmax)}$RangeSeparator${sfc.lat.normalize(ymax)}"
      }
      val tOpts = timesByBin.toSeq.sortBy(_._1).map { case (bin, times) =>
        val time = times.map { case (t1, t2) =>
          s"${sfc.time.normalize(t1)}$RangeSeparator${sfc.time.normalize(t2)}"
        }
        s"$bin$TermSeparator${time.mkString(TermSeparator)}"
      }
      (xyOpts, tOpts)
    } else {
      val normalized = bounds.map { case (xmin, ymin, xmax, ymax) =>
        val (lx, ly, _) = decodeNonPoints(sfc, xmin, ymin, 0) // note: time is not used
        val (ux, uy, _) = decodeNonPoints(sfc, xmax, ymax, 0) // note: time is not used
        s"$lx$RangeSeparator$ly$RangeSeparator$ux$RangeSeparator$uy"
      }
      val tOpts = timesByBin.toSeq.sortBy(_._1).map { case (bin, times) =>
        val time = times.map { case (t1, t2) =>
          s"${decodeNonPoints(sfc, 0, 0, t1)._3}$RangeSeparator${decodeNonPoints(sfc, 0, 0, t2)._3}"
        }
        s"$bin$TermSeparator${time.mkString(TermSeparator)}"
      }
      (normalized, tOpts)
    }

    is.addOption(ZKeyXY, xyOpts.mkString(TermSeparator))
    is.addOption(ZKeyT, tOpts.mkString(WeekSeparator))
    is.addOption(ZOffsetKey, if (isSharing && hasSplits) { "2" } else if (isSharing || hasSplits) { "1" } else { "0" })
    is.addOption(ZLengthKey, if (isPoints) { "8" } else { Z3IndexV2.GEOM_Z_NUM_BYTES.toString })

    is
  }

  private def decodeNonPoints(sfc: Z3SFC, x: Double, y: Double, t: Long): (Int, Int, Int) =
    Z3(sfc.index(x, y, t).z & Z3IndexV2.GEOM_Z_MASK).decode

  private def getRowToZ(offset: Int, zLength: Int): (Array[Byte]) => Long = {
    // account for week - first 2 bytes
    val z0 = offset + 2
    val z1 = offset + 3
    val z2 = offset + 4
    val z3 = offset + 5
    val z4 = offset + 6
    val z5 = offset + 7
    val z6 = offset + 8
    val z7 = offset + 9

    if (zLength == 8) {
      (b) => Longs.fromBytes(b(z0), b(z1), b(z2), b(z3), b(z4), b(z5), b(z6), b(z7))
    } else if (zLength == 3) {
      (b) => Longs.fromBytes(b(z0), b(z1), b(z2), 0, 0, 0, 0, 0)
    } else if (zLength == 4) {
      (b) => Longs.fromBytes(b(z0), b(z1), b(z2), b(z3), 0, 0, 0, 0)
    } else {
      throw new IllegalArgumentException(s"Unhandled number of bytes for z value: $zLength")
    }
  }

  private def getRowToWeek(offset: Int): (Array[Byte]) => Short = {
    val w0 = offset
    val w1 = offset + 1
    (b) => Shorts.fromBytes(b(w0), b(w1))
  }
}