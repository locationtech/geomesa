/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.google.common.primitives.{Longs, Shorts}
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Value, Range => AccRange}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.data.tables.Z3Table
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.sfcurve.zorder.Z3

class Z3Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z3Iterator._

  var source: SortedKeyValueIterator[Key, Value] = null

  var keyXY: Array[String] = null
  var keyT: Array[String] = null

  var xyvals: Array[(Int, Int, Int, Int)] = null
  var tvals: Array[Array[(Int, Int)]] = null

  var minWeek: Short = Short.MinValue
  var maxWeek: Short = Short.MinValue

  var isPoints: Boolean = false
  var hasSplits: Boolean = false
  var rowToWeekZ: Array[Byte] => (Short, Long) = null

  var topKey: Key = null
  var topValue: Value = null
  val row = new Text()

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)

    this.source = source.deepCopy(env)

    isPoints = options.get(PointsKey).toBoolean

    keyXY = options.get(ZKeyXY).split(RangeSeparator)
    keyT = options.get(ZKeyT).split(WeekSeparator).filterNot(_.isEmpty)

    xyvals = keyXY.map(_.toInt).grouped(4).map { case Array(x1, y1, x2, y2) => (x1, y1, x2, y2) }.toArray

    minWeek = Short.MinValue
    maxWeek = Short.MinValue

    val weeksAndTimes = keyT.map { times =>
      val parts = times.split(RangeSeparator)
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
      (week, parts.drop(1).grouped(2).map { case Array(t1, t2) => (t1.toInt, t2.toInt) }.toArray)
    }

    tvals = if (minWeek == Short.MinValue) Array.empty else Array.ofDim(maxWeek - minWeek + 1)
    weeksAndTimes.foreach { case (w, times) => tvals(w - minWeek) = times }

    hasSplits = options.get(SplitsKey).toBoolean
    val count = if (isPoints) 8 else Z3Table.GEOM_Z_NUM_BYTES
    rowToWeekZ = rowToWeekZ(count, hasSplits)
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
    val (week, keyZ) = rowToWeekZ(row.getBytes)
    timeInBounds(week, keyZ) && pointInBounds(keyZ)
  }

  private def pointInBounds(z: Long): Boolean = {
    val x = Z3(z).d0
    val y = Z3(z).d1
    var i = 0
    while (i < xyvals.length) {
      val (xmin, ymin, xmax, ymax) = xyvals(i)
      if (x >= xmin && x <= xmax && y >= ymin && y <= ymax) {
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
          val (tmin, tmax) = times(i)
          if (t >= tmin && t <= tmax) {
            return true
          }
          i += 1
        }
        false
      }
    }
  }

  private def rowToWeekZ(count: Int, splits: Boolean): (Array[Byte]) => (Short, Long) = {
    val zBytes = Longs.fromBytes _
    val wBytes = Shorts.fromBytes _
    (count, splits) match {
      case (3, true)  => (b) => (wBytes(b(1), b(2)), zBytes(b(3), b(4), b(5), 0, 0, 0, 0, 0))
      case (3, false) => (b) => (wBytes(b(0), b(1)), zBytes(b(2), b(3), b(4), 0, 0, 0, 0, 0))
      case (4, true)  => (b) => (wBytes(b(1), b(2)), zBytes(b(3), b(4), b(5), b(6), 0, 0, 0, 0))
      case (4, false) => (b) => (wBytes(b(0), b(1)), zBytes(b(2), b(3), b(4), b(5), 0, 0, 0, 0))
      case (8, true)  => (b) => (wBytes(b(1), b(2)), zBytes(b(3), b(4), b(5), b(6), b(7), b(8), b(9), b(10)))
      case (8, false) => (b) => (wBytes(b(0), b(1)), zBytes(b(2), b(3), b(4), b(5), b(6), b(7), b(8), b(9)))
      case _ => throw new IllegalArgumentException(s"Unhandled number of bytes for z value: $count")
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
      ZKeyXY    -> keyXY.mkString(RangeSeparator),
      ZKeyT     -> keyT.mkString(WeekSeparator),
      PointsKey -> isPoints.toString,
      SplitsKey -> hasSplits.toString
    )
    val iter = new Z3Iterator
    iter.init(source, opts, env)
    iter
  }
}

object Z3Iterator {

  val ZKeyXY = "zxy"
  val ZKeyT  = "zt"

  val PointsKey = "points"
  val SplitsKey = "splits"

  val RangeSeparator = ":"
  val WeekSeparator = ";"

  def configure(sfc: Z3SFC,
                bounds: Seq[(Double, Double, Double, Double)],
                timesByBin: Map[Short, Seq[(Long, Long)]],
                isPoints: Boolean,
                hasSplits: Boolean,
                priority: Int) = {

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
        s"$bin$RangeSeparator${time.mkString(RangeSeparator)}"
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
        s"$bin$RangeSeparator${time.mkString(RangeSeparator)}"
      }
      (normalized, tOpts)
    }

    is.addOption(ZKeyXY, xyOpts.mkString(RangeSeparator))
    is.addOption(ZKeyT, tOpts.mkString(WeekSeparator))
    is.addOption(PointsKey, isPoints.toString)
    is.addOption(SplitsKey, hasSplits.toString)

    is
  }

  private def decodeNonPoints(sfc: Z3SFC, x: Double, y: Double, t: Long): (Int, Int, Int) =
    Z3(sfc.index(x, y, t).z & Z3Table.GEOM_Z_MASK).decode
}