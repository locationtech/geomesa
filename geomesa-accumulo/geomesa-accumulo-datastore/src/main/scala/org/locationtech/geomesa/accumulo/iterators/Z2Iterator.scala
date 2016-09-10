/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import com.google.common.primitives.Longs
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Value, Range => AccRange}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.index.z2.Z2Index
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.sfcurve.zorder.Z2

class Z2Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z2Iterator._

  var source: SortedKeyValueIterator[Key, Value] = null

  var keyXY: Array[String] = null

  var xyvals: Array[(Int, Int, Int, Int)] = null

  var isPoints: Boolean = false
  var isTableSharing: Boolean = false
  var rowToZ: Array[Byte] => Long = null

  var topKey: Key = null
  var topValue: Value = null
  val row = new Text()

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    IteratorClassLoader.initClassLoader(getClass)

    this.source = source.deepCopy(env)

    isPoints = options.get(PointsKey).toBoolean
    isTableSharing = options.get(TableSharingKey).toBoolean

    keyXY = options.get(ZKeyXY).split(RangeSeparator)
    xyvals = keyXY.map(_.toInt).grouped(4).map { case Array(x1, y1, x2, y2) => (x1, y1, x2, y2) }.toArray

    // account for shard and table sharing bytes
    val numBytes = if (isPoints) 8 else Z2Index.GEOM_Z_NUM_BYTES
    rowToZ = rowToZ(numBytes, isTableSharing)
  }

  override def next(): Unit = {
    source.next()
    findTop()
  }

  def findTop(): Unit = {
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
    val x = Z2(keyZ).d0
    val y = Z2(keyZ).d1

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

  private def rowToZ(count: Int, tableSharing: Boolean): (Array[Byte]) => Long = (count, tableSharing) match {
    case (3, true)  => (b) => Longs.fromBytes(b(2), b(3), b(4), 0, 0, 0, 0, 0)
    case (3, false) => (b) => Longs.fromBytes(b(1), b(2), b(3), 0, 0, 0, 0, 0)
    case (4, true)  => (b) => Longs.fromBytes(b(2), b(3), b(4), b(5), 0, 0, 0, 0)
    case (4, false) => (b) => Longs.fromBytes(b(1), b(2), b(3), b(4), 0, 0, 0, 0)
    case (8, true)  => (b) => Longs.fromBytes(b(2), b(3), b(4), b(5), b(6), b(7), b(8), b(9))
    case (8, false) => (b) => Longs.fromBytes(b(1), b(2), b(3), b(4), b(5), b(6), b(7), b(8))
    case _ => throw new IllegalArgumentException(s"Unhandled number of bytes for z value: $count")
  }

  override def getTopValue: Value = topValue
  override def getTopKey: Key = topKey
  override def hasTop: Boolean = topKey != null

  override def seek(range: AccRange, columnFamilies: java.util.Collection[ByteSequence], inclusive: Boolean): Unit = {
    source.seek(range, columnFamilies, inclusive)
    findTop()
  }

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = {
    import scala.collection.JavaConversions._
    val opts = Map(
      ZKeyXY          -> keyXY.mkString(RangeSeparator),
      PointsKey       -> isPoints.toString,
      TableSharingKey -> isTableSharing.toString
    )
    val iter = new Z2Iterator
    iter.init(source, opts, env)
    iter
  }
}

object Z2Iterator {

  val ZKeyXY = "zxy"
  val PointsKey = "points"
  val TableSharingKey = "table-sharing"

  val RangeSeparator = ":"

  def configure(bounds: Seq[(Double, Double, Double, Double)],
                isPoints: Boolean,
                tableSharing: Boolean,
                priority: Int) = {

    val is = new IteratorSetting(priority, "z2", classOf[Z2Iterator])

    // index space values for comparing in the iterator
    val xyOpts = if (isPoints) {
      bounds.map { case (xmin, ymin, xmax, ymax) =>
        s"${Z2SFC.lon.normalize(xmin)}$RangeSeparator${Z2SFC.lat.normalize(ymin)}$RangeSeparator" +
            s"${Z2SFC.lon.normalize(xmax)}$RangeSeparator${Z2SFC.lat.normalize(ymax)}"
      }
    } else {
      bounds.map { case (xmin, ymin, xmax, ymax) =>
        val (lx, ly) = decodeNonPoints(xmin, ymin)
        val (ux, uy) = decodeNonPoints(xmax, ymax)
        s"$lx$RangeSeparator$ly$RangeSeparator$ux$RangeSeparator$uy"
      }
    }

    is.addOption(ZKeyXY, xyOpts.mkString(RangeSeparator))
    is.addOption(PointsKey, isPoints.toString)
    is.addOption(TableSharingKey, tableSharing.toString)
    is
  }

  private def decodeNonPoints(x: Double, y: Double): (Int, Int) =
    Z2(Z2SFC.index(x, y).z & Z2Index.GEOM_Z_MASK).decode
}
