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
import org.locationtech.geomesa.accumulo.data.tables.Z2Table
import org.locationtech.sfcurve.zorder.Z2

class Z2Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z2Iterator.{PointsKey, TableSharingKey, ZKey}

  var source: SortedKeyValueIterator[Key, Value] = null
  var zNums: Array[Int] = null

  var xmin: Int = -1
  var xmax: Int = -1
  var ymin: Int = -1
  var ymax: Int = -1

  var isPoints: Boolean = false
  var isTableSharing: Boolean = false
  var rowToLong: Array[Byte] => Long = null

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

    zNums = options.get(ZKey).split(":").map(_.toInt)
    xmin = zNums(0)
    xmax = zNums(1)
    ymin = zNums(2)
    ymax = zNums(3)

    // account for shard and table sharing bytes
    val offset = if (isTableSharing) 2 else 1
    val numBytes = if (isPoints) 8 else Z2Table.GEOM_Z_NUM_BYTES
    rowToLong = rowToLong(offset, numBytes)
  }

  override def next(): Unit = {
    source.next()
    findTop()
  }

  def findTop(): Unit = {
    topKey = null
    topValue = null
    while (source.hasTop && !inBounds(source.getTopKey, rowToLong)) {
      source.next()
    }
    if (source.hasTop) {
      topKey = source.getTopKey
      topValue = source.getTopValue
    }
  }

  private def inBounds(k: Key, getZ: (Array[Byte] => Long)): Boolean = {
    k.getRow(row)
    val bytes = row.getBytes
    val keyZ = getZ(bytes)
    val (x, y) = new Z2(keyZ).decode
    x >= xmin && x <= xmax && y >= ymin && y <= ymax
  }

  private def rowToLong(offset: Int, count: Int): (Array[Byte]) => Long = {
    count match {
      case 3 => (bb) => Longs.fromBytes(bb(offset), bb(offset + 1), bb(offset + 2), 0, 0, 0, 0, 0)
      case 4 => (bb) => Longs.fromBytes(bb(offset), bb(offset + 1), bb(offset + 2), bb(offset + 3), 0, 0, 0, 0)
      case 8 => (bb) => Longs.fromBytes(bb(offset), bb(offset + 1), bb(offset + 2), bb(offset + 3), bb(offset + 4),
                                        bb(offset + 5), bb(offset + 6), bb(offset + 7))
    }
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
    val iter = new Z2Iterator
    val opts = Map(PointsKey -> isPoints.toString, TableSharingKey -> isTableSharing.toString, ZKey -> zNums.mkString(":"))
    iter.init(source, opts, env)
    iter
  }
}

object Z2Iterator {

  val ZKey = "z"
  val PointsKey = "p"
  val TableSharingKey = "ts"

  def configure(isPoints: Boolean, tableSharing: Boolean, xmin: Int, xmax: Int, ymin: Int, ymax: Int, priority: Int) = {
    val is = new IteratorSetting(priority, "z2", classOf[Z2Iterator])
    is.addOption(PointsKey, isPoints.toString)
    is.addOption(ZKey, s"$xmin:$xmax:$ymin:$ymax")
    is.addOption(TableSharingKey, tableSharing.toString)
    is
  }
}
