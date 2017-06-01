/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data.{ByteSequence, Key, Value, Range => AccRange}
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.apache.hadoop.io.Text
import org.locationtech.geomesa.accumulo.index.legacy.z2.Z2IndexV1
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.index.filters.Z2Filter
import org.locationtech.sfcurve.zorder.Z2
import org.opengis.feature.simple.SimpleFeatureType

class Z2Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z2Iterator._

  var source: SortedKeyValueIterator[Key, Value] = _

  var keyXY: String = _
  var zOffset: Int = -1
  var zLength: Int = -1

  var xyvals: Array[Array[Int]] = _
  var filter: Z2Filter = _

  var topKey: Key = _
  var topValue: Value = _
  val row = new Text()

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    this.source = source

    zOffset = options.get(ZOffsetKey).toInt
    zLength = options.get(ZLengthKey).toInt

    keyXY = options.get(ZKeyXY)
    xyvals = keyXY.split(TermSeparator).map(_.split(RangeSeparator).map(_.toInt))
    filter = new Z2Filter(xyvals, zLength)
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
    filter.inBounds(row.getBytes, zOffset)
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
      ZOffsetKey -> zOffset.toString,
      ZLengthKey -> zLength.toString
    )
    val iter = new Z2Iterator
    iter.init(source.deepCopy(env), opts, env)
    iter
  }
}

object Z2Iterator {

  val ZKeyXY = "zxy"
  val ZOffsetKey = "zo"
  val ZLengthKey = "zl"

  private val RangeSeparator = ":"
  private val TermSeparator  = ";"

  def configure(sft: SimpleFeatureType,
                bounds: Seq[(Double, Double, Double, Double)],
                priority: Int): IteratorSetting = {

    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

    val is = new IteratorSetting(priority, "z2", classOf[Z2Iterator])

    // index space values for comparing in the iterator
    val xyOpts = if (sft.isPoints) {
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

    is.addOption(ZKeyXY, xyOpts.mkString(TermSeparator))
    // account for shard and table sharing bytes
    is.addOption(ZOffsetKey, if (sft.isTableSharing) { "2" } else { "1" })
    is.addOption(ZLengthKey, if (sft.isPoints) { "8" } else { Z2IndexV1.GEOM_Z_NUM_BYTES.toString })
    is
  }

  private def decodeNonPoints(x: Double, y: Double): (Int, Int) =
    Z2(Z2SFC.index(x, y).z & Z2IndexV1.GEOM_Z_MASK).decode
}
