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
import org.locationtech.geomesa.accumulo.index.legacy.z3.Z3IndexV2
import org.locationtech.geomesa.curve.Z3SFC
import org.locationtech.geomesa.index.filters.Z3Filter
import org.locationtech.sfcurve.zorder.Z3


class Z3Iterator extends SortedKeyValueIterator[Key, Value] {

  import org.locationtech.geomesa.accumulo.iterators.Z3Iterator._

  private var source: SortedKeyValueIterator[Key, Value] = _

  private var keyXY: String = _
  private var keyT: String = _
  private var filter: Z3Filter = _
  private var zOffset: Int = -1

  private var topKey: Key = _
  private var topValue: Value = _

  private val row = new Text()

  override def init(source: SortedKeyValueIterator[Key, Value],
                    options: java.util.Map[String, String],
                    env: IteratorEnvironment): Unit = {
    this.source = source

    var minEpoch: Short = Short.MinValue
    var maxEpoch: Short = Short.MinValue
    keyT = options.get(ZKeyT)
    val epochsAndTimes = keyT.split(EpochSeparator).filterNot(_.isEmpty).map { times =>
      val parts = times.split(TermSeparator)
      val epoch = parts(0).toShort
      // set min/max epochs - note: side effect in map
      if (minEpoch == Short.MinValue) {
        minEpoch = epoch
        maxEpoch = epoch
      } else if (epoch < minEpoch) {
        minEpoch = epoch
      } else if (epoch > maxEpoch) {
        maxEpoch = epoch
      }
      (epoch, parts.drop(1).map(_.split(RangeSeparator).map(_.toInt)))
    }

    val tvals: Array[Array[Array[Int]]] =
      if (minEpoch == Short.MinValue) Array.empty else Array.ofDim(maxEpoch - minEpoch + 1)
    epochsAndTimes.foreach { case (w, times) => tvals(w - minEpoch) = times }

    keyXY = options.get(ZKeyXY)
    val xyvals = keyXY.split(TermSeparator).map(_.split(RangeSeparator).map(_.toInt))
    val zLength = options.get(ZLengthKey).toInt
    zOffset = options.get(ZOffsetKey).toInt
    filter = new Z3Filter(xyvals, tvals, minEpoch, maxEpoch, zLength)
  }

  override def next(): Unit = {
    source.next()
    findTop()
  }

  private def findTop(): Unit = {
    topKey = null
    topValue = null
    while (source.hasTop) {
      source.getTopKey.getRow(row)
      if (filter.inBounds(row.getBytes, zOffset)) {
        topKey = source.getTopKey
        topValue = source.getTopValue
        return
      } else {
        source.next()
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
      ZLengthKey -> filter.zLength.toString
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
  val EpochSeparator  = ","

  def configure(sfc: Z3SFC,
                bounds: Seq[(Double, Double, Double, Double)],
                timesByBin: Map[Short, Seq[(Long, Long)]],
                isPoints: Boolean,
                hasSplits: Boolean,
                isSharing: Boolean,
                priority: Int): IteratorSetting = {

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
    is.addOption(ZKeyT, tOpts.mkString(EpochSeparator))
    is.addOption(ZOffsetKey, if (isSharing && hasSplits) { "2" } else if (isSharing || hasSplits) { "1" } else { "0" })
    is.addOption(ZLengthKey, if (isPoints) { "8" } else { Z3IndexV2.GEOM_Z_NUM_BYTES.toString })

    is
  }

  private def decodeNonPoints(sfc: Z3SFC, x: Double, y: Double, t: Long): (Int, Int, Int) =
    Z3(sfc.index(x, y, t).z & Z3IndexV2.GEOM_Z_MASK).decode


}