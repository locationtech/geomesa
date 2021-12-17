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
import org.locationtech.geomesa.index.index.z2.Z2IndexValues
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.sfcurve.zorder.Z2

import java.nio.ByteBuffer

class Z2Filter(val xy: Array[Array[Int]], val seek: Boolean) extends RowFilter with StrictLogging {

  private val zBounds: Array[Array[Long]] = Z2Filter.zBounds(xy)

  override def filter(row: Array[Byte], offset: Int): FilterResult = {
    val z = ByteArrays.readLong(row, offset)
    val x = Z2(z).d0
    val y = Z2(z).d1
    var i = 0
    while (i < xy.length) {
      val xyi = xy(i)
      if (x >= xyi(0) && x <= xyi(2) && y >= xyi(1) && y <= xyi(3)) {
        return FilterResult.InBounds
      }
      i += 1
    }

    if (seek) {
      nextJumpIn(z)
    } else {
      FilterResult.OutOfBounds
    }
  }

  // noinspection ScalaDeprecation
  override def inBounds(row: Array[Byte], offset: Int): Boolean = {
    filter(row, offset) match {
      case FilterResult.InBounds => true
      case _ => false
    }
  }

  private def nextJumpIn(z: Long): FilterResult = {
    var nextZ = Long.MaxValue

    var i = 0
    while (i < zBounds.length) {
      val Array(zmin, zmax) = zBounds(i)
      if (z < zmin) {
        if (zmin < nextZ) {
          nextZ = zmin
        }
      } else if (z < zmax) {
        val next = Z2.zdivide(z, zmin, zmax)._2
        if (next < nextZ) {
          nextZ = next
        }
      }
      i += 1
    }

    logger.trace(s"Seeking ahead from $z to $nextZ")
    FilterResult.SkipAhead(ByteArrays.toBytes(nextZ))
  }

  override def toString: String = Z2Filter.serializeToStrings(this).toSeq.sortBy(_._1).mkString(",")
}

object Z2Filter extends RowFilterFactory[Z2Filter] {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints

  private val ZBoundsOrdering = Ordering.by[Array[Long], Long](_.head)

  private val RangeSeparator = ":"
  private val TermSeparator  = ";"

  def apply(values: Z2IndexValues): Z2Filter = apply(values, seek = true)

  def apply(values: Z2IndexValues, hints: Hints): Z2Filter = apply(values, hints.isScanSeeking)

  private def apply(values: Z2IndexValues, seek: Boolean): Z2Filter = {
    val sfc = values.sfc
    val xy: Array[Array[Int]] = values.spatialBounds.map { case (xmin, ymin, xmax, ymax) =>
      Array(sfc.lon.normalize(xmin), sfc.lat.normalize(ymin), sfc.lon.normalize(xmax), sfc.lat.normalize(ymax))
    }.toArray

    new Z2Filter(xy, seek)
  }

  override def serializeToBytes(filter: Z2Filter): Array[Byte] = {
    // 4 bytes for length plus 1 byte for seek plus 16 bytes for each xy val (4 ints)
    val xyLength = 5 + filter.xy.length * 16
    val buffer = ByteBuffer.allocate(xyLength)

    buffer.putInt(filter.xy.length)
    filter.xy.foreach(bounds => bounds.foreach(buffer.putInt))
    buffer.put(if (filter.seek) { 1.toByte } else { 0.toByte })

    buffer.array()
  }

  override def deserializeFromBytes(serialized: Array[Byte]): Z2Filter = {
    val buffer = ByteBuffer.wrap(serialized)
    val xy = Array.fill(buffer.getInt())(Array.fill(4)(buffer.getInt))
    val seek = if (buffer.hasRemaining) { buffer.get() > 0 } else { true }
    new Z2Filter(xy, seek)
  }

  override def serializeToStrings(filter: Z2Filter): Map[String, String] = {
    val xy = filter.xy.map(bounds => bounds.mkString(RangeSeparator)).mkString(TermSeparator)
    Map(Z3Filter.XYKey -> xy, Z3Filter.SeekKey -> java.lang.Boolean.toString(filter.seek))
  }

  override def deserializeFromStrings(serialized: scala.collection.Map[String, String]): Z2Filter = {
    val xy = serialized(Z3Filter.XYKey).split(TermSeparator).map(_.split(RangeSeparator).map(_.toInt))
    new Z2Filter(xy, serialized.get(Z3Filter.SeekKey).forall(_.toBoolean))
  }

  /**
   * Gets z-low and z-hi for a bounding box in normalized space
   */
  private def zBounds(xy: Array[Array[Int]]): Array[Array[Long]] = {
    val bounds = xy.map { case Array(xmin, ymin, xmax, ymax) => Array(Z2(xmin, ymin).z, Z2(xmax, ymax).z) }
    java.util.Arrays.sort(bounds, ZBoundsOrdering)
    bounds
  }
}