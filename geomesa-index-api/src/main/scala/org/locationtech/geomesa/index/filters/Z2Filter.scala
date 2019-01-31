/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.filters

import java.nio.ByteBuffer

import org.locationtech.geomesa.index.filters.Z3Filter._
import org.locationtech.geomesa.index.index.z2.Z2IndexValues
import org.locationtech.geomesa.utils.index.ByteArrays
import org.locationtech.sfcurve.zorder.Z2

class Z2Filter(val xy: Array[Array[Int]]) {

  def inBounds(buf: Array[Byte], offset: Int): Boolean = {
    val z = ByteArrays.readLong(buf, offset)
    val x = Z2(z).d0
    val y = Z2(z).d1
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

  override def toString: String = Z2Filter.serializeToStrings(this).toSeq.sortBy(_._1).mkString(",")
}

object Z2Filter {

  private val RangeSeparator = ":"
  private val TermSeparator  = ";"

  def apply(values: Z2IndexValues): Z2Filter = {
    val sfc = values.sfc
    val xy: Array[Array[Int]] = values.bounds.map { case (xmin, ymin, xmax, ymax) =>
      Array(sfc.lon.normalize(xmin), sfc.lat.normalize(ymin), sfc.lon.normalize(xmax), sfc.lat.normalize(ymax))
    }.toArray

    new Z2Filter(xy)
  }

  def serializeToBytes(filter: Z2Filter): Array[Byte] = {
    // 4 bytes for length plus 16 bytes for each xy val (4 ints)
    val xyLength = 4 + filter.xy.length * 16
    val buffer = ByteBuffer.allocate(xyLength)

    buffer.putInt(filter.xy.length)
    filter.xy.foreach(bounds => bounds.foreach(buffer.putInt))

    buffer.array()
  }

  def deserializeFromBytes(serialized: Array[Byte]): Z2Filter = {
    val buffer = ByteBuffer.wrap(serialized)
    val xy = Array.fill(buffer.getInt())(Array.fill(4)(buffer.getInt))
    new Z2Filter(xy)
  }

  def serializeToStrings(filter: Z2Filter): Map[String, String] = {
    val xy = filter.xy.map(bounds => bounds.mkString(RangeSeparator)).mkString(TermSeparator)
    Map(XYKey -> xy)
  }

  def deserializeFromStrings(serialized: scala.collection.Map[String, String]): Z2Filter = {
    val xy = serialized(XYKey).split(TermSeparator).map(_.split(RangeSeparator).map(_.toInt))
    new Z2Filter(xy)
  }
}