/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.filters

import java.nio.ByteBuffer

import com.google.common.geometry.S2CellId
import org.locationtech.geomesa.index.filters.RowFilter.RowFilterFactory
import org.locationtech.geomesa.index.index.s2.S2IndexValues
import org.locationtech.geomesa.utils.index.ByteArrays

class S2Filter(val xy: Array[Array[Double]]) extends RowFilter {
  override def inBounds(buf: Array[Byte], offset: Int): Boolean = {
    val latLon = new S2CellId(ByteArrays.readLong(buf, offset)).toLatLng
    val x = latLon.lngDegrees()
    val y = latLon.latDegrees()
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
}

object S2Filter extends RowFilterFactory[S2Filter] {

  private val RangeSeparator = ":"
  private val TermSeparator  = ";"

  val XYKey = "sxy"

  def apply(values: S2IndexValues): S2Filter = {
    val xy = values.bounds.map { case (xmin, ymin, xmax, ymax) => Array(xmin, ymin, xmax, ymax) }
    new S2Filter(xy.toArray)
  }

  override def serializeToBytes(filter: S2Filter): Array[Byte] = {
    // 4 bytes for length plus 16 bytes for each xy val (4 doubles)
    val xyLength = 4 + filter.xy.length * 32
    val buffer = ByteBuffer.allocate(xyLength)

    buffer.putInt(filter.xy.length)

    filter.xy.foreach(bounds => bounds.foreach(buffer.putDouble))

    buffer.array()
  }

  override def deserializeFromBytes(serialized: Array[Byte]): S2Filter = {
    val buffer = ByteBuffer.wrap(serialized)
    val xy = Array.fill(buffer.getInt())(Array.fill(4)(buffer.getDouble))
    new S2Filter(xy)
  }

  override def serializeToStrings(filter: S2Filter): Map[String, String] = {
    val xy = filter.xy.map(bounds => bounds.mkString(RangeSeparator)).mkString(TermSeparator)
    Map(XYKey -> xy)
  }

  override def deserializeFromStrings(serialized: scala.collection.Map[String, String]): S2Filter = {
    val xy = serialized(XYKey).split(TermSeparator).map(_.split(RangeSeparator).map(_.toDouble))
    new S2Filter(xy)
  }
}
