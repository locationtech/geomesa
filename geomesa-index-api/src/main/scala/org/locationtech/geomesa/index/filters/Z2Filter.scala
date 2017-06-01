/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.filters

import java.nio.ByteBuffer

import com.google.common.primitives.{Bytes, Ints, Longs}
import org.locationtech.sfcurve.zorder.Z2

class Z2Filter(val xyvals: Array[Array[Int]], val zLength: Int) extends java.io.Serializable {

  val rowToZ: (Array[Byte], Int) => Long = Z2Filter.getRowToZ(zLength)

  def inBounds(buf: Array[Byte], offset: Int): Boolean = {
    val keyZ = rowToZ(buf, offset)
    pointInBounds(keyZ)
  }

  def pointInBounds(z: Long): Boolean = {
    val x = Z2(z).d0
    val y = Z2(z).d1
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
}

object Z2Filter {
  def getRowToZ(length: Int): (Array[Byte], Int) => Long = {
    if (length == 8) {
      zToRow8
    } else if (length == 4) {
      zToRow4
    } else if (length == 3) {
      zToRow3
    } else {
      throw new IllegalArgumentException(s"Unhandled number of bytes for z value: $length")
    }
  }

  private def zToRow8(b: Array[Byte], i: Int): Long =
    Longs.fromBytes(b(i), b(i + 1), b(i + 2), b(i + 3), b(i + 4), b(i + 5), b(i + 6), b(i + 7))

  private def zToRow4(b: Array[Byte], i: Int): Long =
    Longs.fromBytes(b(i), b(i + 1), b(i + 2), b(i + 3), 0, 0, 0, 0)

  private def zToRow3(b: Array[Byte], i: Int): Long =
    Longs.fromBytes(b(i), b(i + 1), b(i + 2), 0, 0, 0, 0, 0)

  def toByteArray(f: Z2Filter): Array[Byte] = {
    val boundsLength = f.xyvals.length
    val boundsSer =
      f.xyvals.map { bounds =>
        val length = bounds.length
        val ser = Bytes.concat(bounds.map { v => Ints.toByteArray(v) }: _*)
        Bytes.concat(Ints.toByteArray(length), ser)
      }
    Bytes.concat(Ints.toByteArray(boundsLength), Bytes.concat(boundsSer: _*), Ints.toByteArray(f.zLength))
  }

  def fromByteArray(a: Array[Byte]): Z2Filter = {
    val buf = ByteBuffer.wrap(a)
    val boundsLength = buf.getInt()
    val bounds = (0 until boundsLength).map { i =>
      val length = buf.getInt()
      (0 until length).map { j =>
        buf.getInt()
      }.toArray
    }.toArray
    val zLength = buf.getInt
    new Z2Filter(bounds, zLength)
  }
}