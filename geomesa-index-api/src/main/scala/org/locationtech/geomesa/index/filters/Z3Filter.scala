/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.filters

import java.nio.ByteBuffer

import com.google.common.primitives.{Bytes, Ints, Longs, Shorts}
import org.locationtech.sfcurve.zorder.Z3


class Z3Filter(val xyvals: Array[Array[Int]],
               val tvals: Array[Array[Array[Int]]],
               val minEpoch: Short,
               val maxEpoch: Short,
               val zLength: Int) extends java.io.Serializable {

  val rowToZ: (Array[Byte], Int) => Long = Z3Filter.getRowToZ(zLength)

  def inBounds(buf: Array[Byte], offset: Int): Boolean = {
    val keyZ = rowToZ(buf, offset)
    pointInBounds(keyZ) && timeInBounds(Z3Filter.rowToEpoch(buf, offset), keyZ)
  }

  def pointInBounds(z: Long): Boolean = {
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

  def timeInBounds(epoch: Short, z: Long): Boolean = {
    // we know we're only going to scan appropriate epochs, so leave out whole epochs
    if (epoch > maxEpoch || epoch < minEpoch) { true } else {
      val times = tvals(epoch - minEpoch)
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
}

object Z3Filter {

  def getRowToZ(zLength: Int): (Array[Byte], Int) => Long = {
    if (zLength == 8) {
      zToRow8
    } else if (zLength == 4) {
      zToRow4
    } else if (zLength == 3) {
      zToRow3
    } else {
      throw new IllegalArgumentException(s"Unhandled number of bytes for z value: $zLength")
    }
  }

  // account for epoch - first 2 bytes
  private def zToRow8(b: Array[Byte], i: Int): Long =
    Longs.fromBytes(b(i + 2), b(i + 3), b(i + 4), b(i + 5), b(i + 6), b(i + 7), b(i + 8), b(i + 9))

  // account for epoch - first 2 bytes
  private def zToRow4(b: Array[Byte], i: Int): Long =
    Longs.fromBytes(b(i + 2), b(i + 3), b(i + 4), b(i + 5), 0, 0, 0, 0)

  // account for epoch - first 2 bytes
  private def zToRow3(b: Array[Byte], i: Int): Long =
    Longs.fromBytes(b(i + 2), b(i + 3), b(i + 4), 0, 0, 0, 0, 0)

  def rowToEpoch(bytes: Array[Byte], offset: Int): Short =
    Shorts.fromBytes(bytes(offset), bytes(offset + 1))

  def toByteArray(f: Z3Filter): Array[Byte] = {
    val boundsLength = f.xyvals.length
    val boundsSer =
      f.xyvals.map { bounds =>
        val length = bounds.length
        val ser = Bytes.concat(bounds.map { v => Ints.toByteArray(v) }: _*)
        Bytes.concat(Ints.toByteArray(length), ser)
      }
    val xyz = Bytes.concat(Ints.toByteArray(boundsLength), Bytes.concat(boundsSer: _*), Ints.toByteArray(f.zLength))


    val tlength = f.tvals.length
    val tSer =
      f.tvals.map { bounds =>
        val length = bounds.length
        val innerSer = bounds.map { b =>
          val innerLength = b.length
          val ser = Bytes.concat(b.map { v => Ints.toByteArray(v) }: _*)
          Bytes.concat(Ints.toByteArray(innerLength), ser)
        }
        Bytes.concat(Ints.toByteArray(length), Bytes.concat(innerSer: _*))
      }

    val t = Bytes.concat(Ints.toByteArray(tlength), Bytes.concat(tSer: _*))

    Bytes.concat(xyz, t, Shorts.toByteArray(f.minEpoch), Shorts.toByteArray(f.maxEpoch))
  }

  def fromByteArray(a: Array[Byte]): Z3Filter = {
    val buf = ByteBuffer.wrap(a)
    val boundsLength = buf.getInt()
    val bounds = (0 until boundsLength).map { _ =>
      val length = buf.getInt()
      (0 until length).map { _ =>
        buf.getInt()
      }.toArray
    }.toArray
    val zLength = buf.getInt

    val tLength = buf.getInt
    val tvals = (0 until tLength).map { _ =>
      val length = buf.getInt()
      (0 until length).map { _ =>
        val innerLength = buf.getInt()
        (0 until innerLength).map { _ =>
          buf.getInt
        }.toArray
      }.toArray
    }.toArray

    val minEpoch = buf.getShort
    val maxEpoch = buf.getShort
    new Z3Filter(bounds, tvals, minEpoch, maxEpoch, zLength)
  }
}