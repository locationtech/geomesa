package org.locationtech.geomesa.index.filters

import java.nio.ByteBuffer

import com.google.common.primitives.{Bytes, Ints, Longs}
import org.locationtech.sfcurve.zorder.Z2

class Z2Filter(val xyvals: Array[Array[Int]],
               val zOffset: Int,
               val zLength: Int) extends java.io.Serializable {

  val rowToZ: (Array[Byte]) => Long = Z2Filter.getRowToZ(zOffset, zLength)

  def inBounds(buf: Array[Byte]): Boolean = {
    val keyZ = rowToZ(buf)
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
  def getRowToZ(offset: Int, length: Int): (Array[Byte]) => Long = {
    val z0 = offset
    val z1 = offset + 1
    val z2 = offset + 2
    val z3 = offset + 3
    val z4 = offset + 4
    val z5 = offset + 5
    val z6 = offset + 6
    val z7 = offset + 7

    if (length == 8) {
      (b) => Longs.fromBytes(b(z0), b(z1), b(z2), b(z3), b(z4), b(z5), b(z6), b(z7))
    } else if (length == 3) {
      (b) => Longs.fromBytes(b(z0), b(z1), b(z2), 0, 0, 0, 0, 0)
    } else if (length == 4) {
      (b) => Longs.fromBytes(b(z0), b(z1), b(z2), b(z3), 0, 0, 0, 0)
    } else {
      throw new IllegalArgumentException(s"Unhandled number of bytes for z value: $length")
    }
  }

  def toByteArray(f: Z2Filter): Array[Byte] = {
    val boundsLength = f.xyvals.length
    val boundsSer =
      f.xyvals.map { bounds =>
        val length = bounds.length
        val ser = Bytes.concat(bounds.map { v => Ints.toByteArray(v) }: _*)
        Bytes.concat(Ints.toByteArray(length), ser)
      }
    Bytes.concat(Ints.toByteArray(boundsLength), Bytes.concat(boundsSer: _*),
      Ints.toByteArray(f.zOffset),
      Ints.toByteArray(f.zLength))
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
    val zOffset = buf.getInt
    val zLength = buf.getInt
    new Z2Filter(bounds, zOffset, zLength)
  }


}