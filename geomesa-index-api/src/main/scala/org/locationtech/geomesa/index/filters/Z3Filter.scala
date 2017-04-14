package org.locationtech.geomesa.index.filters

import com.google.common.primitives.{Longs, Shorts}
import org.apache.commons.lang.SerializationUtils
import org.locationtech.sfcurve.zorder.Z3


class Z3Filter(val xyvals: Array[Array[Int]],
               val tvals: Array[Array[Array[Int]]],
               val minEpoch: Short,
               val maxEpoch: Short,
               val zOffset: Int,
               val zLength: Int) extends java.io.Serializable {

  val rowToEpoch: (Array[Byte], Int, Int) => Short = Z3Filter.getRowToEpoch(zOffset)
  val rowToZ: (Array[Byte], Int, Int) => Long = Z3Filter.getRowToZ(zOffset, zLength)

  def inBounds(buf: Array[Byte], offset: Int, length: Int): Boolean = {
    val keyZ = rowToZ(buf, offset, length)
    pointInBounds(keyZ) && timeInBounds(rowToEpoch(buf, offset, length), keyZ)
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
  def getRowToZ(offset: Int, zLength: Int): (Array[Byte], Int, Int) => Long = {
    // account for epoch - first 2 bytes
    if (zLength == 8) {
      (b, off, l) =>
        val base = offset + off + 2
        Longs.fromBytes(
          b(base),
          b(base+1),
          b(base+2),
          b(base+3),
          b(base+4),
          b(base+5),
          b(base+6),
          b(base+7))

    } else if (zLength == 3) {
      (b, off, l) =>
        val base = offset + off + 2
        Longs.fromBytes(b(base), b(base+1), b(base+2), 0, 0, 0, 0, 0)

    } else if (zLength == 4) {
      (b, off, l) =>
        val base = offset + off + 2
        Longs.fromBytes(b(base), b(base+1), b(base+2), b(base+3), 0, 0, 0, 0)

    } else {
      throw new IllegalArgumentException(s"Unhandled number of bytes for z value: $zLength")
    }
  }

  def getRowToEpoch(offset: Int): (Array[Byte], Int, Int) => Short = {
    (b, off, _) => Shorts.fromBytes(b(offset + off), b(offset+off+1))
  }

  def toByteArray(f: Z3Filter): Array[Byte] = SerializationUtils.serialize(f)

  def fromByteArray(a: Array[Byte]): Z3Filter = SerializationUtils.deserialize(a).asInstanceOf[Z3Filter]

}