/***********************************************************************
<<<<<<< HEAD
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
=======
<<<<<<< HEAD
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
=======
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 9f430502b2 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
>>>>>>> dce8c58b44 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
 * Copyright (c) 2015 Azavea.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.zorder.sfcurve

class Z2(val z: Long) extends AnyVal {
  import Z2._

  def < (other: Z2): Boolean = z < other.z
  def > (other: Z2): Boolean = z > other.z

  def <= (other: Z2): Boolean = z <= other.z
  def >= (other: Z2): Boolean = z >= other.z

  def + (offset: Long): Z2 = new Z2(z + offset)
  def - (offset: Long): Z2 = new Z2(z - offset)

  def == (other: Z2): Boolean = other.z == z

  def decode: (Int, Int) = (combine(z), combine(z>>1))

  def dim(i: Int): Int = Z2.combine(z >> i)

  def d0: Int = dim(0)
  def d1: Int = dim(1)

  def mid(p: Z2): Z2 = {
    val (x, y) = decode
    val (px, py) = p.decode
    Z2((x + px) >>> 1, (y + py) >>> 1) // overflow safe mean
  }

  def bitsToString = f"(${z.toBinaryString}%16s)(${dim(0).toBinaryString}%8s,${dim(1).toBinaryString}%8s)"
  override def toString = f"$z $decode"
}

object Z2 extends ZN {

  override val Dimensions = 2
  override val BitsPerDimension = 31
  override val TotalBits = 62
  override val MaxMask = 0x7fffffffL // ignore the sign bit, using it breaks < relationship

  def apply(zvalue: Long): Z2 = new Z2(zvalue)

  // Bits of x and y will be encoded as ....y1x1y0x0
  def apply(x: Int, y:  Int): Z2 = new Z2(split(x) | split(y) << 1)

  def unapply(z: Z2): Option[(Int, Int)] = Some(z.decode)

  /** insert 0 between every bit in value. Only first 31 bits can be considered. */
  override def split(value: Long): Long = {
    var x: Long = value & MaxMask
    x = (x ^ (x << 32)) & 0x00000000ffffffffL
    x = (x ^ (x << 16)) & 0x0000ffff0000ffffL
    x = (x ^ (x <<  8)) & 0x00ff00ff00ff00ffL // 11111111000000001111111100000000..
    x = (x ^ (x <<  4)) & 0x0f0f0f0f0f0f0f0fL // 1111000011110000
    x = (x ^ (x <<  2)) & 0x3333333333333333L // 11001100..
    x = (x ^ (x <<  1)) & 0x5555555555555555L // 1010...
    x
  }

  /** combine every other bit to form a value. Maximum value is 31 bits. */
  override def combine(z: Long): Int = {
    var x = z & 0x5555555555555555L
    x = (x ^ (x >>  1)) & 0x3333333333333333L
    x = (x ^ (x >>  2)) & 0x0f0f0f0f0f0f0f0fL
    x = (x ^ (x >>  4)) & 0x00ff00ff00ff00ffL
    x = (x ^ (x >>  8)) & 0x0000ffff0000ffffL
    x = (x ^ (x >> 16)) & 0x00000000ffffffffL
    x.toInt
  }

  override def contains(range: ZRange, value: Long): Boolean = {
    val (x, y) = Z2(value).decode
    x >= Z2(range.min).d0 && x <= Z2(range.max).d0 && y >= Z2(range.min).d1 && y <= Z2(range.max).d1
  }

  override def overlaps(range: ZRange, value: ZRange): Boolean = {
    def overlaps(a1: Int, a2: Int, b1: Int, b2: Int) = math.max(a1, b1) <= math.min(a2, b2)
    overlaps(Z2(range.min).d0, Z2(range.max).d0, Z2(value.min).d0, Z2(value.max).d0) &&
        overlaps(Z2(range.min).d1, Z2(range.max).d1, Z2(value.min).d1, Z2(value.max).d1)
  }
}
