/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.curve

class Z3(val z: Long) extends AnyVal {
  import Z3._

  def < (other: Z3) = z < other.z
  def > (other: Z3) = z > other.z
  def >= (other: Z3) = z >= other.z
  def <= (other: Z3) = z <= other.z
  def + (offset: Long) = new Z3(z + offset)
  def - (offset: Long) = new Z3(z - offset)
  def == (other: Z3) = other.z == z

  def d0 = combine(z)
  def d1 = combine(z >> 1)
  def d2 = combine(z >> 2)

  def decode: (Int, Int, Int) = (d0, d1, d2)

  def dim(i: Int): Int = if (i == 0) d0 else if (i == 1) d1 else if (i == 2) d2 else {
    throw new IllegalArgumentException(s"Invalid dimension $i - valid dimensions are 0,1,2")
  }

  def inRange(rmin: Z3, rmax: Z3): Boolean = {
    val (x, y, z) = decode
    x >= rmin.d0 &&
      x <= rmax.d0 &&
      y >= rmin.d1 &&
      y <= rmax.d1 &&
      z >= rmin.d2 &&
      z <= rmax.d2
  }

  def mid(p: Z3): Z3 = {
    val (x, y, z) = decode
    val (px, py, pz) = p.decode
    Z3((x + px) / 2, (y + py) / 2, (z + pz) / 2)
  }

  def bitsToString = f"(${z.toBinaryString.toLong}%016d)(${d0.toBinaryString.toLong}%08d,${d1.toBinaryString.toLong}%08d,${d2.toBinaryString.toLong}%08d)"
  override def toString = f"$z $decode"
}

object Z3 {

  final val MAX_DIM = 3
  final val MAX_BITS = 21
  final val MAX_MASK = 0x1fffffL
  final val TOTAL_BITS = MAX_BITS * MAX_DIM

  def apply(zvalue: Long) = new Z3(zvalue)

  /** insert 00 between every bit in value. Only first 21 bits can be considered. */
  def split(value: Long): Long = {
    var x = value & MAX_MASK
    x = (x | x << 32) & 0x1f00000000ffffL
    x = (x | x << 16) & 0x1f0000ff0000ffL
    x = (x | x << 8)  & 0x100f00f00f00f00fL
    x = (x | x << 4)  & 0x10c30c30c30c30c3L
    (x | x << 2)      & 0x1249249249249249L
  }

  /** combine every third bit to form a value. Maximum value is 21 bits. */
  def combine(z: Long): Int = {
    var x = z & 0x1249249249249249L
    x = (x ^ (x >>  2)) & 0x10c30c30c30c30c3L
    x = (x ^ (x >>  4)) & 0x100f00f00f00f00fL
    x = (x ^ (x >>  8)) & 0x1f0000ff0000ffL
    x = (x ^ (x >> 16)) & 0x1f00000000ffffL
    x = (x ^ (x >> 32)) & MAX_MASK
    x.toInt
  }

  /**
   * So this represents the order of the tuple, but the bits will be encoded in reverse order:
   *   ....z1y1x1z0y0x0
   * This is a little confusing.
   */
  def apply(x: Int, y:  Int, z: Int): Z3 = {
    new Z3(split(x) | split(y) << 1 | split(z) << 2)
  }

  def unapply(z: Z3): Option[(Int, Int, Int)] = Some(z.decode)

  /**
   * Returns (litmax, bigmin) for the given range and point
   */
  def zdivide(p: Z3, rmin: Z3, rmax: Z3): (Z3, Z3) = {
    val (litmax, bigmin) = zdiv(load, MAX_DIM)(p.z, rmin.z, rmax.z)
    (new Z3(litmax), new Z3(bigmin))
  }

  /** Loads either 1000... or 0111... into starting at given bit index of a given dimension */
  private def load(target: Long, p: Long, bits: Int, dim: Int): Long = {
    val mask = ~(Z3.split(MAX_MASK >> (MAX_BITS-bits)) << dim)
    val wiped = target & mask
    wiped | (split(p) << dim)
  }

  /**
   * Recurse down the oct-tree and report all z-ranges which are contained
   * in the cube defined by the min and max points
   */
  def zranges(min: Z3, max: Z3): Seq[(Long, Long)] = {
    val (commonPrefix, commonBits) = longestCommonPrefix(min.z, max.z)

    // base our recursion on the depth of the tree that we get 'for free' from the common prefix
    val maxRecurse = if (commonBits < 30) 7 else if (commonBits < 40) 6 else 5

    val searchRange = Z3Range(min, max)
    var mq = new MergeQueue // stores our results

    def zranges(prefix: Long, offset: Int, oct: Long, level: Int): Unit = {
      val min: Long = prefix | (oct << offset) // QR + 000...
      val max: Long = min | (1L << offset) - 1 // QR + 111...
      val octRange = Z3Range(new Z3(min), new Z3(max))

      if (searchRange containsInUserSpace octRange) {
        // whole range matches, happy day
        mq += (octRange.min.z, octRange.max.z)
      } else if (searchRange overlapsInUserSpace octRange) {
        if (level < maxRecurse && offset > 0) {
          // some portion of this range is excluded
          // let our children work on each subrange
          val nextOffset = offset - MAX_DIM
          val nextLevel = level + 1
          zranges(min, nextOffset, 0, nextLevel)
          zranges(min, nextOffset, 1, nextLevel)
          zranges(min, nextOffset, 2, nextLevel)
          zranges(min, nextOffset, 3, nextLevel)
          zranges(min, nextOffset, 4, nextLevel)
          zranges(min, nextOffset, 5, nextLevel)
          zranges(min, nextOffset, 6, nextLevel)
          zranges(min, nextOffset, 7, nextLevel)
        } else {
          // bottom out - add the entire range so we don't miss anything
          mq += (octRange.min.z, octRange.max.z)
        }
      }
    }

    // kick off recursion over the narrowed space
    zranges(commonPrefix, TOTAL_BITS - commonBits, 0, 0)

    // return our aggregated results
    mq.toSeq
  }

  /**
   * Calculates the longest common binary prefix between two z longs
   *
   * @return (common prefix, number of bits in common)
   */
  def longestCommonPrefix(lower: Long, upper: Long): (Long, Int) = {
    var bitShift = TOTAL_BITS - MAX_DIM
    while ((lower >>> bitShift) == (upper >>> bitShift) && bitShift > -1) {
      bitShift -= MAX_DIM
    }
    bitShift += MAX_DIM // increment back to the last valid value
    (lower & (Long.MaxValue << bitShift), TOTAL_BITS - bitShift)
  }

  /**
   * Implements the the algorithm defined in: Tropf paper to find:
   * LITMAX: maximum z-index in query range smaller than current point, xd
   * BIGMIN: minimum z-index in query range greater than current point, xd
   *
   * @param load: function that knows how to load bits into appropraite dimension of a z-index
   * @param xd: z-index that is outside of the query range
   * @param rmin: minimum z-index of the query range, inclusive
   * @param rmax: maximum z-index of the query range, inclusive
   * @return (LITMAX, BIGMIN)
   */
  def zdiv(load: (Long, Long, Int, Int) => Long, dims: Int)(xd: Long, rmin: Long, rmax: Long): (Long, Long) = {
    require(rmin < rmax, "min ($rmin) must be less than max $(rmax)")
    var zmin: Long = rmin
    var zmax: Long = rmax
    var bigmin: Long = 0L
    var litmax: Long = 0L

    def bit(x: Long, idx: Int) = {
      ((x & (1L << idx)) >> idx).toInt
    }
    def over(bits: Long)  = 1L << (bits - 1)
    def under(bits: Long) = (1L << (bits - 1)) - 1

    var i = 64
    while (i > 0) {
      i -= 1

      val bits = i/dims+1
      val dim  = i%dims

      ( bit(xd, i), bit(zmin, i), bit(zmax, i) ) match {
        case (0, 0, 0) =>
        // continue

        case (0, 0, 1) =>
          zmax   = load(zmax, under(bits), bits, dim)
          bigmin = load(zmin, over(bits), bits, dim)

        case (0, 1, 0) =>
        // sys.error(s"Not possible, MIN <= MAX, (0, 1, 0)  at index $i")

        case (0, 1, 1) =>
          bigmin = zmin
          return (litmax, bigmin)

        case (1, 0, 0) =>
          litmax = zmax
          return (litmax, bigmin)

        case (1, 0, 1) =>
          litmax = load(zmax, under(bits), bits, dim)
          zmin = load(zmin, over(bits), bits, dim)

        case (1, 1, 0) =>
        // sys.error(s"Not possible, MIN <= MAX, (1, 1, 0) at index $i")

        case (1, 1, 1) =>
        // continue
      }
    }
    (litmax, bigmin)
  }

  /**
   * Cuts Z-Range in two and trims based on user space, can be used to perform augmented binary search
   *
   * @param xd: division point
   * @param inRange: is xd in query range
   */
  def cut(r: Z3Range, xd: Z3, inRange: Boolean): List[Z3Range] = {
    if (r.min.z == r.max.z) {
      Nil
    } else if (inRange) {
      if (xd.z == r.min.z)      // degenerate case, two nodes min has already been counted
        Z3Range(r.max, r.max) :: Nil
      else if (xd.z == r.max.z) // degenerate case, two nodes max has already been counted
        Z3Range(r.min, r.min) :: Nil
      else
        Z3Range(r.min, xd - 1) :: Z3Range(xd + 1, r.max) :: Nil
    } else {
      val (litmax, bigmin) = Z3.zdivide(xd, r.min, r.max)
      Z3Range(r.min, litmax) :: Z3Range(bigmin, r.max) :: Nil
    }
  }
}
