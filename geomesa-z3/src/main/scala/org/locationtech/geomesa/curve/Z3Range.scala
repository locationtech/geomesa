/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.curve

/**
 * Represents a cube in index space defined by min and max as two opposing points.
 * All operations refer to user space.
 */
case class Z3Range(min: Z3, max: Z3) {

  // contains in user space - each dimension is contained
  def contains(r: Z3Range): Boolean = {
    min.dim(0) <= r.min.dim(0) && max.dim(0) >= r.max.dim(0) &&
        min.dim(1) <= r.min.dim(1) && max.dim(1) >= r.max.dim(1) &&
        min.dim(2) <= r.min.dim(2) && max.dim(2) >= r.max.dim(2)
  }

  // overlap in user space - if all dimensions overlaps
  def overlaps(r: Z3Range): Boolean = {
    math.max(min.dim(0), r.min.dim(0)) <= math.min(max.dim(0), r.max.dim(0)) &&
        math.max(min.dim(1), r.min.dim(1)) <= math.min(max.dim(1), r.max.dim(1)) &&
        math.max(min.dim(2), r.min.dim(2)) <= math.min(max.dim(2), r.max.dim(2))
  }
}

object Z3Range {

  /**
   * Recurse down the oct-tree and report all z-ranges which are contained
   * in the cube defined by the min and max points
   */
  def zranges(min: Z3, max: Z3, precision: Int = 64): Seq[(Long, Long)] = {
    val ZPrefix(commonPrefix, commonBits) = longestCommonPrefix(min.z, max.z)

    val searchRange = Z3Range(min, max)
    var mq = new MergeQueue // stores our results

    // base our recursion on the depth of the tree that we get 'for free' from the common prefix,
    // and on the expansion factor of the number child regions, which is proportional to the number of dimensions
    val maxRecurse = if (commonBits < 31) 7 else if (commonBits < 41) 6 else 5

    def zranges(prefix: Long, offset: Int, oct: Long, level: Int): Unit = {
      val min: Long = prefix | (oct << offset) // QR + 00000...
      val max: Long = min | (1L << offset) - 1 // QR + 11111...
      val octRange = Z3Range(Z3(min), Z3(max))

      if (searchRange.contains(octRange) || offset < 64 - precision) {
        // whole range matches, happy day
        mq += (octRange.min.z, octRange.max.z)
      } else if (searchRange.overlaps(octRange)) {
        if (level < maxRecurse && offset > 0) {
          // some portion of this range is excluded
          // let our children work on each subrange
          val nextOffset = offset - Z3.dims
          val nextLevel = level + 1
          var nextRegion = 0
          while (nextRegion < Z3.subRegions) {
            zranges(min, nextOffset, nextRegion, nextLevel)
            nextRegion += 1
          }
        } else {
          // bottom out - add the entire range so we don't miss anything
          mq += (octRange.min.z, octRange.max.z)
        }
      }
    }

    // kick off recursion over the narrowed space
    zranges(commonPrefix, 64 - commonBits, 0, 0)

    // return our aggregated results
    mq.toSeq
  }

  /**
   * Calculates the longest common binary prefix between two z longs
   *
   * @return (common prefix, number of bits in common)
   */
  def longestCommonPrefix(lower: Long, upper: Long): ZPrefix = {
    var bitShift = Z3.bits - Z3.dims
    while ((lower >>> bitShift) == (upper >>> bitShift) && bitShift > -1) {
      bitShift -= Z3.dims
    }
    bitShift += Z3.dims // increment back to the last valid value
    ZPrefix(lower & (Long.MaxValue << bitShift), 64 - bitShift)
  }

  /**
   * Returns (litmax, bigmin) for the given range and point
   */
  def zdivide(p: Z3, rmin: Z3, rmax: Z3): (Z3, Z3) = {
    val (litmax, bigmin) = zdiv(load, Z3.dims)(p.z, rmin.z, rmax.z)
    (Z3(litmax), Z3(bigmin))
  }

  /** Loads either 1000... or 0111... into starting at given bit index of a given dimension */
  private def load(target: Long, p: Long, bits: Int, dim: Int): Long = {
    val mask = ~(Z3.split(Z3.maxValue >> (Z3.bitsPerDim - bits)) << dim)
    val wiped = target & mask
    wiped | (Z3.split(p) << dim)
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
  private def zdiv(load: (Long, Long, Int, Int) => Long, dims: Int)(xd: Long, rmin: Long, rmax: Long): (Long, Long) = {
    require(rmin < rmax, s"min ($rmin) must be less than max ($rmax)")
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

  case class ZPrefix(prefix: Long, precision: Int) // precision in bits
}
