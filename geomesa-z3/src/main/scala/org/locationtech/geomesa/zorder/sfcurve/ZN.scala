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
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 1463162d60 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257 (GEOMESA-3254 Add Bloop build support)
>>>>>>> fa60953a42 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
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
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 7564665969 (GEOMESA-3254 Add Bloop build support)
=======
>>>>>>> e74fa3f690 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> b727e40f7c (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
>>>>>>> 9e49c1aac7 (GEOMESA-3254 Add Bloop build support)
<<<<<<< HEAD
>>>>>>> 3515f7f054 (GEOMESA-3254 Add Bloop build support)
=======
=======
>>>>>>> 3e610250ce (GEOMESA-3254 Add Bloop build support)
>>>>>>> 0bd247219b (GEOMESA-3254 Add Bloop build support)
 * Copyright (c) 2015 Azavea.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.zorder.sfcurve

import scala.collection.mutable.ArrayBuffer

/**
  * N-dimensional z-curve base class
  */
abstract class ZN {

  // number of bits used to store each dimension
  def BitsPerDimension: Int

  // number of dimensions
  def Dimensions: Int

  // max value for this z object - can be used to mask another long using &
  def MaxMask: Long

  // total bits used - usually bitsPerDim * dims
  def TotalBits: Int

  // number of quadrants in our quad/oct tree - has to be lazy to instantiate correctly
  private lazy val Quadrants = math.pow(2, Dimensions)

  /**
    * Insert (Dimensions - 1) zeros between each bit to create a zvalue from a single dimension.
    * Only the first BitsPerDimension can be considered.
    *
    * @param value value to split
    * @return
    */
  def split(value: Long): Long

  /**
    * Combine every (Dimensions - 1) bits to re-create a single dimension. Opposite of split.
    *
    * @param z value to combine
    * @return
    */
  def combine(z: Long): Int

  /**
    * Is the value contained in the range. Considers user-space.
    *
    * @param range range
    * @param value value to be tested
    * @return
    */
  def contains(range: ZRange, value: Long): Boolean

  /**
    * Is the value contained in the range. Considers user-space.
    *
    * @param range range
    * @param value value to be tested
    * @return
    */
  def contains(range: ZRange, value: ZRange): Boolean = contains(range, value.min) && contains(range, value.max)

  /**
    * Does the value overlap with the range. Considers user-space.
    *
    * @param range range
    * @param value value to be tested
    * @return
    */
  def overlaps(range: ZRange, value: ZRange): Boolean

  /**
    * Returns (litmax, bigmin) for the given range and point
    *
    * @param p point
    * @param rmin minimum value
    * @param rmax maximum value
    * @return (litmax, bigmin)
    */
  def zdivide(p: Long, rmin: Long, rmax: Long): (Long, Long) = ZN.zdiv(load, Dimensions)(p, rmin, rmax)

  def zranges(zbounds: ZRange): Seq[IndexRange] = zranges(Array(zbounds))
  def zranges(zbounds: ZRange, precision: Int): Seq[IndexRange] = zranges(Array(zbounds), precision)
  def zranges(zbounds: ZRange, precision: Int, maxRanges: Option[Int]): Seq[IndexRange] =
    zranges(Array(zbounds), precision, maxRanges)

  /**
    * Calculates ranges in index space that match any of the input bounds. Uses breadth-first searching to
    * allow a limit on the number of ranges returned.
    *
    * To improve performance, the following decisions have been made:
    *   uses loops instead of foreach/maps
    *   uses java queues instead of scala queues
    *   allocates initial sequences of decent size
    *   sorts once at the end before merging
    *
    * @param zbounds search space
    * @param precision precision to consider, in bits (max 64)
    * @param maxRanges loose cap on the number of ranges to return. A higher number of ranges will have less
    *                  false positives, but require more processing.
    * @param maxRecurse max levels of recursion to apply before stopping
    * @return ranges covering the search space
    */
  def zranges(zbounds: Array[ZRange],
              precision: Int = 64,
              maxRanges: Option[Int] = None,
              maxRecurse: Option[Int] = Some(ZN.DefaultRecurse)): Seq[IndexRange] = {

    import ZN.LevelTerminator

    // stores our results - initial size of 100 in general saves us some re-allocation
    val ranges = new java.util.ArrayList[IndexRange](100)

    // values remaining to process - initial size of 100 in general saves us some re-allocation
    val remaining = new java.util.ArrayDeque[(Long, Long)](100)

    // calculate the common prefix in the z-values - we start processing with the first diff
    val ZPrefix(commonPrefix, commonBits) = longestCommonPrefix(zbounds.flatMap(b => Seq(b.min, b.max)): _*)

    var offset = 64 - commonBits

    // checks if a range is contained in the search space
    def isContained(range: ZRange): Boolean = {
      var i = 0
      while (i < zbounds.length) {
        if (contains(zbounds(i), range)) {
          return true
        }
        i += 1
      }
      false
    }

    // checks if a range overlaps the search space
    def isOverlapped(range: ZRange): Boolean = {
      var i = 0
      while (i < zbounds.length) {
        if (overlaps(zbounds(i), range)) {
          return true
        }
        i += 1
      }
      false
    }

    // checks a single value and either:
    //   eliminates it as out of bounds
    //   adds it to our results as fully matching, or
    //   queues up it's children for further processing
    def checkValue(prefix: Long, quadrant: Long): Unit = {
      val min: Long = prefix | (quadrant << offset) // QR + 000...
      val max: Long = min | (1L << offset) - 1 // QR + 111...
      val quadrantRange = ZRange(min, max)

      if (isContained(quadrantRange) || offset < 64 - precision) {
        // whole range matches, happy day
        ranges.add(IndexRange(min, max, contained = true))
      } else if (isOverlapped(quadrantRange)) {
        // some portion of this range is excluded
        // queue up each sub-range for processing
        remaining.add((min, max))
      }
    }

    // bottom out and get all the ranges that partially overlapped but we didn't fully process
    // note: this method is only called when we know there are items remaining in the queue
    def bottomOut(): Unit = {
      while ({{
        val minMax = remaining.poll
        if (!minMax.eq(LevelTerminator)) {
          ranges.add(IndexRange(minMax._1, minMax._2, contained = false))
        }
      }; !remaining.isEmpty })()
    }

    // initial level - we just check the single quadrant
    checkValue(commonPrefix, 0)
    remaining.add(LevelTerminator)
    offset -= Dimensions

    // level of recursion
    var level = 0

    val rangeStop = maxRanges.getOrElse(Int.MaxValue)
    val recurseStop = maxRecurse.getOrElse(ZN.DefaultRecurse)

    while ({{
      val next = remaining.poll
      if (next.eq(LevelTerminator)) {
        // we've fully processed a level, increment our state
        if (!remaining.isEmpty) {
          level += 1
          offset -= Dimensions
          if (level >= recurseStop || offset < 0) {
            bottomOut()
          } else {
            remaining.add(LevelTerminator)
          }
        }
      } else {
        val prefix = next._1
        var quadrant = 0L
        while (quadrant < Quadrants) {
          checkValue(prefix, quadrant)
          quadrant += 1
        }
        // subtract one from remaining.size to account for the LevelTerminator
        if (ranges.size + remaining.size - 1 >= rangeStop) {
          bottomOut()
        }
      }
    }; !remaining.isEmpty })()

    // we've got all our ranges - now reduce them down by merging overlapping values
    ranges.sort(IndexRange.IndexRangeIsOrdered)

    var current = ranges.get(0) // note: should always be at least one range
    val result = ArrayBuffer.empty[IndexRange]
    var i = 1
    while (i < ranges.size()) {
      val range = ranges.get(i)
      if (range.lower <= current.upper + 1) {
        // merge the two ranges
        current = IndexRange(current.lower, math.max(current.upper, range.upper), current.contained && range.contained)
      } else {
        // append the last range and set the current range for future merging
        result.append(current)
        current = range
      }
      i += 1
    }
    // append the last range - there will always be one left that wasn't added
    result.append(current)

    result.toSeq
  }

  /**
   * Cuts Z-Range in two and trims based on user space, can be used to perform augmented binary search
   *
   * @param xd: division point
   * @param inRange: is xd in query range
   */
  def cut(r: ZRange, xd: Long, inRange: Boolean): List[ZRange] = {
    if (r.min == r.max) {
      Nil
    } else if (inRange) {
      if (xd == r.min) { // degenerate case, two nodes min has already been counted
        ZRange(r.max, r.max) :: Nil
      } else if (xd == r.max) { // degenerate case, two nodes max has already been counted
        ZRange(r.min, r.min) :: Nil
      } else {
        ZRange(r.min, xd - 1) :: ZRange(xd + 1, r.max) :: Nil
      }
    } else {
      val (litmax, bigmin) = zdivide(xd, r.min, r.max)
      ZRange(r.min, litmax) :: ZRange(bigmin, r.max) :: Nil
    }
  }

  /**
    * Calculates the longest common binary prefix between two z longs
    *
    * @return (common prefix, number of bits in common)
    */
  def longestCommonPrefix(values: Long*): ZPrefix = {
    var bitShift = TotalBits - Dimensions
    var head = values.head >>> bitShift
    while (values.tail.forall(v => (v >>> bitShift) == head) && bitShift > -1) {
      bitShift -= Dimensions
      head = values.head >>> bitShift
    }
    bitShift += Dimensions // increment back to the last valid value
    ZPrefix(values.head & (Long.MaxValue << bitShift), 64 - bitShift)
  }

  /** Loads either 1000... or 0111... into starting at given bit index of a given dimension */
  private def load(target: Long, p: Long, bits: Int, dim: Int): Long = {
    val mask = ~(split(MaxMask >> (BitsPerDimension - bits)) << dim)
    val wiped = target & mask
    wiped | (split(p) << dim)
  }
}

object ZN {

  val DefaultRecurse = 7

  // indicator that we have searched a full level of the quad/oct tree
  private val LevelTerminator = (-1L, -1L)

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
  private [zorder] def zdiv(load: (Long, Long, Int, Int) => Long, dims: Int)
                           (xd: Long, rmin: Long, rmax: Long): (Long, Long) = {
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
}
