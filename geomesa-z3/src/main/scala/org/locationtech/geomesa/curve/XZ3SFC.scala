/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.geomesa.curve.XZ3SFC.{QueryWindow, XElement}
import org.locationtech.sfcurve.IndexRange

import scala.collection.mutable.ArrayBuffer

/**
  * Extended Z-order curve implementation used for efficiently storing polygons.
  *
  * Based on 'XZ-Ordering: A Space-Filling Curve for Objects with Spatial Extension'
  * by Christian Böhm, Gerald Klump  and Hans-Peter Kriegel, expanded to 3 dimensions
  *
  * @param g resolution level of the curve - i.e. how many times the space will be recursively quartered

  */
class XZ3SFC(val g: Short, val xBounds: (Double, Double), val yBounds: (Double, Double), val zBounds: (Double, Double)) {

  // TODO see what the max value of g can be where we can use Ints instead of Longs and possibly refactor to use Ints

  private val xLo = xBounds._1
  private val xHi = xBounds._2
  private val yLo = yBounds._1
  private val yHi = yBounds._2
  private val zLo = zBounds._1
  private val zHi = zBounds._2

  private val xSize = xHi - xLo
  private val ySize = yHi - yLo
  private val zSize = zHi - zLo

  /**
    * Index a polygon by it's bounding box
    *
    * @param xmin min x value in xBounds
    * @param ymin min y value in yBounds
    * @param zmin min z value in zBounds
    * @param xmax max x value in xBounds, must be >= xmin
    * @param ymax max y value in yBounds, must be >= ymin
    * @param zmax max z value in zBounds, must be >= tmin
    * @param lenient standardize boundaries to valid values, or raise an exception
    * @return z value for the bounding box
    */
  def index(xmin: Double, ymin: Double, zmin: Double, xmax: Double, ymax: Double, zmax: Double, lenient: Boolean = false): Long = {
    // normalize inputs to [0,1]
    val (nxmin, nymin, nzmin, nxmax, nymax, nzmax) = normalize(xmin, ymin, zmin, xmax, ymax, zmax, lenient)

    // calculate the length of the sequence code (section 4.1 of XZ-Ordering paper)

    val maxDim = math.max(math.max(nxmax - nxmin, nymax - nymin), nzmax - nzmin)

    // l1 (el-one) is a bit confusing to read, but corresponds with the paper's definitions
    val l1 = math.floor(math.log(maxDim) / XZSFC.LogPointFive).toInt

    // the length will either be (l1) or (l1 + 1)
    val length = if (l1 >= g) { g } else {
      val w2 = math.pow(0.5, l1 + 1) // width of an element at resolution l2 (l1 + 1)

      // predicate for checking how many axis the polygon intersects
      // math.floor(min / w2) * w2 == start of cell containing min
      def predicate(min: Double, max: Double): Boolean = max <= (math.floor(min / w2) * w2) + (2 * w2)

      if (predicate(nxmin, nxmax) && predicate(nymin, nymax) && predicate(nzmin, nzmax)) l1 + 1 else l1
    }

    sequenceCode(nxmin, nymin, nzmin, length)
  }

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param query a window to cover in the form (xmin, ymin, zmin, xmax, ymax, zmax) where all values are in user space
    * @return
    */
  def ranges(query: (Double, Double, Double, Double, Double, Double)): Seq[IndexRange] = ranges(Seq(query))

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param query a window to cover in the form (xmin, ymin, zmin, xmax, ymax, zmax) where all values are in user space
    * @param maxRanges a rough upper limit on the number of ranges to generate
    * @return
    */
  def ranges(query: (Double, Double, Double, Double, Double, Double), maxRanges: Option[Int]): Seq[IndexRange] =
    ranges(Seq(query), maxRanges)

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param xmin min x value in user space
    * @param ymin min y value in user space
    * @param zmin min z value in user space
    * @param xmax max x value in user space, must be >= xmin
    * @param ymax max y value in user space, must be >= ymin
    * @param zmax max z value in user space, must be >= zmin
    * @return
    */
  def ranges(xmin: Double, ymin: Double, zmin: Double, xmax: Double, ymax: Double, zmax: Double): Seq[IndexRange] =
    ranges(Seq((xmin, ymin, zmin, xmax, ymax, zmax)))

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param xmin min x value in user space
    * @param ymin min y value in user space
    * @param zmin min z value in user space
    * @param xmax max x value in user space, must be >= xmin
    * @param ymax max y value in user space, must be >= ymin
    * @param zmax max z value in user space, must be >= zmin
    * @param maxRanges a rough upper limit on the number of ranges to generate
    * @return
    */
  def ranges(xmin: Double,
             ymin: Double,
             zmin: Double,
             xmax: Double,
             ymax: Double,
             zmax: Double,
             maxRanges: Option[Int]): Seq[IndexRange] =
    ranges(Seq((xmin, ymin, zmin, xmax, ymax, zmax)), maxRanges)

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param queries a sequence of OR'd windows to cover. Each window is in the form
    *                (xmin, ymin, zmin, xmax, ymax, zmax) where all values are in user space
    * @param maxRanges a rough upper limit on the number of ranges to generate
    * @return
    */
  def ranges(queries: Seq[(Double, Double, Double, Double, Double, Double)],
             maxRanges: Option[Int] = None): Seq[IndexRange] = {
    // normalize inputs to [0,1]
    val windows = queries.map { case (xmin, ymin, zmin, xmax, ymax, zmax) =>
      val (nxmin, nymin, nzmin, nxmax, nymax, nzmax) = normalize(xmin, ymin, zmin, xmax, ymax, zmax, lenient = false)
      QueryWindow(nxmin, nymin, nzmin, nxmax, nymax, nzmax)
    }
    ranges(windows.toArray, maxRanges.getOrElse(Int.MaxValue))
  }

  /**
    * Determine XZ-curve ranges that will cover a given query window
    *
    * @param query a sequence of OR'd windows to cover, normalized to [0,1]
    * @param rangeStop a rough max value for the number of ranges to return
    * @return
    */
  private def ranges(query: Array[QueryWindow], rangeStop: Int): Seq[IndexRange] = {

    import XZ3SFC.{LevelOneElements, LevelTerminator}

    // stores our results - initial size of 100 in general saves us some re-allocation
    val ranges = new java.util.ArrayList[IndexRange](100)

    // values remaining to process - initial size of 100 in general saves us some re-allocation
    val remaining = new java.util.ArrayDeque[XElement](100)

    // checks if a quad is contained in the search space
    def isContained(oct: XElement): Boolean = {
      var i = 0
      while (i < query.length) {
        if (oct.isContained(query(i))) {
          return true
        }
        i += 1
      }
      false
    }

    // checks if a quad overlaps the search space
    def isOverlapped(oct: XElement): Boolean = {
      var i = 0
      while (i < query.length) {
        if (oct.overlaps(query(i))) {
          return true
        }
        i += 1
      }
      false
    }

    // checks a single value and either:
    //   eliminates it as out of bounds
    //   adds it to our results as fully matching, or
    //   adds it to our results as partial matching and queues up it's children for further processing
    def checkValue(oct: XElement, level: Short): Unit = {
      if (isContained(oct)) {
        // whole range matches, happy day
        val (min, max) = sequenceInterval(oct.xmin, oct.ymin, oct.zmin, level, partial = false)
        ranges.add(IndexRange(min, max, contained = true))
      } else if (isOverlapped(oct)) {
        // some portion of this range is excluded
        // add the partial match and queue up each sub-range for processing
        val (min, max) = sequenceInterval(oct.xmin, oct.ymin, oct.zmin, level, partial = true)
        ranges.add(IndexRange(min, max, contained = false))
        oct.children.foreach(remaining.add)
      }
    }

    // initial level
    LevelOneElements.foreach(remaining.add)
    remaining.add(LevelTerminator)

    // level of recursion
    var level: Short = 1

    while (level < g && !remaining.isEmpty && ranges.size < rangeStop) {
      val next = remaining.poll
      if (next.eq(LevelTerminator)) {
        // we've fully processed a level, increment our state
        if (!remaining.isEmpty) {
          level = (level + 1).toShort
          remaining.add(LevelTerminator)
        }
      } else {
        checkValue(next, level)
      }
    }

    // bottom out and get all the ranges that partially overlapped but we didn't fully process
    while (!remaining.isEmpty) {
      val oct = remaining.poll
      if (oct.eq(LevelTerminator)) {
        level = (level + 1).toShort
      } else {
        val (min, max) = sequenceInterval(oct.xmin, oct.ymin, oct.zmin, level, partial = false)
        ranges.add(IndexRange(min, max, contained = false))
      }
    }

    // we've got all our ranges - now reduce them down by merging overlapping values
    // note: we don't bother reducing the ranges as in the XZ paper, as accumulo handles lots of ranges fairly well
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

    result
  }

  /**
    * Computes the sequence code for a given point - for polygons this is the lower-left corner.
    *
    * Based on Definition 2 from the XZ-Ordering paper
    *
    * @param x normalized x value [0,1]
    * @param y normalized y value [0,1]
    * @param z normalized z value [0,1]
    * @param length length of the sequence code that will be generated
    * @return
    */
  private def sequenceCode(x: Double, y: Double, z: Double, length: Int): Long = {
    var xmin = 0.0
    var ymin = 0.0
    var zmin = 0.0
    var xmax = 1.0
    var ymax = 1.0
    var zmax = 1.0

    var cs = 0L

    var i = 0
    while (i < length) {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      val zCenter = (zmin + zmax) / 2.0
      (x < xCenter, y < yCenter, z < zCenter) match {
        case (true,  true, true)   => cs += 1L                                             ; xmax = xCenter; ymax = yCenter; zmax = zCenter
        case (false, true, true)   => cs += 1L + 1L * (math.pow(8, g - i).toLong - 1L) / 7L; xmin = xCenter; ymax = yCenter; zmax = zCenter
        case (true,  false, true)  => cs += 1L + 2L * (math.pow(8, g - i).toLong - 1L) / 7L; xmax = xCenter; ymin = yCenter; zmax = zCenter
        case (false, false, true)  => cs += 1L + 3L * (math.pow(8, g - i).toLong - 1L) / 7L; xmin = xCenter; ymin = yCenter; zmax = zCenter
        case (true,  true, false)  => cs += 1L + 4L * (math.pow(8, g - i).toLong - 1L) / 7L; xmax = xCenter; ymax = yCenter; zmin = zCenter
        case (false, true, false)  => cs += 1L + 5L * (math.pow(8, g - i).toLong - 1L) / 7L; xmin = xCenter; ymax = yCenter; zmin = zCenter
        case (true,  false, false) => cs += 1L + 6L * (math.pow(8, g - i).toLong - 1L) / 7L; xmax = xCenter; ymin = yCenter; zmin = zCenter
        case (false, false, false) => cs += 1L + 7L * (math.pow(8, g - i).toLong - 1L) / 7L; xmin = xCenter; ymin = yCenter; zmin = zCenter
      }
      i += 1
    }

    cs
  }

  /**
    * Computes an interval of sequence codes for a given point - for polygons this is the lower-left corner.
    *
    * @param x normalized x value [0,1]
    * @param y normalized y value [0,1]
    * @param length length of the sequence code that will used as the basis for this interval
    * @param partial true if the element partially intersects the query window, false if it is fully contained
    * @return
    */
  private def sequenceInterval(x: Double, y: Double, z: Double, length: Short, partial: Boolean): (Long, Long) = {
    val min = sequenceCode(x, y, z, length)
    // if a partial match, we just use the single sequence code as an interval
    // if a full match, we have to match all sequence codes starting with the single sequence code
    val max = if (partial) { min } else {
      // from lemma 3 in the XZ-Ordering paper
      min + (math.pow(8, g - length + 1).toLong - 1L) / 7L
    }
    (min, max)
  }

  /**
    * Normalize user space values to [0,1]
    *
    * @param xmin min x value in user space
    * @param ymin min y value in user space
    * @param zmin min z value in user space
    * @param xmax max x value in user space, must be >= xmin
    * @param ymax max y value in user space, must be >= ymin
    * @param zmax max z value in user space, must be >= zmin
    * @param lenient standardize boundaries to valid values, or raise an exception
    * @return
    */
  private def normalize(xmin: Double,
                        ymin: Double,
                        zmin: Double,
                        xmax: Double,
                        ymax: Double,
                        zmax: Double,
                        lenient: Boolean): (Double, Double, Double, Double, Double, Double) = {
    require(xmin <= xmax && ymin <= ymax && zmin <= zmax,
      s"Bounds must be ordered: [$xmin $xmax] [$ymin $ymax] [$zmin $zmax]")

    try {
      require(xmin >= xLo && xmax <= xHi && ymin >= yLo && ymax <= yHi && zmin >= zLo && zmax <= zHi,
        s"Values out of bounds ([$xLo $xHi] [$yLo $yHi] [$zLo $zHi]): [$xmin $xmax] [$ymin $ymax] [$zmin $zmax]")

      val nxmin = (xmin - xLo) / xSize
      val nymin = (ymin - yLo) / ySize
      val nzmin = (zmin - zLo) / zSize
      val nxmax = (xmax - xLo) / xSize
      val nymax = (ymax - yLo) / ySize
      val nzmax = (zmax - zLo) / zSize

      (nxmin, nymin, nzmin, nxmax, nymax, nzmax)
    } catch {
      case _: IllegalArgumentException if lenient =>

        val bxmin = if (xmin < xLo) { xLo } else if (xmin > xHi) { xHi } else { xmin }
        val bymin = if (ymin < yLo) { yLo } else if (ymin > yHi) { yHi } else { ymin }
        val bzmin = if (zmin < zLo) { zLo } else if (zmin > zHi) { zHi } else { zmin }
        val bxmax = if (xmax < xLo) { xLo } else if (xmax > xHi) { xHi } else { xmax }
        val bymax = if (ymax < yLo) { yLo } else if (ymax > yHi) { yHi } else { ymax }
        val bzmax = if (zmax < zLo) { zLo } else if (zmax > zHi) { zHi } else { zmax }

        val nxmin = (bxmin - xLo) / xSize
        val nymin = (bymin - yLo) / ySize
        val nzmin = (bzmin - zLo) / zSize
        val nxmax = (bxmax - xLo) / xSize
        val nymax = (bymax - yLo) / ySize
        val nzmax = (bzmax - zLo) / zSize

        (nxmin, nymin, nzmin, nxmax, nymax, nzmax)
    }
  }
}

object XZ3SFC {

  // the initial level of octs
  private val LevelOneElements = XElement(0.0, 0.0, 0.0, 1.0, 1.0, 1.0, 1.0).children

  // indicator that we have searched a full level of the oct tree
  private val LevelTerminator = XElement(-1.0, -1.0, -1.0, -1.0, -1.0, -1.0, 0.0)

  private val cache = new java.util.concurrent.ConcurrentHashMap[(Short, TimePeriod), XZ3SFC]()

  def apply(g: Short, period: TimePeriod): XZ3SFC = {
    var sfc = cache.get((g, period))
    if (sfc == null) {
      sfc = new XZ3SFC(g, (-180.0, 180.0), (-90.0, 90.0), (0.0, BinnedTime.maxOffset(period).toDouble))
      cache.put((g, period), sfc)
    }
    sfc
  }

  /**
    * Region being queried. Bounds are normalized to [0-1].
    *
    * @param xmin x lower bound in [0-1]
    * @param ymin y lower bound in [0-1]
    * @param zmin z lower bound in [0-1]
    * @param xmax x upper bound in [0-1], must be >= xmin
    * @param ymax y upper bound in [0-1], must be >= ymin
    * @param zmax z upper bound in [0-1], must be >= zmin
    */
  private case class QueryWindow(xmin: Double, ymin: Double, zmin: Double, xmax: Double, ymax: Double, zmax: Double)

  /**
    * An extended Z curve element. Bounds refer to the non-extended z element for simplicity of calculation.
    *
    * An extended Z element refers to a normal Z curve element that has it's upper bounds expanded by double it's
    * width/length/height. By convention, an element is always a cube.
    *
    * @param xmin x lower bound in [0-1]
    * @param ymin y lower bound in [0-1]
    * @param zmin z lower bound in [0-1]
    * @param xmax x upper bound in [0-1], must be >= xmin
    * @param ymax y upper bound in [0-1], must be >= ymin
    * @param zmax z upper bound in [0-1], must be >= zmin
    * @param length length of the non-extended side (note: by convention width should be equal to height and depth)
    */
  private case class XElement(xmin: Double,
                              ymin: Double,
                              zmin: Double,
                              xmax: Double,
                              ymax: Double,
                              zmax: Double,
                              length: Double) {

    // extended x and y bounds
    lazy val xext = xmax + length
    lazy val yext = ymax + length
    lazy val zext = zmax + length

    def isContained(window: QueryWindow): Boolean =
      window.xmin <= xmin && window.ymin <= ymin && window.zmin <= zmin &&
          window.xmax >= xext && window.ymax >= yext && window.zmax >= zext

    def overlaps(window: QueryWindow): Boolean =
      window.xmax >= xmin && window.ymax >= ymin && window.zmax >= zmin &&
          window.xmin <= xext && window.ymin <= yext && window.zmin <= zext

    def children: Seq[XElement] = {
      val xCenter = (xmin + xmax) / 2.0
      val yCenter = (ymin + ymax) / 2.0
      val zCenter = (zmin + zmax) / 2.0
      val len = length / 2.0
      val c0 = copy(xmax = xCenter, ymax = yCenter, zmax = zCenter, length = len)
      val c1 = copy(xmin = xCenter, ymax = yCenter, zmax = zCenter, length = len)
      val c2 = copy(xmax = xCenter, ymin = yCenter, zmax = zCenter, length = len)
      val c3 = copy(xmin = xCenter, ymin = yCenter, zmax = zCenter, length = len)
      val c4 = copy(xmax = xCenter, ymax = yCenter, zmin = zCenter, length = len)
      val c5 = copy(xmin = xCenter, ymax = yCenter, zmin = zCenter, length = len)
      val c6 = copy(xmax = xCenter, ymin = yCenter, zmin = zCenter, length = len)
      val c7 = copy(xmin = xCenter, ymin = yCenter, zmin = zCenter, length = len)
      Seq(c0, c1, c2, c3, c4, c5, c6, c7)
    }
  }
}
