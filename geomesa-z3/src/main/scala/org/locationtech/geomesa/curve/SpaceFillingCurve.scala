/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.sfcurve.IndexRange

trait SpaceFillingCurve[T] {

  import SpaceFillingCurve.FullPrecision

  def lat: NormalizedDimension
  def lon: NormalizedDimension
  def index(x: Double, y: Double, lenient: Boolean = false): T
  def invert(i: T): (Double, Double)

  def ranges(x: (Double, Double), y: (Double, Double)): Seq[IndexRange] =
    ranges(Seq((x._1, y._1, x._2, y._2)), FullPrecision, None)

  def ranges(x: (Double, Double), y: (Double, Double), precision: Int): Seq[IndexRange] =
    ranges(Seq((x._1, y._1, x._2, y._2)), precision, None)

  def ranges(x: (Double, Double), y: (Double, Double), precision: Int, maxRanges: Option[Int]): Seq[IndexRange] =
    ranges(Seq((x._1, y._1, x._2, y._2)), precision, maxRanges)

  /**
    * Gets ranges
    *
    * @param xy sequence of bounding boxes, in the form of (xmin, ymin, xmax, ymax)
    * @param precision precision of the zvalues to consider, up to 64 bits
    * @param maxRanges rough upper bound on the number of ranges to return
    * @return
    */
  def ranges(xy: Seq[(Double, Double, Double, Double)],
             precision: Int = FullPrecision,
             maxRanges: Option[Int] = None): Seq[IndexRange]
}

trait SpaceTimeFillingCurve[T] {

  import SpaceFillingCurve.FullPrecision

  def lat: NormalizedDimension
  def lon: NormalizedDimension
  def time: NormalizedDimension
  def index(x: Double, y: Double, t: Long, lenient: Boolean = false): T
  def invert(i: T): (Double, Double, Long)

  def ranges(x: (Double, Double), y: (Double, Double), t: (Long, Long)): Seq[IndexRange] =
    ranges(Seq((x._1, y._1, x._2, y._2)), Seq(t), FullPrecision, None)

  def ranges(x: (Double, Double), y: (Double, Double), t: (Long, Long), precision: Int): Seq[IndexRange] =
    ranges(Seq((x._1, y._1, x._2, y._2)), Seq(t), precision, None)

  def ranges(x: (Double, Double),
             y: (Double, Double),
             t: (Long, Long),
             precision: Int,
             maxRanges: Option[Int]): Seq[IndexRange] =
    ranges(Seq((x._1, y._1, x._2, y._2)), Seq(t), precision, maxRanges)

  /**
    * Gets ranges
    *
    * @param xy sequence of bounding boxes, in the form of (xmin, ymin, xmax, ymax)
    * @param t sequence of time bounds, in the form of (tmin, tmax)
    * @param precision precision of the zvalues to consider, up to 64 bits
    * @param maxRanges rough upper bound on the number of ranges to return
    * @return
    */
  def ranges(xy: Seq[(Double, Double, Double, Double)],
             t: Seq[(Long, Long)],
             precision: Int = FullPrecision,
             maxRanges: Option[Int] = None): Seq[IndexRange]
}

object SpaceFillingCurve {
  val FullPrecision: Int = 64
}