/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

/**
  * Maps a double within a known range to an Int in [0, bins)
  */
trait NormalizedDimension {

  /**
    * Min value considered for normalization range
    *
    * @return
    */
  def min: Double

  /**
    * Max value considered for normalizing
    *
    * @return
    */
  def max: Double

  /**
    * Max value to normalize to
    *
    * @return
    */
  def maxIndex: Int

  /**
    * Normalize the value
    *
    * @param x [min, max]
    * @return [0, maxIndex]
    */
  def normalize(x: Double): Int

  /**
    * Denormalize the value in bin x
    *
    * @param x [0, maxIndex]
    * @return [min, max]
    */
  def denormalize(x: Int): Double
}

object NormalizedDimension {

  class BitNormalizedDimension(val min: Double, val max: Double, precision: Int) extends NormalizedDimension {

    require(precision > 0 && precision < 32, "Precision (bits) must be in [1,31]")

    // (1L << precision) is equivalent to math.pow(2, precision).toLong
    private val bins = 1L << precision
    private val normalizer = bins / (max - min)
    private val denormalizer = (max - min) / bins

    override val maxIndex: Int = (bins - 1).toInt // note: call .toInt after subtracting 1 to avoid sign issues

    override def normalize(x: Double): Int =
      if (x >= max) { maxIndex } else { math.floor((x - min) * normalizer).toInt }

    override def denormalize(x: Int): Double =
      if (x >= maxIndex) { min + (maxIndex + 0.5d) * denormalizer } else { min + (x + 0.5d) * denormalizer }
  }

  case class NormalizedLat(precision: Int) extends BitNormalizedDimension(-90d, 90d, precision)

  case class NormalizedLon(precision: Int) extends BitNormalizedDimension(-180d, 180d, precision)

  case class NormalizedTime(precision: Int, override val max: Double) extends BitNormalizedDimension(0d, max, precision)


  // legacy normalization, doesn't correctly bin lower bound
  @deprecated("use BitNormalizedDimension instead")
  class SemiNormalizedDimension(val min: Double, val max: Double, precision: Long) extends NormalizedDimension {
    override val maxIndex: Int = precision.toInt
    override def normalize(x: Double): Int = math.ceil((x - min) / (max - min) * precision).toInt
    override def denormalize(x: Int): Double = if (x == 0) { min } else { (x - 0.5d) * (max - min) / precision + min }
  }

  @deprecated("use NormalizedLat instead")
  case class SemiNormalizedLat(precision: Long) extends SemiNormalizedDimension(-90d, 90d, precision)

  @deprecated("use NormalizedLon instead")
  case class SemiNormalizedLon(precision: Long) extends SemiNormalizedDimension(-180d, 180d, precision)

  @deprecated("use NormalizedTime instead")
  case class SemiNormalizedTime(precision: Long, override val max: Double)
      extends SemiNormalizedDimension(0d, max, precision)
}
