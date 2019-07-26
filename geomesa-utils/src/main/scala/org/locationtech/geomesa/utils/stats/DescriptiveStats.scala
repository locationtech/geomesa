/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.stats

import org.ejml.simple.SimpleMatrix
import org.locationtech.geomesa.utils.stats.SimpleMatrixUtils._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.collection.immutable.ListMap
import scala.Array._

class DescriptiveStats private [stats] (val sft: SimpleFeatureType,
                                        val properties: Seq[String]) extends Stat with Serializable {

  override type S = DescriptiveStats

  @deprecated("properties")
  lazy val attributes: Seq[Int] = properties.map(sft.indexOf)

  private val indices = properties.map(sft.indexOf).toArray

  private val size = properties.size
  private val size_squared = size * size

  private [stats] var _count = 0L
  private [stats] val _min:  SimpleMatrix = new SimpleMatrix(size, 1)
  private [stats] val _max:  SimpleMatrix = new SimpleMatrix(size, 1)
  private [stats] val _sum:  SimpleMatrix = new SimpleMatrix(size, 1)
  private [stats] val _mean: SimpleMatrix = new SimpleMatrix(size, 1)
  private [stats] val _m2n:  SimpleMatrix = new SimpleMatrix(size, 1)
  private [stats] val _m3n:  SimpleMatrix = new SimpleMatrix(size, 1)
  private [stats] val _m4n:  SimpleMatrix = new SimpleMatrix(size, 1)
  private [stats] val _c2:   SimpleMatrix = new SimpleMatrix(size, size)

  clear()

  override def clear(): Unit = {
    _count = 0L
    _min.set(java.lang.Double.MAX_VALUE)
    _max.set(0d - java.lang.Double.MAX_VALUE)
    _sum.set(0d)
    _mean.set(1d)
    _m2n.set(0d)
    _m3n.set(0d)
    _m4n.set(0d)
    _c2.set(0d)
  }

  def copyFrom(that: DescriptiveStats): Unit = {
    _count = that._count
    _min.set(that._min)
    _max.set(that._max)
    _sum.set(that._sum)
    _mean.set(that._mean)
    _m2n.set(that._m2n)
    _m3n.set(that._m3n)
    _m4n.set(that._m4n)
    _c2.set(that._c2)
  }

  def count: Long = _count

  def minimum: Array[Double] = (if (isEmpty) _max else _min).getMatrix.data.clone()

  def maximum: Array[Double] = (if (isEmpty) _min else _max).getMatrix.data.clone()

  def bounds: Array[(Double, Double)] = minimum.zip(maximum)

  def sum: Array[Double] = _sum.getMatrix.data.clone()

  def mean: Array[Double] = requireCount(1) { _mean }

  def centralMoment2: Array[Double] = requireCount(1) { _m2n / _count }

  def centralMoment3: Array[Double] = requireCount(1) { _m3n / _count }

  def centralMoment4: Array[Double] = requireCount(1) { _m4n / _count }

  def populationVariance: Array[Double] = requireCount(1) { _m2n / _count }

  def populationStandardDeviation: Array[Double] = requireCount(1) { (_m2n / _count) ** 0.5 }

  def populationSkewness: Array[Double] = requireCount(1) { Math.sqrt(_count) * _m3n / (_m2n ** 1.5)  }

  def populationKurtosis: Array[Double] = requireCount(1) { _m4n * _count / (_m2n ** 2.0) }

  def populationExcessKurtosis: Array[Double] = populationKurtosis.map(_ - 3.0 )

  def sampleVariance: Array[Double] = requireCount(2) { _m2n / (_count - 1) }

  def sampleStandardDeviation: Array[Double] = requireCount(2) { (_m2n / (_count - 1)) ** 0.5 }

  def sampleSkewness: Array[Double] = requireCount(3) {
    _m3n * (_count * Math.sqrt(_count - 1) / (_count - 2)) / (_m2n ** 1.5)
  }

  def sampleKurtosis: Array[Double] = requireCount(4) {
    _m4n * (count * (_count + 1) * (_count - 1) / (_count - 2) / (_count - 3)) / (_m2n ** 2.0)
  }

  def sampleExcessKurtosis: Array[Double] = sampleKurtosis.map(_ - 3.0 )

  def coMoment2: Array[Double] = requireCount(2, size_squared) { _c2 }

  def populationCovariance: Array[Double] = requireCount(2, size_squared) { _c2 / _count }

  def populationCorrelation: Array[Double] = requireCount(2, size_squared) {
    val mn2_sqrt = _m2n ** 0.5; (_c2 / (mn2_sqrt |*| mn2_sqrt.T)).diag(1.0)
  }

  def sampleCovariance: Array[Double] = requireCount(2, size_squared) { _c2 / (_count -1 ) }

  /* population and sample calculations are equal w/ df term cancellation */
  def sampleCorrelation: Array[Double] = populationCorrelation

  private def requireCount(count: Int, length: Int = size)(op: => SimpleMatrix): Array[Double] =
    if (_count < count) Array.fill(length)(Double.NaN) else op.getMatrix.data.clone()

  override def observe(sf: SimpleFeature): Unit = {
    val values = Array.newBuilder[Double]
    values.sizeHint(indices.length)

    var i = 0
    while (i < indices.length) {
      sf.getAttribute(indices(i)) match {
        case n: Number =>
          val double = n.doubleValue()
          if (double != double) {
            return // NaN, short-circuit evaluation
          }
          values += double

        case null => return // short-circuit evaluation

        case n => throw new IllegalArgumentException(s"Not a number: $n")
      }
      i += 1
    }

    val values_v = new SimpleMatrix(size, 1, true, values.result(): _*)

    if (_count < 1) {
      _count = 1
      _min.set(values_v)
      _max.set(values_v)
      _sum.set(values_v)
      _mean.set(values_v)
    } else {

      _sum += values_v

      val r = _count
      val n = { _count += 1; _count }
      val n_i = 1d / n

      val delta = values_v - _mean

      val A = delta * n_i
      _mean += A

      _m4n += A * (A * A * delta * r * (n * (n - 3d) + 3d) + A * _m2n * 6d - _m3n * 4d)

      val B = values_v - _mean
      _m3n += A * (B * delta * (n - 2d) - _m2n * 3d)
      _m2n += delta * B


      /* optimize original code (below) by special handling of diagonal and reflection about it
       * _c2 += (delta |*| delta.T * (n_i * r))
       */
      val coef = n_i * r
      var ri = 0
      while (ri < size) {
        _c2.set(ri, ri, _m2n.get(ri)) // c2 diagonal is equal to m2n
        val rd = delta.get(ri)
        var ci = ri + 1  // traverse upper diagonal
        while (ci < size) {
          val c2 = _c2.get(ri, ci) + rd * delta.get(ci) * coef
          _c2.set(ri, ci, c2) // set upper diagonal
          _c2.set(ci, ri, c2) // set lower diagonal
          ci += 1
        }
        ri += 1
      } // c2 update

      var i = 0
      while (i < size) {
        val v = values_v.get(i)
        if (v > _max.get(i)) {
          _max.set(i, v)
        } else if (v < _min.get(i)) { // 'else if' optimization due to how min/max set when _count == 1 (below)
          _min.set(i, v)
        }
        i += 1
      } // min/max update

    }
  }

  override def unobserve(sf: SimpleFeature): Unit = {}

  override def +(that: DescriptiveStats): DescriptiveStats = {
    val stats = new DescriptiveStats(sft, properties)
    if (that.isEmpty) {
      stats.copyFrom(this)
    } else if (this.isEmpty) {
      stats.copyFrom(that)
    } else {
      stats.copyFrom(this)
      stats += that
    }
    stats
  }

  override def +=(that: DescriptiveStats): Unit = {
    if (this == that)
      return
    if (that.isEmpty)
      return
    if (this.isEmpty) {
      copyFrom(that)
    } else {

      val n1 = this._count
      val n2 = that._count
      val n1_squared = n1 * n1
      val n2_squared = n2 * n2
      val n_product = n1 * n2
      val n = n1 + n2
      val n_i = 1d / n

      val delta = that._mean - this._mean

      val A = delta * n_i
      val A_squared = A * A

      _m4n += that._m4n +
        n_product * (n1_squared - n_product + n2_squared) * delta * A * A_squared +
        (that._m2n * n1_squared + _m2n * n2_squared) * A_squared * 6d +
        (that._m3n * n1 - _m3n * n2) * A * 4d

      _m3n += that._m3n +
        n_product * (n1 - n2) * delta * A_squared +
        (that._m2n * n1 - _m2n * n2) * A * 3d

      _m2n += that._m2n +
        delta * A * n_product

      /* optimize original code (below) by special handling of diagonal and reflection about it
       * _c2 += (that._c2 + (delta |*| delta.T) * (n_product * n_i))
       */
      val coef = n_product * n_i
      var ri = 0
      while (ri < size) {
        _c2.set(ri, ri, _m2n.get(ri)) // c2 diagonal is equal to m2n
        val rd = delta.get(ri)
        var ci = ri + 1 // traverse upper diagonal
        while (ci < size) {
          val c2 = _c2.get(ri, ci) + that._c2.get(ri,ci) + rd * delta.get(ci) * coef
          _c2.set(ri, ci, c2) // set upper diagonal
          _c2.set(ci, ri, c2) // set lower diagonal
          ci += 1
        }
        ri += 1
      } // c2 update

      _mean += A * n2

      _sum += that._sum

      var i = 0
      while (i < size) {
        val min = that._min.get(i)
        val max = that._max.get(i)
        if (min < _min.get(i)) {
          _min.set(i, min)
        }
        if (max > _max.get(i)) {
          _max.set(i, max)
        }
        i += 1
      }  // min/max update

      _count += that._count
    }
  }

  override def isEmpty: Boolean = _count < 1

  override def isEquivalent(other: Stat): Boolean = other match {
    case that: DescriptiveStats =>
      properties == that.properties &&
        _count  == that._count &&
        _min.isIdenticalWithinTolerances(that._min, 1e-6, 1e-12) &&
        _max.isIdenticalWithinTolerances(that._max, 1e-6, 1e-12) &&
        _sum.isIdenticalWithinTolerances(that._sum, 1e-6, 1e-12) &&
        _mean.isIdenticalWithinTolerances(that._mean, 1e-6, 1e-12) &&
        _m2n.isIdenticalWithinTolerances(that._m2n, 1e-6, 1e-12) &&
        _m3n.isIdenticalWithinTolerances(that._m3n, 1e-6, 1e-12) &&
        _m4n.isIdenticalWithinTolerances(that._m4n, 1e-6, 1e-12) &&
        _c2.isIdenticalWithinTolerances(that._c2, 1e-6, 1e-12)
    case _ => false
  }

  override def toJsonObject: Map[String, Any] =
    if (isEmpty) {
      Map("count" -> 0)
    } else {
      ListMap(
        "count" -> count,
        "minimum" -> minimum,
        "maximum" -> maximum,
        "mean" -> mean,
        "population_variance" -> populationVariance,
        "population_standard_deviation" -> populationStandardDeviation,
        "population_skewness" -> populationSkewness,
        "population_kurtosis" -> populationKurtosis,
        "population_excess_kurtosis" -> populationExcessKurtosis,
        "sample_variance" -> sampleVariance,
        "sample_standard_deviation" -> sampleStandardDeviation,
        "sample_skewness" -> sampleSkewness,
        "sample_kurtosis" -> sampleKurtosis,
        "sample_excess_kurtosis" -> sampleExcessKurtosis,
        "population_covariance" -> populationCovariance,
        "population_correlation" -> populationCorrelation,
        "sample_covariance" -> sampleCovariance,
        "sample_correlation" -> sampleCorrelation
      )
    }
}
