/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.ejml.data.DenseMatrix64F
import org.ejml.ops.CommonOps
import org.ejml.simple.SimpleMatrix
import org.locationtech.geomesa.utils.stats.SimpleMatrixUtils._
import org.opengis.feature.simple.SimpleFeature

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap
import scala.Array._

class DescriptiveStats(val attributes: Seq[Int]) extends Stat with Serializable {

  override type S = DescriptiveStats

  private[stats] val size = attributes.size
  private[stats] val size_squared = size * size

  private[stats] var _count: Long = _
  private[stats] val _min: SimpleMatrix = new SimpleMatrix(size, 1)
  private[stats] val _max: SimpleMatrix = new SimpleMatrix(size, 1)
  private[stats] val _sum: SimpleMatrix = new SimpleMatrix(size, 1)
  private[stats] val _mean: SimpleMatrix = new SimpleMatrix(size, 1)
  private[stats] val _m2n: SimpleMatrix = new SimpleMatrix(size, 1)
  private[stats] val _m3n: SimpleMatrix = new SimpleMatrix(size, 1)
  private[stats] val _m4n: SimpleMatrix = new SimpleMatrix(size, 1)
  private[stats] val _c2: SimpleMatrix = new SimpleMatrix(size, size)

  clear()

  override def clear(): Unit = {
    _count = 0
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

  def min: Array[Double] = (if (isEmpty) _max else _min).getMatrix.data.clone()
  def max: Array[Double] = (if (isEmpty) _min else _max).getMatrix.data.clone()
  def bounds: Array[(Double, Double)] = min.zip(max)

  def sum: Array[Double] = _sum.getMatrix.data.clone()

  def mean: Array[Double] = requireCount(1) { _mean }

  def m2: Array[Double] = requireCount(1) { _m2n / _count }
  def m3: Array[Double] = requireCount(1) { _m3n / _count }
  def m4: Array[Double] = requireCount(1) { _m4n / _count }

  def pvar: Array[Double] = requireCount(1) { _m2n / _count }
  def psdev: Array[Double] = requireCount(1) { (_m2n / _count) ** 0.5 }
  def pskew: Array[Double] = requireCount(1) { Math.sqrt(_count) * _m3n / (_m2n ** 1.5)  }
  def pkurt: Array[Double] = requireCount(1) { _m4n * _count / (_m2n ** 2.0) }
  def pekurt: Array[Double] = pkurt.map(_ -3.0 )

  def svar: Array[Double] = requireCount(2) { _m2n / (_count - 1) }
  def ssdev: Array[Double] = requireCount(2) { (_m2n / (_count - 1)) ** 0.5 }
  def sskew: Array[Double] = requireCount(3) { _m3n * (_count * Math.sqrt(_count - 1) / (_count - 2)) / (_m2n ** 1.5) }
  def skurt: Array[Double] = requireCount(4) {
    _m4n * (count * (_count + 1) * (_count - 1) / (_count - 2) / (_count - 3)) / (_m2n ** 2.0)
  }
  def sekurt: Array[Double] = skurt.map(_ -3.0 )
  
  def c2: Array[Double] = requireCount(2, size_squared) { _c2 }

  def pcov: Array[Double] = requireCount(2, size_squared) { _c2 / _count}
  def pcor: Array[Double] = requireCount(2, size_squared) {
    val mn2_sqrt = _m2n ** 0.5; (_c2 / (mn2_sqrt |*| mn2_sqrt.T)).diag(1.0) }

  def scov: Array[Double] = requireCount(2, size_squared) { _c2 / (_count -1 ) }
  def scor: Array[Double] = pcor /* population and sample calculations are equal w/ df term cancellation */

  private def requireCount(count: Int, length: Int = size)(op: => SimpleMatrix):Array[Double] =
    if (_count < count) Array.fill(length)(Double.NaN) else op.getMatrix.data.clone()

  override def observe(sf: SimpleFeature): Unit =
    observe(attributes.map(sf.getAttribute(_).asInstanceOf[Number]).toArray)

  def observe(values: Array[Number]): Unit = {
    if (values.forall(_ != null)) {
      val values_d = values.map(_.doubleValue)
      if (values_d.forall(v => v == v)) {
        val values_v = new SimpleMatrix(size, 1, true, values_d: _*)

        if (_count > 0) {

          updateMinMax(_min, _max, values_v)

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

          _c2 += (delta |*| delta.T * (n_i * r))

        } else {
          _count = 1
          _min.set(values_v)
          _max.set(values_v)
          _sum.set(values_v)
          _mean.set(values_v)
        }
      }
    }
  }

  override def unobserve(sf: SimpleFeature): Unit = {}

  override def +(that: DescriptiveStats): DescriptiveStats = {
    val stats = new DescriptiveStats(attributes)
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
        (n1_squared * that._m2n + n2_squared * _m2n) * A_squared * 6d +
        (n1 * that._m3n - n2 * _m3n) * A * 4d

      _m3n += that._m3n +
        n_product * (n1 - n2) * delta * A_squared +
        ( n1 * that._m2n - n2 * _m2n) * A * 3d

      _m2n += that._m2n +
        n_product * delta * A

      _c2 += (that._c2 + (delta |*| delta.T) * (n_product * n_i))

      _mean += n2 * A

      _sum += that._sum

      _min.min(that._min)
      _max.max(that._max)

      _count += that._count
    }
  }

  override def isEmpty: Boolean = _count < 1

  override def isEquivalent(other: Stat): Boolean = other match {
    case that: DescriptiveStats =>
      attributes == that.attributes &&
        _count  == that._count &&
        _min.isIdentical(that._min, 1e-9) &&
        _max.isIdentical(that._max, 1e-9) &&
        _sum.isIdentical(that._sum, 1e-9) &&
        _mean.isIdentical(that._mean, 1e-9) &&
        _sum.isIdentical(that._sum, 1e-9) &&
        _m2n.isIdentical(that._m2n, 1e-9) &&
        _m3n.isIdentical(that._m3n, 1e-9) &&
        _m4n.isIdentical(that._m4n, 1e-9) &&
        _c2.isIdentical(that._c2, 1e-9)
    case _ => false
  }

  override def toJson: String = {
    val map = if (isEmpty) {
      Map("count" -> 0)
    } else {
      ListMap("count" -> count,
        "minimum" -> min,
        "maximum" -> max,
        "mean" -> mean,
        "population_variance" -> pvar,
        "population_standard_deviation" -> psdev,
        "population_skewness" -> pskew,
        "population_kurtosis" -> pkurt,
        "population_excess_kurtosis" -> pekurt,
        "sample_variance" -> svar,
        "sample_standard_deviation" -> ssdev,
        "sample_skewness" -> sskew,
        "sample_kurtosis" -> skurt,
        "sample_excess_kurtosis" -> sekurt,
        "population_covariance" -> pcov,
        "population_correlation" -> pcor,
        "sample_covariance" -> scov,
        "sample_correlation" -> scor)
    }
    Stat.JSON.toJson(map.asJava)
  }
}

object SimpleMatrixUtils {

  implicit def toDenseMatrix64F(sm: SimpleMatrix): DenseMatrix64F = sm.getMatrix

  implicit class SimpleMatrixOps(a: SimpleMatrix) {

    def +(b: Double): SimpleMatrix = a.plus(b)
    def +(b: SimpleMatrix): SimpleMatrix = a.plus(b)

    def +=(b: Double): Unit = CommonOps.add(a, b, a)
    def +=(b: SimpleMatrix): Unit = CommonOps.add(a, b, a)

    def -(b: Double): SimpleMatrix = a.minus(b)
    def -(b: SimpleMatrix): SimpleMatrix = a.minus(b)

    def -=(b: Double): Unit = CommonOps.subtract(a, b, a)
    def -=(b: SimpleMatrix): Unit = CommonOps.subtract(a, b, a)

    def *(b: Double): SimpleMatrix = a.scale(b)
    def *(b: SimpleMatrix): SimpleMatrix = a.elementMult(b)

    def *=(b: Double): Unit = CommonOps.scale(b, a, a)
    def *=(b: SimpleMatrix): Unit = CommonOps.elementMult(a, b, a)

    def /(b: Double): SimpleMatrix = a.divide(b)
    def /(b: SimpleMatrix): SimpleMatrix = a.elementDiv(b)

    def /=(b: Double): Unit = CommonOps.divide(a, b, a)
    def /=(b: SimpleMatrix): Unit = CommonOps.elementDiv(a, b, a)

    def **(b: Double): SimpleMatrix = a.elementPower(b)
    def **=(b: Double): Unit = CommonOps.elementPower(a, b, a)

    def diag(v: Double): SimpleMatrix = {
      val m = new SimpleMatrix(a)
      (0 until Math.min(m.getNumRows, m.getNumCols)).foreach(i => m.set(i, i, v))
      m
    }

    def |*|(b: SimpleMatrix): SimpleMatrix = a.mult(b)

    def T: SimpleMatrix = a.transpose

    def min(values: SimpleMatrix): Unit = {
      val n = values.getNumElements
      var i = 0
      while (i < n) {
        val v = values.get(i)
        val min = a.get(i)
        if (v < min) {
          a.set(i, v)
        }
        i += 1
      }
    }
    def max(values: SimpleMatrix): Unit = {
      val n = values.getNumElements
      var i = 0
      while (i < n) {
        val v = values.get(i)
        val max = a.get(i)
        if (v > max) {
          a.set(i, v)
        }
        i += 1
      }
    }
  }

  implicit class DoubleOps(a: Double) {
    def +(b: SimpleMatrix): SimpleMatrix = b.plus(a)
    def -(b: SimpleMatrix): SimpleMatrix = {
      val c = new SimpleMatrix(b.getNumRows, b.getNumCols)
      CommonOps.subtract(a, b, c)
      c
    }
    def *(b: SimpleMatrix): SimpleMatrix = b.scale(a)
    def /(b: SimpleMatrix): SimpleMatrix = {
      val c = new SimpleMatrix(b.getNumRows, b.getNumCols)
      CommonOps.divide(a, b, c)
      c
    }
    def **(b: SimpleMatrix): SimpleMatrix = {
      val c = new SimpleMatrix(b.getNumRows, b.getNumCols)
      CommonOps.elementPower(a, b, c)
      c
    }
  }

  def updateMinMax(mins: SimpleMatrix, maxs: SimpleMatrix, values: SimpleMatrix): Unit = {
    val n = values.getNumElements
    var i = 0
    while (i < n) {
      val v = values.get(i)
      val min = mins.get(i)
      val max = maxs.get(i)
      if (v < min) {
        mins.set(i, v)
      } else if (v > max) {
        maxs.set(i, v)
      }
      i += 1
    }
  }
}
