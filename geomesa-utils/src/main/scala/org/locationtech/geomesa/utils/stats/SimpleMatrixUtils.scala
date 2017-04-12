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
    def **(b: SimpleMatrix) = a.elementPower(b)

    def **=(b: Double): Unit = CommonOps.elementPower(a, b, a)
    def **=(b: SimpleMatrix): Unit = CommonOps.elementPower(a, b, a)

    def diag(v: Double): SimpleMatrix = {
      val m = new SimpleMatrix(a)
      (0 until Math.min(m.getNumRows, m.getNumCols)).foreach(i => m.set(i, i, v))
      m
    }

    def |*|(b: SimpleMatrix): SimpleMatrix = a.mult(b)

    def T: SimpleMatrix = a.transpose
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
}
