/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

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
    def **(b: SimpleMatrix): SimpleMatrix = a.elementPower(b)

    def **=(b: Double): Unit = CommonOps.elementPower(a, b, a)
    def **=(b: SimpleMatrix): Unit = CommonOps.elementPower(a, b, a)

    def diag(v: Double): SimpleMatrix = {
      val m = new SimpleMatrix(a)
      (0 until Math.min(m.getNumRows, m.getNumCols)).foreach(i => m.set(i, i, v))
      m
    }

    def |*|(b: SimpleMatrix): SimpleMatrix = a.mult(b)

    def T: SimpleMatrix = a.transpose

    def isIdenticalWithinTolerances(b: SimpleMatrix, rel_tol: Double = 1e-9, abs_tol: Double = 1e-15): Boolean = {
      import java.lang.{Double => jDouble}
      if (a.numRows != b.numRows || a.numCols != b.numCols) {
        return false
      }
      require(rel_tol >= 0 && abs_tol >=0, "Tolerance must be greater than or equal to zero.")
      val length = a.getNumElements
      var i = 0

      while (i < length) {
        val va = a.get(i)
        val vb = b.get(i)
        val va_nan = va != va     /* quick NaN test */
        val vb_nan = vb != vb     /* quick NaN test */
        if (va_nan || vb_nan) {   /* if either NaN */
          if (va_nan != vb_nan) { /* then both should be NaN */
            return false
          }
        } else {
          val va_inf = jDouble.isInfinite(va)
          val vb_inf = jDouble.isInfinite(vb)
          if (va_inf || vb_inf) {
            if (va != vb) {
              return false
            }
          } else {
            /* check absolute tolerance, important for low magnitude values (0) */
            val diff = Math.abs(va - vb)
            if (diff > abs_tol) {
              val va_abs = Math.abs(va)
              val vb_abs = Math.abs(vb)
              /* check relative tolerance, important for high magnitude values */
              if (diff > Math.max(va_abs, vb_abs) * rel_tol) {
                return false
              }
            }
          }
        }
        i += 1
      }
      true
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
}
