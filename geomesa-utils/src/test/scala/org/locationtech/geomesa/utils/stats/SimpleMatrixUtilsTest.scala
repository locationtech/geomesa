/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.utils.stats

import org.ejml.data.DenseMatrix64F
import org.ejml.simple.SimpleMatrix
import org.specs2.mutable.Specification
import org.locationtech.geomesa.utils.stats.SimpleMatrixUtils._

class SimpleMatrixUtilsTest extends Specification with StatTestHelper {

  implicit class extract(sm: SimpleMatrix) {
    def array: Array[Double] = sm.getMatrix.data
    def matrix: DenseMatrix64F = sm.getMatrix
  }

  "SimpleMatrixUtils" should {
    "overload +" >> {
      "matrix + scalar" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.array mustEqual Array(0d, 0d, 0d, 0d)
        val result = initial + 1d
        initial.array mustEqual Array(0d, 0d, 0d, 0d)
        result.array mustEqual Array(1d, 1d, 1d, 1d)
      }
      "scalar + matrix" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.array mustEqual Array(0d, 0d, 0d, 0d)
        val result = 1d + initial
        initial.array mustEqual Array(0d, 0d, 0d, 0d)
        result.array mustEqual Array(1d, 1d, 1d, 1d)
      }
      "matrix + matrix" >> {
        val initial0 = new SimpleMatrix(2, 2);
        initial0.set(1)
        val initial1 = new SimpleMatrix(2, 2);
        initial1.set(1)
        initial0.array mustEqual Array(1d, 1d, 1d, 1d)
        initial1.array mustEqual Array(1d, 1d, 1d, 1d)
        val result = initial0 + initial1
        initial0.array mustEqual Array(1d, 1d, 1d, 1d)
        initial1.array mustEqual Array(1d, 1d, 1d, 1d)
        result.array mustEqual Array(2d, 2d, 2d, 2d)
      }
    }
    "overload +=" >> {
      "matrix += scalar" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.array mustEqual Array(0d, 0d, 0d, 0d)
        initial += 1d
        initial.array mustEqual Array(1d, 1d, 1d, 1d)
      }
      "matrix += matrix" >> {
        val initial0 = new SimpleMatrix(2, 2)
        initial0.set(1d)
        val initial1 = new SimpleMatrix(2, 2)
        initial1.set(1d)
        initial0.array mustEqual Array(1d, 1d, 1d, 1d)
        initial1.array mustEqual Array(1d, 1d, 1d, 1d)
        initial0 += initial1
        initial0.array mustEqual Array(2d, 2d, 2d, 2d)
        initial1.array mustEqual Array(1d, 1d, 1d, 1d)
      }
    }
    "overload -" >> {
      "matrix - scalar" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.array mustEqual Array(0d, 0d, 0d, 0d)
        val result = initial - 1d
        initial.array mustEqual Array(0d, 0d, 0d, 0d)
        result.array mustEqual Array(-1d, -1d, -1d, -1d)
      }
      "scalar - matrix" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.set(1d)
        initial.array mustEqual Array(1d, 1d, 1d, 1d)
        val result = 1d - initial
        initial.array mustEqual Array(1d, 1d, 1d, 1d)
        result.array mustEqual Array(0d, 0d, 0d, 0d)
      }
      "matrix + matrix" >> {
        val initial0 = new SimpleMatrix(2, 2)
        initial0.set(-2)
        val initial1 = new SimpleMatrix(2, 2)
        initial1.set(1)
        initial0.array mustEqual Array(-2d, -2d, -2d, -2d)
        initial1.array mustEqual Array(1d, 1d, 1d, 1d)
        val result = initial0 - initial1
        initial0.array mustEqual Array(-2d, -2d, -2d, -2d)
        initial1.array mustEqual Array(1d, 1d, 1d, 1d)
        result.array mustEqual Array(-3d, -3d, -3d, -3d)
      }
    }
    "overload -=" >> {
      "matrix += scalar" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.array mustEqual Array(0d, 0d, 0d, 0d)
        initial -= 1d
        initial.array mustEqual Array(-1d, -1d, -1d, -1d)
      }
      "matrix += matrix" >> {
        val initial0 = new SimpleMatrix(2, 2)
        initial0.set(1d)
        val initial1 = new SimpleMatrix(2, 2)
        initial1.set(3d)
        initial0.array mustEqual Array(1d, 1d, 1d, 1d)
        initial1.array mustEqual Array(3d, 3d, 3d, 3d)
        initial0 -= initial1
        initial0.array mustEqual Array(-2d, -2d, -2d, -2d)
        initial1.array mustEqual Array(3d, 3d, 3d, 3d)
      }
    }
    "overload * (element-wise multiplication)" >> {
      "matrix * scalar" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.matrix.setData(Array(1d, 2d, 3d, 4d))
        val result = initial * 2d
        initial.array mustEqual Array(1d, 2d, 3d, 4d)
        result.array mustEqual Array(2d, 4d, 6d, 8d)
      }
      "scalar * matrix" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.matrix.setData(Array(1d, 3d, 9d, 12d))
        val result = 3 * initial
        initial.array mustEqual Array(1d, 3d, 9d, 12d)
        result.array mustEqual Array(3d, 9d, 27d, 36d)
      }
      "matrix * matrix" >> {
        val initial0 = new SimpleMatrix(2, 2)
        initial0.matrix.setData(Array(1d, 2d, 3d, 4d))
        val initial1 = new SimpleMatrix(2, 2)
        initial1.matrix.setData(Array(2d, 3d, 4d, 5d))
        val result = initial0 * initial1
        initial0.array mustEqual Array(1d, 2d, 3d, 4d)
        initial1.array mustEqual Array(2d, 3d, 4d, 5d)
        result.array mustEqual Array(2d, 6d, 12d, 20d)
      }
    }
    "overload *= (element-wise multiplication)" >> {
      "matrix *= scalar" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.matrix.setData(Array(1d, 2d, 3d, 4d))
        initial *= 2d
        initial.array mustEqual Array(2d, 4d, 6d, 8d)
      }
      "matrix * matrix" >> {
        val initial0 = new SimpleMatrix(2, 2)
        initial0.matrix.setData(Array(1d, 2d, 3d, 4d))
        val initial1 = new SimpleMatrix(2, 2)
        initial1.matrix.setData(Array(2d, 3d, 4d, 5d))
        initial0 *= initial1
        initial0.array mustEqual Array(2d, 6d, 12d, 20d)
        initial1.array mustEqual Array(2d, 3d, 4d, 5d)
      }
    }
    "overload / (element-wise division)" >> {
      "matrix / scalar" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.matrix.setData(Array(1d, 2d, 3d, 4d))
        val result = initial / 2d
        initial.array mustEqual Array(1d, 2d, 3d, 4d)
        result.array mustEqual Array(0.5d, 1d, 1.5d, 2d)
      }
      "scalar / matrix" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.matrix.setData(Array(12d, 6d, 4d, 3d))
        val result = 12 / initial
        initial.array mustEqual Array(12d, 6d, 4d, 3d)
        result.array mustEqual Array(1d, 2d, 3d, 4d)
      }
      "matrix / matrix" >> {
        val initial0 = new SimpleMatrix(2, 2)
        initial0.matrix.setData(Array(10d, 12d, 6d, 4d))
        val initial1 = new SimpleMatrix(2, 2)
        initial1.matrix.setData(Array(5d, 4d, 3d, 4d))
        val result = initial0 / initial1
        initial0.array mustEqual Array(10d, 12d, 6d, 4d)
        initial1.array mustEqual Array(5d, 4d, 3d, 4d)
        result.array mustEqual Array(2d, 3d, 2d, 1d)
      }
    }
    "overload /= (element-wise division)" >> {
      "matrix /= scalar" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.matrix.setData(Array(1d, 2d, 3d, 4d))
        initial /= 2d
        initial.array mustEqual Array(0.5d, 1d, 1.5d, 2d)
      }
      "matrix /= matrix" >> {
        val initial0 = new SimpleMatrix(2, 2)
        initial0.matrix.setData(Array(10d, 12d, 6d, 4d))
        val initial1 = new SimpleMatrix(2, 2)
        initial1.matrix.setData(Array(5d, 4d, 3d, 4d))
        initial0 /= initial1
        initial0.array mustEqual Array(2d, 3d, 2d, 1d)
        initial1.array mustEqual Array(5d, 4d, 3d, 4d)
      }
    }
    "overload ** (element-wise power)" >> {
      "matrix ** scalar" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.matrix.setData(Array(1d, 2d, 3d, 4d))
        val result = initial ** 2d
        initial.array mustEqual Array(1d, 2d, 3d, 4d)
        result.array mustEqual Array(1d, 4d, 9d, 16d)
      }
      "scalar ** matrix" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.matrix.setData(Array(1d, 2d, 3d, 4d))
        val result = 2 ** initial
        initial.array mustEqual Array(1d, 2d, 3d, 4d)
        result.array mustEqual Array(2d, 4d, 8d, 16d)
      }
      "matrix ** matrix" >> {
        val initial0 = new SimpleMatrix(2, 2)
        initial0.matrix.setData(Array(2d, 3d, 4d, 5d))
        val initial1 = new SimpleMatrix(2, 2)
        initial1.matrix.setData(Array(5d, 4d, 3d, 2d))
        val result = initial0 ** initial1
        initial0.array mustEqual Array(2d, 3d, 4d, 5d)
        initial1.array mustEqual Array(5d, 4d, 3d, 2d)
        result.array mustEqual Array(32d, 81d, 64d, 25d)
      }
    }
    "overload **= (element-wise division)" >> {
      "matrix **= scalar" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.matrix.setData(Array(1d, 2d, 3d, 4d))
        initial **= 2d
        initial.array mustEqual Array(1d, 4d, 9d, 16d)
      }
      "matrix **= matrix" >> {
        val initial0 = new SimpleMatrix(2, 2)
        initial0.matrix.setData(Array(2d, 3d, 4d, 5d))
        val initial1 = new SimpleMatrix(2, 2)
        initial1.matrix.setData(Array(5d, 4d, 3d, 2d))
        initial0 **= initial1
        initial0.array mustEqual Array(32d, 81d, 64d, 25d)
        initial1.array mustEqual Array(5d, 4d, 3d, 2d)
      }
    }
    "add diag convenience method" >> {
      "diag(scalar)" >> {
        val initial = new SimpleMatrix(2, 2)
        initial.array mustEqual Array(0d, 0d, 0d, 0d)
        val result = initial.diag(1)
        initial.array mustEqual Array(0d, 0d, 0d, 0d)
        result.array mustEqual Array(1d, 0d, 0d, 1d)
      }
    }
    "add |*| (matrix multiplication) convenience method" >> {
      "row vector |*| column vector" >> {
        val row = new SimpleMatrix(1, 3)
        row.matrix.setData(Array(1d, 2d, 3d))
        val col = new SimpleMatrix(3, 1)
        col.matrix.setData(Array(1d, 2d, 3d))
        val result = row |*| col
        result.array mustEqual Array(14d)
      }
      "col vector |*| row vector" >> {
        val row = new SimpleMatrix(1, 3)
        row.matrix.setData(Array(1d, 2d, 3d))
        val col = new SimpleMatrix(3, 1)
        col.matrix.setData(Array(1d, 2d, 3d))
        val result = col |*| row
        result.array mustEqual Array(1d, 2d, 3d, 2d, 4d, 6d, 3d, 6d, 9d)
      }
    }
    "add T (transpose) convenience method" >> {
      "row vector T" >> {
        val row = new SimpleMatrix(1, 3)
        row.matrix.setData(Array(1d, 2d, 3d))
        val col = row.T
        col.array mustEqual Array(1d, 2d, 3d)
        col.getNumCols mustEqual 1
        col.getNumRows mustEqual 3
      }
      "col vector T" >> {
        val col = new SimpleMatrix(3, 1)
        col.matrix.setData(Array(1d, 2d, 3d))
        val row = col.T
        row.array mustEqual Array(1d, 2d, 3d)
        row.getNumCols mustEqual 3
        row.getNumRows mustEqual 1
      }
    }
  }
}
