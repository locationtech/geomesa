/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DensityProcessTest extends Specification {

  "DensityProcess" should {
    "flip a square grid" in {
      val grid = Array.tabulate(10, 10) { case (x, y) => s"$x.$y".toFloat }
      val inverted = DensityProcess.flipXY(grid)

      inverted must haveLength(10)
      inverted(0) mustEqual Array(0.9f, 1.9f, 2.9f, 3.9f, 4.9f, 5.9f, 6.9f, 7.9f, 8.9f, 9.9f)
      inverted(1) mustEqual Array(0.8f, 1.8f, 2.8f, 3.8f, 4.8f, 5.8f, 6.8f, 7.8f, 8.8f, 9.8f)
      inverted(2) mustEqual Array(0.7f, 1.7f, 2.7f, 3.7f, 4.7f, 5.7f, 6.7f, 7.7f, 8.7f, 9.7f)
      inverted(3) mustEqual Array(0.6f, 1.6f, 2.6f, 3.6f, 4.6f, 5.6f, 6.6f, 7.6f, 8.6f, 9.6f)
      inverted(4) mustEqual Array(0.5f, 1.5f, 2.5f, 3.5f, 4.5f, 5.5f, 6.5f, 7.5f, 8.5f, 9.5f)
      inverted(5) mustEqual Array(0.4f, 1.4f, 2.4f, 3.4f, 4.4f, 5.4f, 6.4f, 7.4f, 8.4f, 9.4f)
      inverted(6) mustEqual Array(0.3f, 1.3f, 2.3f, 3.3f, 4.3f, 5.3f, 6.3f, 7.3f, 8.3f, 9.3f)
      inverted(7) mustEqual Array(0.2f, 1.2f, 2.2f, 3.2f, 4.2f, 5.2f, 6.2f, 7.2f, 8.2f, 9.2f)
      inverted(8) mustEqual Array(0.1f, 1.1f, 2.1f, 3.1f, 4.1f, 5.1f, 6.1f, 7.1f, 8.1f, 9.1f)
      inverted(9) mustEqual Array(0.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f, 9.0f)
    }
    "flip a rectangular grid" in {
      val grid = Array.tabulate(6, 4) { case (x, y) => s"$x.$y".toFloat }
      val inverted = DensityProcess.flipXY(grid)

      inverted must haveLength(4)
      inverted(0) mustEqual Array(0.3f, 1.3f, 2.3f, 3.3f, 4.3f, 5.3f)
      inverted(1) mustEqual Array(0.2f, 1.2f, 2.2f, 3.2f, 4.2f, 5.2f)
      inverted(2) mustEqual Array(0.1f, 1.1f, 2.1f, 3.1f, 4.1f, 5.1f)
      inverted(3) mustEqual Array(0.0f, 1.0f, 2.0f, 3.0f, 4.0f, 5.0f)
    }
  }

  def printGrid[T](grid: Array[Array[T]]): Unit = {
    grid.indices.foreach { y =>
      grid(0).indices.foreach { x =>
        print(grid(y)(x) + " ")
      }
      println
    }
  }
}
