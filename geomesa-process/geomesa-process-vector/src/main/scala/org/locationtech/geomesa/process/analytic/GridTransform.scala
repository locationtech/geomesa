/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.process.analytic

import org.locationtech.jts.geom.Envelope

class GridTransform(env: Envelope, xSize: Int, ySize: Int, isClamped: Boolean = true) {

  var dx: Double = env.getWidth / (xSize - 1)
  var dy: Double = env.getHeight / (ySize - 1)

  def x(i: Int): Double =
    if (i >= xSize - 1) env.getMaxX else env.getMinX + i.toDouble * dx

  def y(j: Int): Double =
    if (j >= ySize - 1) env.getMaxY else env.getMinY + j.toDouble * dy

  def i(x: Double): Int = {
    if (isClamped && x > env.getMaxX)
      xSize
    else if (isClamped && x < env.getMinX)
      -1
    else {
      var i = ((x - env.getMinX) / dx).toInt
      if (isClamped && i >= xSize)
        i = xSize - 1
      i
    }
  }

  def j(y: Double): Int = {
    if (isClamped && y > env.getMaxY)
      ySize
    else if (isClamped && y < env.getMinY)
      -1
    else {
      var j = ((y - env.getMinY) / dy).toInt
      if (isClamped && j >= ySize)
        j = ySize - 1
      j
    }
  }
}
