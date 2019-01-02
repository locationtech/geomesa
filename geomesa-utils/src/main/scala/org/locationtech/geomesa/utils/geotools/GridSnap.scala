/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.utils.geotools

import org.locationtech.jts.geom.{Envelope, Geometry}

import scala.math.abs

class GridSnap(env: Envelope, xSize: Int, ySize: Int) {

  lazy val envelope: Geometry = GeometryUtils.geoFactory.toGeometry(env)

  private val xMin = env.getMinX
  private val xMax = env.getMaxX
  private val yMin = env.getMinY
  private val yMax = env.getMaxY

  private val dx = (xMax - xMin) / xSize
  private val dy = (yMax - yMin) / ySize

  private val xOffset = xMin + dx / 2 // offset by half dx to get to center of grid cell
  private val yOffset = yMin + dy / 2 // offset by half dy to get to center of grid cell

  /**
   * Computes the X ordinate of the i'th grid column.
   *
   * @param i the index of a grid column
   * @return the X ordinate of the column
   */
  def x(i: Int): Double = xOffset + dx * i

  /**
   * Computes the Y ordinate of the i'th grid row.
   *
   * @param j the index of a grid row
   * @return the Y ordinate of the row
   */
  def y(j: Int): Double = yOffset + dy * j

  /**
   * Computes the column index of an X ordinate.
   *
   * @param x the X ordinate
   * @return the column index
   */
  def i(x: Double): Int = {
    if (x < xMin || x > xMax) { -1 } else {
      val i = math.floor((x - xMin) / dx).toInt
      // i == length check catches the upper bound
      if (i < 0 || i > xSize) { -1 } else if (i == xSize) { xSize - 1 } else { i }
    }
  }

  /**
   * Computes the column index of an Y ordinate.
   *
   * @param y the Y ordinate
   * @return the column index
   */
  def j(y: Double): Int = {
    if (y < yMin || y > yMax) { -1 } else {
      val i = math.floor((y - yMin) / dy).toInt
      // i == length check catches the upper bound
      if (i < 0 || i > ySize) { -1 } else if (i == ySize) { ySize - 1 } else { i }
    }
  }

  def snap(x: Double, y: Double): (Double, Double) = (this.x(i(x)), this.y(j(y)))

  /**
    * Generate a sequence of snapped points between two given snapped coordinates using Bresenham's Line Algorithm.
    * Will not return any duplicate points, and will always include the start and end points
    *
    * @param x0 x0
    * @param y0 y0
    * @param x1 x1
    * @param y1 y1
    * @return
    */
  def bresenhamLine(x0: Int, y0: Int, x1: Int, y1: Int): Iterator[(Int, Int)] = {
    val deltaX = abs(x1 - x0)
    val deltaY = abs(y1 - y0)
    if (deltaX == 0 && deltaY == 0) { Iterator.single((x0, y0)) } else {
      val stepX = if (x0 < x1) { 1 } else { -1 }
      val stepY = if (y0 < y1) { 1 } else { -1 }
      if (deltaX > deltaY) {
        val deltaError = deltaY.toDouble / deltaX
        var error = 0d
        Iterator.iterate((x0, y0)) { case (x, y) =>
          error += deltaError
          if (error >= 0.5) {
            error -= 1d
            (x + stepX, y + stepY)
          } else {
            (x + stepX, y)
          }
        }.take(deltaX)
      } else {
        val deltaError = deltaX.toDouble / deltaY
        var error = 0d
        Iterator.iterate((x0, y0)) { case (x, y) =>
          error += deltaError
          if (error >= 0.5) {
            error -= 1d
            (x + stepX, y + stepY)
          } else {
            (x, y + stepY)
          }
        }.take(deltaY)
      }
    }
  }
}
