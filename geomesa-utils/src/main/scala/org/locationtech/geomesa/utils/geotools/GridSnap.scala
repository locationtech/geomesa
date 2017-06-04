/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.utils.geotools

import com.vividsolutions.jts.geom.{Coordinate, Envelope}
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.grid.DefaultGridFeatureBuilder
import org.geotools.grid.oblong.Oblongs
import org.geotools.referencing.crs.DefaultGeographicCRS

import scala.math.abs

class GridSnap(env: Envelope, xSize: Int, ySize: Int) {

  val dx = BigDecimal.apply(env.getWidth)  / xSize
  val dy = BigDecimal.apply(env.getHeight) / ySize
  val xOffset = env.getMinX + dx / 2 // offset by half dx to get to center of grid cell
  val yOffset = env.getMinY + dy / 2 // offset by half dy to get to center of grid cell

  /**
   * Computes the X ordinate of the i'th grid column.
   *
   * @param i the index of a grid column
   * @return the X ordinate of the column
   */
  def x(i: Int): Double = {
    val iInBounds = if (i < 0) 0 else if (i > xSize - 1) xSize - 1 else i
    (xOffset + iInBounds * dx).toDouble
  }

  /**
   * Computes the Y ordinate of the i'th grid row.
   *
   * @param j the index of a grid row
   * @return the Y ordinate of the row
   */
  def y(j: Int): Double = {
    val jInBounds = if (j < 0) 0 else if (j > ySize - 1) ySize - 1 else j
    (yOffset + jInBounds * dy).toDouble
  }

  /**
   * Computes the column index of an X ordinate.
   *
   * @param x the X ordinate
   * @return the column index
   */
  def i(x: Double): Int =
    if (x >= env.getMaxX) {
      xSize - 1
    } else if (x <= env.getMinX) {
      0
    } else {
      math.min(((x - env.getMinX) / dx).toInt, xSize - 1)
    }

  /**
   * Computes the column index of an Y ordinate.
   *
   * @param y the Y ordinate
   * @return the column index
   */
  def j(y: Double): Int =
    if (y >= env.getMaxY)  {
      ySize - 1
    } else if (y <= env.getMinY)  {
      0
    } else {
      math.min(((y - env.getMinY) / dy).toInt, ySize - 1)
    }

  def snap(x: Double, y: Double): (Double, Double) = (this.x(i(x)), this.y(j(y)))

  /** Generate a Sequence of Coordinates between two given Snap Coordinates using Bresenham's Line Algorithm */
  def genBresenhamCoordList(x0: Int, y0: Int, x1: Int, y1: Int): List[Coordinate] = {
    val ( deltaX, deltaY ) = (abs(x1 - x0), abs(y1 - y0))
    if ((deltaX == 0) && (deltaY == 0)) List[Coordinate]()
    else {
      val ( stepX, stepY ) = (if (x0 < x1) 1 else -1, if (y0 < y1) 1 else -1)
      val (fX, fY) =  ( stepX * x1, stepY * y1 )
      def iter = new Iterator[Coordinate] {
        var (xT, yT) = (x0, y0)
        var error = (if (deltaX > deltaY) deltaX else -deltaY) / 2
        def next() = {
          val errorT = error
          if(errorT > -deltaX){ error -= deltaY; xT += stepX }
          if(errorT < deltaY){ error += deltaX; yT += stepY }
          new Coordinate(x(xT), y(yT))
        }
        def hasNext = stepX * xT <= fX && stepY * yT <= fY
      }
      iter.toList.dropRight(1).distinct
    }
  }

  /** Generate a Set of Coordinates between two given Snap Coordinates which includes both start and end points*/
  def generateLineCoordSet(coordOne: Coordinate, coordTwo: Coordinate): Set[Coordinate] = {
    genBresenhamCoordList(i(coordOne.x), j(coordOne.y), i(coordTwo.x), j(coordTwo.y)).toSet + coordOne + coordTwo
  }

  /** Generate an ordered List of Coordinates between two given Snap Coordinates which includes both start and end points*/
  def generateLineCoordList(coordOne: Coordinate, coordTwo: Coordinate): List[Coordinate] = {
    (coordOne +: genBresenhamCoordList(i(coordOne.x), j(coordOne.y), i(coordTwo.x), j(coordTwo.y)) :+ coordTwo).distinct
  }

  /** return a SimpleFeatureSource grid the same size and extent as the bbox */
  lazy val generateCoverageGrid: SimpleFeatureSource = {
    val dxt = env.getWidth / xSize
    val dyt = env.getHeight / ySize
    val gridBounds = new ReferencedEnvelope(env.getMinX, env.getMaxX, env.getMinY, env.getMaxY, DefaultGeographicCRS.WGS84)
    val gridBuilder = new DefaultGridFeatureBuilder()
    val grid = Oblongs.createGrid(gridBounds, dxt, dyt, gridBuilder)
    grid
  }
}
