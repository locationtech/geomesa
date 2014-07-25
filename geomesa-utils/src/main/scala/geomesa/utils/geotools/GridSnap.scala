/*
 * Copyright 2013 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package geomesa.utils.geotools

import com.vividsolutions.jts.geom.{Coordinate, Envelope}
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.data.simple.SimpleFeatureSource
import org.geotools.grid.oblong.Oblongs
import org.geotools.grid.{DefaultGridFeatureBuilder}
import org.geotools.referencing.crs.DefaultGeographicCRS
import scala.math.abs

class GridSnap(env: Envelope, xSize: Int, ySize: Int) {

  val dx = env.getWidth / (xSize - 1)
  val dy = env.getHeight / (ySize - 1)

  /**
   * Computes the X ordinate of the i'th grid column.
   * @param i the index of a grid column
   * @return the X ordinate of the column
   */
  def x(i: Int) =
    if (i >= xSize - 1) env.getMaxX
    else env.getMinX + i * dx

  /**
   * Computes the Y ordinate of the i'th grid row.
   * @param j the index of a grid row
   * @return the Y ordinate of the row
   */
  def y(j: Int) =
    if (j >= ySize - 1) env.getMaxY()
    else env.getMinY + j * dy

  /**
   * Computes the column index of an X ordinate.
   * @param v the X ordinate
   * @return the column index
   */
  def i(v: Double): Int = v match {
    case x if x > env.getMaxX => xSize
    case x if x < env.getMinX => -1
    case x =>
      val ret = (x - env.getMinX) / dx
      if (ret >= xSize) xSize - 1
      else ret.toInt
  }

  /**
   * Computes the column index of an Y ordinate.
   * @param v the Y ordinate
   * @return the column index
   */
  def j(v: Double): Int = v match {
    case y if y > env.getMaxY => ySize
    case y if y < env.getMinY => -1
    case y =>
      val ret = (y - env.getMinY) / dy
      if (ret >= ySize) ySize - 1
      else ret.toInt
  }

  /** Generate a Sequence of Coordinates between two given Snap Coordinates using Bresenham's Line Algorithm */
  def genBresenhamCoordSet(x0: Int, y0: Int, x1: Int, y1: Int): Set[Coordinate] = {
    val ( deltaX, deltaY ) = (abs(x1 - x0), abs(y1 - y0))
    if ((deltaX == 0) && (deltaY == 0)) Set[Coordinate](new Coordinate(x(x0), y(y0)))
    else {
      val ( stepX, stepY ) = (if (x0 < x1) 1 else -1, if (y0 < y1) 1 else -1)
      val (fX, fY) =  ( stepX * x1, stepY * y1 )
      def iter = new Iterator[Coordinate] {
        var (xT, yT) = (x0, y0)
        var error = (if (deltaX > deltaY) deltaX else -deltaY) / 2
        def next = {
          val errorT = error
          if(errorT > -deltaX){ error -= deltaY; xT += stepX }
          if(errorT < deltaY){ error += deltaX; yT += stepY }
          new Coordinate(x(xT), y(yT))
        }
        def hasNext = stepX * xT <= fX && stepY * yT <= fY
      }
      iter.toList.dropRight(1).toSet
    }
  }

  /** Generate a Set of Coordinates between two given Snap Coordinates which includes both start and end points*/
  def generateLineCoordSet(coordOne: Coordinate, coordTwo: Coordinate): Set[Coordinate] = {
    genBresenhamCoordSet(i(coordOne.x), j(coordOne.y), i(coordTwo.x), j(coordTwo.y)) + coordOne + coordTwo
  }

  /** return a SimpleFeatureSource grid the same size and extent as the bbox */
  lazy val generateCoverageGrid:SimpleFeatureSource = {
    val dxt = env.getWidth / xSize
    val dyt = env.getHeight / ySize
    val gridBounds = new ReferencedEnvelope(env.getMinX, env.getMaxX, env.getMinY, env.getMaxY, DefaultGeographicCRS.WGS84)
    val gridBuilder = new DefaultGridFeatureBuilder()
    val grid = Oblongs.createGrid(gridBounds, dxt, dyt, gridBuilder)
    grid
  }
}
