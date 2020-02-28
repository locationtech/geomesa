/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geotools

import java.awt.image.BufferedImage

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.jts.geom._

import scala.annotation.tailrec
import scala.util.control.NonFatal

/**
  * Renders geometries to a fixed-size grid of pixels
  *
  * @param env the rendering envelope
  * @param xSize x pixel count
  * @param ySize y pixel count
  */
class RenderingGrid(env: Envelope, xSize: Int, ySize: Int) extends LazyLogging {

  private val grid = new GridSnap(env, xSize, ySize)
  private val pixels = scala.collection.mutable.Map.empty[(Int, Int), Double].withDefaultValue(0d)

  private val xMin = env.getMinX
  private val xMax = env.getMaxX

  private val wide = xMax - xMin > 360d

  private var count = 0L

  /**
    * Render a point
    *
    * @param point geometry
    * @param weight weight
    */
  def render(point: Point, weight: Double): Unit = {
    val j = grid.j(point.getY)
    if (j != -1) {
      translate(point.getX).foreach(i => pixels(i, j) += weight)
    }
    count += 1
  }

  /**
    * Render a multi-point
    *
    * @param multiPoint geometry
    * @param weight weight
    */
  def render(multiPoint: MultiPoint, weight: Double): Unit = {
    var i = 0
    while (i < multiPoint.getNumGeometries) {
      render(multiPoint.getGeometryN(i).asInstanceOf[Point], weight)
      i += 1
    }
    count += (1 - i)
  }

  /**
    * Render a line string
    *
    * @param lineString geometry
    * @param weight weight
    */
  def render(lineString: LineString, weight: Double): Unit = {
    if (lineString.getNumPoints > 0) {
      var iN, jN = -1 // track the last pixel we've written to avoid double-counting

      // our working coordinates
      var p0: Coordinate = null
      var i0: Seq[Int] = null
      var j0: Int = -1
      var p1 = lineString.getCoordinateN(0)
      var i1 = translate(p1.x)
      var j1 = grid.j(p1.y)

      var n = 1
      while (n < lineString.getNumPoints) {
        // increment to the next pair of points
        p0 = p1
        i0 = i1
        j0 = j1
        p1 = lineString.getCoordinateN(n)
        i1 = translate(p1.x)
        j1 = grid.j(p1.y)

        if (i0.isEmpty || j0 == -1 || i1.isEmpty || j1 == -1) {
          // line is not entirely contained in the grid region
          // find the intersection of the line segment with the grid region
          try {
            val intersection = GeometryUtils.geoFactory.createLineString(Array(p0, p1)).intersection(grid.envelope)
            if (!intersection.isEmpty) {
              render(intersection, weight)
              count -= 1 // don't double count
            }
          } catch {
            case NonFatal(e) => logger.error(s"Error intersecting line string [$p0 $p1] with ${grid.envelope}", e)
          }
        } else {
          val bresenham = grid.bresenhamLine(i0.head, j0, i1.head, j1)
          // check the first point for overlap with last line segment
          val (iF, jF) = bresenham.next
          if (iF != iN || jF != jN) {
            pixels(iF, jF) += weight
            i0.tail.foreach { i0N =>
              pixels(iF - i0.head + i0N, jF) += weight
            }
          }
          if (!bresenham.hasNext) {
            iN = iF
            jN = jF
          } else {
            @tailrec
            def writeNext(): Unit = {
              val (i, j) = bresenham.next
              pixels(i, j) += weight
              i0.tail.foreach { i0N =>
                pixels(i - i0.head + i0N, j) += weight
              }
              if (bresenham.hasNext) {
                writeNext()
              } else {
                iN = i
                jN = j
              }
            }
            writeNext()
          }
        }

        n += 1
      }
    }
    count += 1
  }

  /**
    * Render a multi-line
    *
    * @param multiLineString geometry
    * @param weight weight
    */
  def render(multiLineString: MultiLineString, weight: Double): Unit = {
    var i = 0
    while (i < multiLineString.getNumGeometries) {
      render(multiLineString.getGeometryN(i).asInstanceOf[LineString], weight)
      i += 1
    }
    count += (1 - i)
  }

  /**
    * Render a polygon
    *
    * @param polygon geometry
    * @param weight weight
    */
  def render(polygon: Polygon, weight: Double): Unit = {
    val envelope = polygon.getEnvelopeInternal
    val imins = translate(envelope.getMinX)
    val imaxes = translate(envelope.getMaxX)
    val jmin = grid.j(envelope.getMinY)
    val jmax = grid.j(envelope.getMaxY)

    if (imins.isEmpty || imaxes.isEmpty || jmin == -1 || jmax == -1) {
      // polygon is not entirely contained in the grid region
      // find the intersection of the polygon with the grid region
      try {
        val intersection = polygon.intersection(grid.envelope)
        if (!intersection.isEmpty) {
          render(intersection, weight)
          count -= 1 // don't double count
        }
      } catch {
        case NonFatal(e) => logger.error(s"Error intersecting polygon [$polygon] with ${grid.envelope}", e)
      }
    } else {
      val imin = imins.head
      val iLength = imaxes.head - imin + 1
      val jLength = jmax - jmin + 1

      val raster = {
        // use java awt graphics to draw our polygon on the grid
        val image = new BufferedImage(iLength, jLength, BufferedImage.TYPE_BYTE_BINARY)
        val graphics = image.createGraphics()

        val border = polygon.getExteriorRing
        val xPoints = Array.ofDim[Int](border.getNumPoints)
        val yPoints = Array.ofDim[Int](border.getNumPoints)
        var i = 0
        while (i < xPoints.length) {
          val coord = border.getCoordinateN(i)
          xPoints(i) = translate(coord.x).head - imin
          yPoints(i) = grid.j(coord.y) - jmin
          i += 1
        }
        graphics.fillPolygon(xPoints, yPoints, xPoints.length)
        image.getRaster
      }

      var i, j = 0
      while (i < iLength) {
        while (j < jLength) {
          if (raster.getSample(i, j, 0) != 0) {
            imins.foreach(im => pixels(i + im, j + jmin) += weight)
          }
          j += 1
        }
        j = 0
        i += 1
      }
    }
    count += 1
  }

  /**
    * Render a multi-polygon
    *
    * @param multiPolygon geometry
    * @param weight weight
    */
  def render(multiPolygon: MultiPolygon, weight: Double): Unit = {
    var i = 0
    while (i < multiPolygon.getNumGeometries) {
      render(multiPolygon.getGeometryN(i).asInstanceOf[Polygon], weight)
      i += 1
    }
    count += (1 - i)
  }

  /**
    * Render an arbitrary geometry
    *
    * @param geometry geometry
    * @param weight weight
    */
  def render(geometry: Geometry, weight: Double): Unit = {
    geometry match {
      case g: Point              => render(g, weight)
      case g: LineString         => render(g, weight)
      case g: Polygon            => render(g, weight)
      case g: MultiPoint         => render(g, weight)
      case g: MultiLineString    => render(g, weight)
      case g: MultiPolygon       => render(g, weight)
      case g: GeometryCollection =>
        var i = 0
        while (i < g.getNumGeometries) {
          render(g.getGeometryN(i), weight)
          i += 1
        }
        count += (1 - i)

      case _ => throw new NotImplementedError(s"Unexpected geometry type: $geometry")
    }
  }

  /**
    * Have any pixels been rendered?
    *
    * @return
    */
  def isEmpty: Boolean = pixels.isEmpty

  /**
    * Number of features rendered in this grid (not accounting for weights).
    *
    * May not be exact - features that are outside the grid envelope will still be counted.
    *
    * @return
    */
  def size: Long = count

  /**
    * Pixel weights
    *
    * @return
    */
  def iterator: Iterator[((Int, Int), Double)] = pixels.iterator

  /**
    * Clear any rendered pixels
    */
  def clear(): Unit = pixels.clear()

  /**
    * Translate a point into the output envelope. If the envelope is larger than 360 degrees, points may
    * be rendered more than once
    *
    * @param x longitude
    * @return
    */
  private def translate(x: Double): Seq[Int] = {
    if (x < xMin) {
      val xt = x + (360d * math.ceil((xMin - x) / 360))
      if (xt > xMax) { Seq.empty } else { widen(xt) }
    } else if (x > xMax) {
      val xt = x - (360d * math.ceil((x - xMax) / 360))
      if (xt < xMin) { Seq.empty } else { widen(xt) }
    } else {
      widen(x)
    }
  }

  /**
    * Returns any pixels that should be rendered for the given point
    *
    * @param x longitude
    * @return
    */
  private def widen(x: Double): Seq[Int] = {
    if (wide) {
      val seq = Seq.newBuilder[Int]
      // start with the smallest x value greater than xMin
      var xup = x - 360d * math.floor((x - xMin) / 360)
      while (xup <= xMax) {
        seq += grid.i(xup)
        xup += 360d
      }
      seq.result
    } else {
      Seq(grid.i(x))
    }
  }
}
