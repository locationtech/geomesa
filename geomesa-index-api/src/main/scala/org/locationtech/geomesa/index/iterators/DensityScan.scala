/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators
import java.awt.image.BufferedImage

import com.typesafe.scalalogging.LazyLogging
import org.geotools.factory.Hints
import org.geotools.factory.Hints.ClassKey
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.features.kryo.impl.{KryoFeatureDeserialization, KryoFeatureSerialization}
import org.locationtech.geomesa.filter.FilterHelper
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.DensityScan.DensityResult
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.converters.FastConverter
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, GridSnap}
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes
import org.locationtech.jts.geom._
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.Expression

import scala.annotation.tailrec
import scala.util.control.NonFatal

trait DensityScan extends AggregatingScan[DensityResult] {

  // we snap each point into a pixel and aggregate based on that
  protected var gridSnap: GridSnap = _
  protected var getWeight: SimpleFeature => Double = _
  protected var writeGeom: (SimpleFeature, Double, DensityResult) => Unit = _

  private var batchSize: Int = -1
  private var count: Int = -1

  override protected def initResult(
      sft: SimpleFeatureType,
      transform: Option[SimpleFeatureType],
      options: Map[String, String]): DensityResult = {
    import DensityScan.Configuration._

    gridSnap = {
      val bounds = options(EnvelopeOpt).split(",").map(_.toDouble)
      val envelope = new Envelope(bounds(0), bounds(1), bounds(2), bounds(3))
      val Array(width, height) = options(GridOpt).split(",").map(_.toInt)
      new GridSnap(envelope, width, height)
    }

    getWeight = DensityScan.getWeight(sft, options.get(WeightOpt))
    writeGeom = DensityScan.writeGeometry(sft, gridSnap)
    count = 0
    batchSize = DensityScan.BatchSize.toInt.get // has a valid default so should be safe to .get

    scala.collection.mutable.Map.empty[(Int, Int), Double].withDefaultValue(0d)
  }

  override protected def aggregateResult(sf: SimpleFeature, result: DensityResult): Unit = {
    writeGeom(sf, getWeight(sf), result)
    count += 1
  }

  override protected def notFull(result: DensityResult): Boolean =
    if (count < batchSize) { true } else { count = 0; false }

  override protected def encodeResult(result: DensityResult): Array[Byte] = DensityScan.encodeResult(result)
}

object DensityScan extends LazyLogging {

  import org.locationtech.geomesa.index.conf.QueryHints.RichHints
  import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

  type DensityResult = scala.collection.mutable.Map[(Int, Int), Double]
  type GridIterator  = SimpleFeature => Iterator[(Double, Double, Double)]

  val BatchSize = SystemProperty("geomesa.density.batch.size", "100000")

  val DensitySft: SimpleFeatureType = SimpleFeatureTypes.createType("density", "*geom:Point:srid=4326")
  val DensityValueKey = new ClassKey(classOf[Array[Byte]])

  // configuration keys
  object Configuration {
    val EnvelopeOpt = "envelope"
    val GridOpt     = "grid"
    val WeightOpt   = "weight"
  }

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _],
                filter: Option[Filter],
                hints: Hints): Map[String, String] = {
    import AggregatingScan.{OptionToConfig, StringToConfig}
    import Configuration.{EnvelopeOpt, GridOpt, WeightOpt}

    val envelope = hints.getDensityEnvelope.get
    val (width, height) = hints.getDensityBounds.get
    val base = AggregatingScan.configure(sft, index, filter, None, hints.getSampling) // note: don't pass transforms
    base ++ AggregatingScan.optionalMap(
      EnvelopeOpt -> s"${envelope.getMinX},${envelope.getMaxX},${envelope.getMinY},${envelope.getMaxY}",
      GridOpt     -> s"$width,$height",
      WeightOpt   -> hints.getDensityWeight
    )
  }
  /**
    * Encodes a sparse matrix into a byte array
    */
  def encodeResult(result: DensityResult): Array[Byte] = {
    val output = KryoFeatureSerialization.getOutput(null)
    result.toList.groupBy(_._1._1).foreach { case (row, cols) =>
      output.writeInt(row, true)
      output.writeInt(cols.size, true)
      cols.foreach { case (xy, weight) =>
        output.writeInt(xy._2, true)
        output.writeDouble(weight)
      }
    }
    output.toBytes
  }

  /**
    * Returns a mapping of simple features (returned from a density query) to weighted points in the
    * form of (x, y, weight)
    */
  def decodeResult(envelope: Envelope, gridWidth: Int, gridHeight: Int): GridIterator =
    decodeResult(new GridSnap(envelope, gridWidth, gridHeight))

  /**
    * Decodes a result feature into an iterator of (x, y, weight)
    */
  def decodeResult(gridSnap: GridSnap)(sf: SimpleFeature): Iterator[(Double, Double, Double)] = {
    val result = sf.getUserData.get(DensityValueKey).asInstanceOf[Array[Byte]]
    val input = KryoFeatureDeserialization.getInput(result, 0, result.length)
    new Iterator[(Double, Double, Double)]() {
      private var x = 0.0
      private var colCount = 0
      override def hasNext: Boolean = input.position < input.limit
      override def next(): (Double, Double, Double) = {
        if (colCount == 0) {
          x = gridSnap.x(input.readInt(true))
          colCount = input.readInt(true)
        }
        val y = gridSnap.y(input.readInt(true))
        val weight = input.readDouble()
        colCount -= 1
        (x, y, weight)
      }
    }
  }

  def getWeight(sft: SimpleFeatureType, weight: Option[String]): (SimpleFeature) => Double = {
    // function to get the weight from the feature - defaults to 1.0 unless an attribute/exp is specified
    val weightIndex = weight.map(sft.indexOf).getOrElse(-2)
    if (weightIndex == -2) {
      (_) => 1.0
    } else if (weightIndex == -1) {
      getWeightFromExpression(ECQL.toExpression(weight.get))
    } else if (classOf[Number].isAssignableFrom(sft.getDescriptor(weightIndex).getType.getBinding)) {
      getWeightFromNumber(weightIndex)
    } else {
      getWeightFromNonNumber(weightIndex)
    }
  }

  /**
    * Get the attributes used by a density query
    *
    * @param hints query hints
    * @param sft simple feature type
    * @return
    */
  def propertyNames(hints: Hints, sft: SimpleFeatureType): Seq[String] = {
    val weight = hints.getDensityWeight.map(ECQL.toExpression)
    (Seq(sft.getGeomField) ++ weight.toSeq.flatMap(FilterHelper.propertyNames(_, sft))).distinct
  }

  def writeGeometry(sft: SimpleFeatureType, grid: GridSnap): (SimpleFeature, Double, DensityResult) => Unit = {
    val geomIndex = sft.getGeomIndex
    sft.getGeometryDescriptor.getType.getBinding match {
      case b if b == classOf[Point]           => writePoint(geomIndex, grid)
      case b if b == classOf[MultiPoint]      => writeMultiPoint(geomIndex, grid)
      case b if b == classOf[LineString]      => writeLineString(geomIndex, grid)
      case b if b == classOf[MultiLineString] => writeMultiLineString(geomIndex, grid)
      case _                                  => writeGeometry(geomIndex, grid)
    }
  }

  /**
    * Gets the weight for a feature from a double attribute
    */
  private def getWeightFromNumber(i: Int)(sf: SimpleFeature): Double = {
    val d = sf.getAttribute(i).asInstanceOf[Number]
    if (d == null) { 0.0 } else { d.doubleValue }
  }

  /**
    * Tries to convert a non-double attribute into a double
    */
  private def getWeightFromNonNumber(i: Int)(sf: SimpleFeature): Double = {
    val d = sf.getAttribute(i)
    if (d == null) { 0.0 } else {
      val converted = FastConverter.convert(d, classOf[java.lang.Double])
      if (converted == null) { 1.0 } else { converted.doubleValue() }
    }
  }

  /**
    * Evaluates an arbitrary expression against the simple feature to return a weight
    */
  private def getWeightFromExpression(e: Expression)(sf: SimpleFeature): Double = {
    val d = e.evaluate(sf, classOf[java.lang.Double])
    if (d == null) 0.0 else d
  }

  /**
    * Writes a density record from a feature that has a point geometry
    */
  private def writePoint(geomIndex: Int, grid: GridSnap)
                        (sf: SimpleFeature, weight: Double, result: DensityResult): Unit =
    writePointToResult(sf.getAttribute(geomIndex).asInstanceOf[Point], weight, grid, result)

  /**
    * Writes a density record from a feature that has a multi-point geometry
    */
  private def writeMultiPoint(geomIndex: Int, grid: GridSnap)
                             (sf: SimpleFeature, weight: Double, result: DensityResult): Unit =
    writeMultiPointToResult(sf.getAttribute(geomIndex).asInstanceOf[MultiPoint], weight, grid, result)

  /**
    * Writes a density record from a feature that has a line geometry
    */
  private def writeLineString(geomIndex: Int, grid: GridSnap)
                             (sf: SimpleFeature, weight: Double, result: DensityResult): Unit =
    writeLineToResult(sf.getAttribute(geomIndex).asInstanceOf[LineString], weight, grid, result)

  /**
    * Writes a density record from a feature that has a multi-line geometry
    */
  private def writeMultiLineString(geomIndex: Int, grid: GridSnap)
                                  (sf: SimpleFeature, weight: Double, result: DensityResult): Unit =
    writeMultiLineToResult(sf.getAttribute(geomIndex).asInstanceOf[MultiLineString], weight, grid, result)

  /**
    * Writes a density record from a feature that has a polygon geometry
    */
  private def writePolygon(geomIndex: Int, grid: GridSnap)
                          (sf: SimpleFeature, weight: Double, result: DensityResult): Unit =
    writePolygonToResult(sf.getAttribute(geomIndex).asInstanceOf[Polygon], weight, grid, result)

  /**
    * Writes a density record from a feature that has a polygon geometry
    */
  private def writeMultiPolygon(geomIndex: Int, grid: GridSnap)
                               (sf: SimpleFeature, weight: Double, result: DensityResult): Unit =
    writeMultiPolygonToResult(sf.getAttribute(geomIndex).asInstanceOf[MultiPolygon], weight, grid, result)

  /**
    * Writes a density record from a feature that has an arbitrary geometry
    */
  private def writeGeometry(geomIndex: Int, grid: GridSnap)
                           (sf: SimpleFeature, weight: Double, result: DensityResult): Unit =
    writeGeometryToResult(sf.getAttribute(geomIndex).asInstanceOf[Geometry], weight, grid, result)

  private def writePointToResult(pt: Point, weight: Double, grid: GridSnap, result: DensityResult): Unit =
    writeSnappedPoint(grid.i(pt.getX), grid.j(pt.getY), weight, result)

  private def writeMultiPointToResult(pts: MultiPoint, weight: Double, grid: GridSnap, result: DensityResult): Unit = {
    var i = 0
    while (i < pts.getNumGeometries) {
      writePointToResult(pts.getGeometryN(i).asInstanceOf[Point], weight, grid, result)
      i += 1
    }
  }

  private def writeLineToResult(ls: LineString, weight: Double, grid: GridSnap, result: DensityResult): Unit = {
    if (ls.getNumPoints < 1) {
      logger.warn(s"Encountered line string with fewer than two points: $ls")
    } else {
      var iN, jN = -1 // track the last pixel we've written to avoid double-counting
      val points = Seq.tabulate(ls.getNumPoints) { i => val p = ls.getCoordinateN(i); (p, grid.i(p.x), grid.j(p.y)) }

      points.sliding(2).foreach { case Seq((p0, i0, j0), (p1, i1, j1)) =>
        if (i0 == -1 || j0 == -1 || i1 == -1 || j1 == -1) {
          // line is not entirely contained in the grid region
          // find the intersection of the line segment with the grid region
          try {
            val intersection = GeometryUtils.geoFactory.createLineString(Array(p0, p1)).intersection(grid.envelope)
            if (!intersection.isEmpty) {
              writeGeometryToResult(intersection, weight, grid, result)
            }
          } catch {
            case NonFatal(e) => logger.error(s"Error intersecting line string [$p0 $p1] with ${grid.envelope}", e)
          }
        } else {
          val bresenham = grid.bresenhamLine(i0, j0, i1, j1)
          // check the first point for overlap with last line segment
          val (iF, jF) = bresenham.next
          if (iF != iN || jF != jN) {
            writeSnappedPoint(iF, jF, weight, result)
          }
          var next = bresenham.hasNext
          if (!next) {
            iN = iF
            jN = jF
          } else {
            @tailrec
            def writeNext(): Unit = {
              val (i, j) = bresenham.next
              writeSnappedPoint(i, j, weight, result)
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
      }
    }
  }

  private def writeMultiLineToResult(lines: MultiLineString, weight: Double, grid: GridSnap, result: DensityResult): Unit = {
    var i = 0
    while (i < lines.getNumGeometries) {
      writeLineToResult(lines.getGeometryN(i).asInstanceOf[LineString], weight, grid, result)
      i += 1
    }
  }

  private def writePolygonToResult(poly: Polygon, weight: Double, grid: GridSnap, result: DensityResult): Unit = {
    val envelope = poly.getEnvelopeInternal
    val imin = grid.i(envelope.getMinX)
    val imax = grid.i(envelope.getMaxX)
    val jmin = grid.j(envelope.getMinY)
    val jmax = grid.j(envelope.getMaxY)

    if (imin == -1 || imax == -1 || jmin == -1 || jmax == -1) {
      // polygon is not entirely contained in the grid region
      // find the intersection of the polygon with the grid region
      try {
        val intersection = poly.intersection(grid.envelope)
        if (!intersection.isEmpty) {
          writeGeometryToResult(intersection, weight, grid, result)
        }
      } catch {
        case NonFatal(e) => logger.error(s"Error intersecting polygon [$poly] with ${grid.envelope}", e)
      }
    } else {
      val iLength = imax - imin + 1
      val jLength = jmax - jmin + 1

      val raster = {
        // use java awt graphics to draw our polygon on the grid
        val image = new BufferedImage(iLength, jLength, BufferedImage.TYPE_BYTE_BINARY)
        val graphics = image.createGraphics()

        val border = poly.getExteriorRing
        val xPoints = Array.ofDim[Int](border.getNumPoints)
        val yPoints = Array.ofDim[Int](border.getNumPoints)
        var i = 0
        while (i < xPoints.length) {
          val coord = border.getCoordinateN(i)
          xPoints(i) = grid.i(coord.x) - imin
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
            writeSnappedPoint(i + imin, j + jmin, weight, result)
          }
          j += 1
        }
        j = 0
        i += 1
      }
    }
  }

  private def writeMultiPolygonToResult(polys: MultiPolygon, weight: Double, grid: GridSnap, result: DensityResult): Unit = {
    var i = 0
    while (i < polys.getNumGeometries) {
      writePolygonToResult(polys.getGeometryN(i).asInstanceOf[Polygon], weight, grid, result)
      i += 1
    }
  }

  private def writeGeometryToResult(geometry: Geometry, weight: Double, grid: GridSnap, result: DensityResult): Unit = {
    geometry match {
      case g: Point              => writePointToResult(g, weight, grid, result)
      case g: LineString         => writeLineToResult(g, weight, grid, result)
      case g: Polygon            => writePolygonToResult(g, weight, grid, result)
      case g: MultiPoint         => writeMultiPointToResult(g, weight, grid, result)
      case g: MultiLineString    => writeMultiLineToResult(g, weight, grid, result)
      case g: MultiPolygon       => writeMultiPolygonToResult(g, weight, grid, result)

      case g: GeometryCollection =>
        var i = 0
        while (i < g.getNumGeometries) {
          writeGeometryToResult(g.getGeometryN(i), weight, grid, result)
          i += 1
        }
    }
  }

  private def writeSnappedPoint(i: Int, j: Int, weight: Double, result: DensityResult): Unit = {
    if (i != -1 && j != -1) {
      result(i, j) += weight
    }
  }
}