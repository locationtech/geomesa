/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.iterators
import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, Point}
import org.geotools.factory.Hints
import org.geotools.factory.Hints.ClassKey
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.Converters
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.iterators.DensityScan.DensityResult
import org.locationtech.geomesa.utils.geotools.GridSnap
import org.locationtech.geomesa.utils.interop.SimpleFeatureTypes
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.Expression

trait DensityScan extends AggregatingScan[DensityResult] {

  // we snap each point into a pixel and aggregate based on that
  protected var gridSnap: GridSnap = _
  protected var getWeight: (SimpleFeature) => Double = _
  protected var writeGeom: (SimpleFeature, Double, DensityResult) => Unit = _

  override protected def initResult(sft: SimpleFeatureType,
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

    scala.collection.mutable.Map.empty[(Int, Int), Double]
  }

  override protected def aggregateResult(sf: SimpleFeature, result: DensityResult): Unit =
    writeGeom(sf, getWeight(sf), result)

  override protected def encodeResult(result: DensityResult): Array[Byte] = DensityScan.encodeResult(result)
}

object DensityScan {

  type DensityResult = scala.collection.mutable.Map[(Int, Int), Double]
  type GridIterator  = (SimpleFeature) => Iterator[(Double, Double, Double)]

  val DensitySft = SimpleFeatureTypes.createType("density", "*geom:Point:srid=4326")
  val DensityValueKey = new ClassKey(classOf[Array[Byte]])

  // configuration keys
  object Configuration {
    val EnvelopeOpt = "envelope"
    val GridOpt     = "grid"
    val WeightOpt   = "weight"
  }

  def configure(sft: SimpleFeatureType,
                index: GeoMesaFeatureIndex[_, _, _],
                filter: Option[Filter],
                hints: Hints): Map[String, String] = {
    import AggregatingScan.{OptionToConfig, StringToConfig}
    import Configuration.{EnvelopeOpt, GridOpt, WeightOpt}
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints

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
    val output = KryoFeatureSerializer.getOutput(null)
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
    val input = KryoFeatureSerializer.getInput(result, 0, result.length)
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

  def writeGeometry(sft: SimpleFeatureType, grid: GridSnap): (SimpleFeature, Double, DensityResult) => Unit = {
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
    val geomIndex = sft.getGeomIndex
    if (sft.isPoints) {
      (sf, weight, result) => writePoint(sf, geomIndex, weight, grid, result)
    } else {
      (sf, weight, result) => writeNonPoint(sf.getAttribute(geomIndex).asInstanceOf[Geometry], weight, grid, result)
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
      val converted = Converters.convert(d, classOf[java.lang.Double])
      if (converted == null) 1.0 else converted
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
  private def writePoint(sf: SimpleFeature, geomIndex: Int, weight: Double, grid: GridSnap, result: DensityResult): Unit =
    writePointToResult(sf.getAttribute(geomIndex).asInstanceOf[Point], weight, grid, result)

  /**
    * Writes a density record from a feature that has an arbitrary geometry
    */
  private def writeNonPoint(geom: Geometry, weight: Double, grid: GridSnap, result: DensityResult): Unit = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    writePointToResult(geom.safeCentroid(), weight, grid, result)
  }

  private def writePointToResult(pt: Point, weight: Double, grid: GridSnap, result: DensityResult): Unit =
    writeSnappedPoint((grid.i(pt.getX), grid.j(pt.getY)), weight, result)

  private def writePointToResult(pt: Coordinate, weight: Double, grid: GridSnap, result: DensityResult): Unit =
    writeSnappedPoint((grid.i(pt.x), grid.j(pt.y)), weight, result)

  def writePointToResult(x: Double, y: Double, weight: Double, grid: GridSnap, result: DensityResult): Unit =
    writeSnappedPoint((grid.i(x), grid.j(y)), weight, result)

  private def writeSnappedPoint(xy: (Int, Int), weight: Double, result: DensityResult): Unit =
    result.update(xy, result.getOrElse(xy, 0.0) + weight)
}