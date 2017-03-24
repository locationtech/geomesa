/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.iterators

import java.util.Map.Entry

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom._
import org.apache.accumulo.core.client.IteratorSetting
import org.apache.accumulo.core.data._
import org.apache.accumulo.core.iterators.{IteratorEnvironment, SortedKeyValueIterator}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.Converters
import org.locationtech.geomesa.accumulo.AccumuloFeatureIndexType
import org.locationtech.geomesa.accumulo.iterators.KryoLazyDensityIterator.DensityResult
import org.locationtech.geomesa.features.ScalaSimpleFeature
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.{GeometryUtils, GridSnap, SimpleFeatureTypes}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.Expression

import scala.collection.mutable

/**
 * Density iterator - only works on kryo-encoded features
 */
class KryoLazyDensityIterator extends KryoLazyAggregatingIterator[DensityResult] with LazyLogging {

  import KryoLazyDensityIterator._

  var geomIndex: Int = -1
  // we snap each point into a pixel and aggregate based on that
  var gridSnap: GridSnap = null
  var writeGeom: (SimpleFeature, DensityResult) => Unit = null

  override def init(options: Map[String, String]): DensityResult = {
    geomIndex = sft.getGeomIndex
    gridSnap = {
      val bounds = options(ENVELOPE_OPT).split(",").map(_.toDouble)
      val envelope = new Envelope(bounds(0), bounds(1), bounds(2), bounds(3))
      val Array(width, height) = options(GRID_OPT).split(",").map(_.toInt)
      new GridSnap(envelope, width, height)
    }

    // function to get the weight from the feature - defaults to 1.0 unless an attribute/exp is specified
    val weightIndex = options.get(WEIGHT_OPT).map(sft.indexOf).getOrElse(-2)
    val weightFn: (SimpleFeature) => Double =
      if (weightIndex == -2) {
        (_) => 1.0
      } else if (weightIndex == -1) {
        val expression = ECQL.toExpression(options(WEIGHT_OPT))
        getWeightFromExpression(expression)
      } else if (sft.getDescriptor(weightIndex).getType.getBinding == classOf[java.lang.Double]) {
        getWeightFromDouble(weightIndex)
      } else {
        getWeightFromNonDouble(weightIndex)
      }

    writeGeom = if (sft.isPoints) {
      (sf, result) => writePoint(sf, weightFn(sf), result)
    } else {
      (sf, result) => writeNonPoint(sf.getDefaultGeometry.asInstanceOf[Geometry], weightFn(sf), result)
    }

    mutable.Map.empty[(Int, Int), Double]
  }

  override def aggregateResult(sf: SimpleFeature, result: DensityResult): Unit = writeGeom(sf, result)

  override def encodeResult(result: DensityResult): Array[Byte] = KryoLazyDensityIterator.encodeResult(result)

  /**
   * Gets the weight for a feature from a double attribute
   */
  private def getWeightFromDouble(i: Int)(sf: SimpleFeature): Double = {
    val d = sf.getAttribute(i).asInstanceOf[java.lang.Double]
    if (d == null) 0.0 else d
  }

  /**
   * Tries to convert a non-double attribute into a double
   */
  private def getWeightFromNonDouble(i: Int)(sf: SimpleFeature): Double = {
    val d = sf.getAttribute(i)
    if (d == null) {
      0.0
    } else {
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
  def writePoint(sf: SimpleFeature, weight: Double, result: DensityResult): Unit =
    writePointToResult(sf.getAttribute(geomIndex).asInstanceOf[Point], weight, result)

  /**
   * Writes a density record from a feature that has an arbitrary geometry
   */
  def writeNonPoint(geom: Geometry, weight: Double, result: DensityResult): Unit = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    writePointToResult(geom.safeCentroid(), weight, result)
  }

  protected[iterators] def writePointToResult(pt: Point, weight: Double, result: DensityResult): Unit =
    writeSnappedPoint((gridSnap.i(pt.getX), gridSnap.j(pt.getY)), weight, result)

  protected[iterators] def writePointToResult(pt: Coordinate, weight: Double, result: DensityResult): Unit =
    writeSnappedPoint((gridSnap.i(pt.x), gridSnap.j(pt.y)), weight, result)

  protected[iterators] def writePointToResult(x: Double, y: Double, weight: Double, result: DensityResult): Unit =
    writeSnappedPoint((gridSnap.i(x), gridSnap.j(y)), weight, result)

  private def writeSnappedPoint(xy: (Int, Int), weight: Double, result: DensityResult): Unit =
    result.update(xy, result.getOrElse(xy, 0.0) + weight)

  override def deepCopy(env: IteratorEnvironment): SortedKeyValueIterator[Key, Value] = ???
}

object KryoLazyDensityIterator extends LazyLogging {

  type DensityResult = mutable.Map[(Int, Int), Double]
  type GridIterator = (SimpleFeature) => Iterator[(Double, Double, Double)]

  // needs to be lazy to avoid class loading issues before init is called
  lazy val DENSITY_SFT = SimpleFeatureTypes.createType("density", "result:String,*geom:Point:srid=4326")

  val DEFAULT_PRIORITY = 25

  // configuration keys
  private val ENVELOPE_OPT = "envelope"
  private val GRID_OPT     = "grid"
  private val WEIGHT_OPT   = "weight"

  /**
   * Creates an iterator config for the kryo density iterator
   */
  def configure(sft: SimpleFeatureType,
                index: AccumuloFeatureIndexType,
                filter: Option[Filter],
                hints: Hints,
                deduplicate: Boolean,
                priority: Int = DEFAULT_PRIORITY): IteratorSetting = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    val envelope = hints.getDensityEnvelope.get
    val (width, height) = hints.getDensityBounds.get
    val weight = hints.getDensityWeight
    val is = new IteratorSetting(priority, "density-iter", classOf[KryoLazyDensityIterator])
    configure(is, sft, index, filter, deduplicate, envelope, width, height, weight)
  }

  protected[iterators] def configure(is: IteratorSetting,
                                     sft: SimpleFeatureType,
                                     index: AccumuloFeatureIndexType,
                                     filter: Option[Filter],
                                     deduplicate: Boolean,
                                     envelope: Envelope,
                                     gridWidth: Int,
                                     gridHeight: Int,
                                     weightAttribute: Option[String]): IteratorSetting = {
    // we never need to dedupe densities - either we don't have dupes or we weight based on the duplicates
    KryoLazyAggregatingIterator.configure(is, sft, index, filter, deduplicate, None)
    is.addOption(ENVELOPE_OPT, s"${envelope.getMinX},${envelope.getMaxX},${envelope.getMinY},${envelope.getMaxY}")
    is.addOption(GRID_OPT, s"$gridWidth,$gridHeight")
    weightAttribute.foreach(is.addOption(WEIGHT_OPT, _))
    is
  }

  /**
   * Adapts the iterator to create simple features.
   * WARNING - the same feature is re-used and mutated - the iterator stream should be operated on serially.
   */
  def kvsToFeatures(): (Entry[Key, Value]) => SimpleFeature = {
    val sf = new ScalaSimpleFeature("", DENSITY_SFT)
    sf.setAttribute(1, GeometryUtils.zeroPoint)
    (e: Entry[Key, Value]) => {
      // set the value directly in the array, as we don't support byte arrays as properties
      // TODO GEOMESA-823 support byte arrays natively
      sf.values(0) = e.getValue.get()
      sf
    }
  }

  /**
   * Encodes a sparse matrix into a byte array
   */
  def encodeResult(result: mutable.Map[(Int, Int), Double]): Array[Byte] = {
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
  private def decodeResult(gridSnap: GridSnap)(sf: SimpleFeature): Iterator[(Double, Double, Double)] = {
    val result = sf.getAttribute(0).asInstanceOf[Array[Byte]]
    val input = KryoFeatureSerializer.getInput(result, 0, result.length)
    new Iterator[(Double, Double, Double)]() {
      private var x = 0.0
      private var colCount = 0
      override def hasNext = input.position < input.limit
      override def next() = {
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
}
