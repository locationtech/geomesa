/** *********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
* ************************************************************************/

package org.locationtech.geomesa.hbase.coprocessor

import com.google.common.base.Throwables
import com.google.protobuf.ByteString
import com.google.protobuf.RpcCallback
import com.google.protobuf.RpcController
import com.google.protobuf.Service
import org.apache.commons.io.IOUtils
import org.apache.hadoop.hbase.{Cell, Coprocessor, CoprocessorEnvironment}
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.coprocessor.CoprocessorException
import org.apache.hadoop.hbase.coprocessor.CoprocessorService
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment
import org.apache.hadoop.hbase.exceptions.DeserializationException
import org.apache.hadoop.hbase.protobuf.ResponseConverter
import org.apache.hadoop.hbase.regionserver.InternalScanner
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer
import org.locationtech.geomesa.features.SerializationOption.SerializationOptions
import org.locationtech.geomesa.hbase.proto.KryoLazyDensityProto
import org.locationtech.geomesa.hbase.proto.KryoLazyDensityProto._
import org.locationtech.geomesa.utils.geotools.{GridSnap, SimpleFeatureTypes}
import org.slf4j.LoggerFactory
import java.io._

import com.vividsolutions.jts.geom.{Coordinate, Envelope, Geometry, Point}
import org.geotools.factory.Hints
import org.geotools.filter.text.ecql.ECQL
import org.geotools.util.Converters
import org.locationtech.geomesa.process.utils.KryoLazyDensityUtils
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.filter.Filter
import org.opengis.filter.expression.Expression

import scala.collection.JavaConversions._
import scala.collection.mutable

class KryoLazyDensityCoprocessor extends KryoLazyDensityService with Coprocessor with CoprocessorService {

  import KryoLazyDensityCoprocessor._

  var geomIndex: Int = -1
  // we snap each point into a pixel and aggregate based on that
  var gridSnap: GridSnap = null
  var writeGeom: (SimpleFeature, DensityResult) => Unit = null
  var sft: SimpleFeatureType = null

  def aggregateResult(sf: SimpleFeature, result: DensityResult): Unit = writeGeom(sf, result)

  def init(options: mutable.Map[String, String], sft: SimpleFeatureType): DensityResult = {
    this.sft = sft
    import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
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

  protected def writePointToResult(pt: Point, weight: Double, result: DensityResult): Unit =
    writeSnappedPoint((gridSnap.i(pt.getX), gridSnap.j(pt.getY)), weight, result)

  protected def writePointToResult(pt: Coordinate, weight: Double, result: DensityResult): Unit =
    writeSnappedPoint((gridSnap.i(pt.x), gridSnap.j(pt.y)), weight, result)

  protected def writePointToResult(x: Double, y: Double, weight: Double, result: DensityResult): Unit =
    writeSnappedPoint((gridSnap.i(x), gridSnap.j(y)), weight, result)

  private def writeSnappedPoint(xy: (Int, Int), weight: Double, result: DensityResult): Unit =
    result.update(xy, result.getOrElse(xy, 0.0) + weight)

  private var env : RegionCoprocessorEnvironment = null
  final private val logger = LoggerFactory.getLogger(classOf[KryoLazyDensityCoprocessor])

  @throws[IOException]
  def start(env: CoprocessorEnvironment) {
    if (env.isInstanceOf[RegionCoprocessorEnvironment])
      this.env = env.asInstanceOf[RegionCoprocessorEnvironment]
    else
      throw new CoprocessorException("Must be loaded on a table region!")
  }

  @throws[IOException]
  def stop(coprocessorEnvironment: CoprocessorEnvironment) {
  }

  def getService: Service = this

  def getDensity(controller: RpcController, request: KryoLazyDensityProto.DensityRequest, done: RpcCallback[KryoLazyDensityProto.DensityResponse]) {
    val byteFilterArray = request.getByteFilter.toByteArray
    var response : DensityResponse = null
    var scanner : InternalScanner = null
    try {
      val scan = new Scan
      val options: mutable.Map[String, String] = deserializeOptions(byteFilterArray)
      val sft = SimpleFeatureTypes.createType("input", options(SFT_OPT))
      val serializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId)
      val densityResult: DensityResult = this.init(options, sft)
      scanner = env.getRegion.getScanner(scan)
      val results = new java.util.ArrayList[Cell]
      var hasMore = false
      do {
        hasMore = scanner.next(results)
        for (cell <- results) {
          val sf = serializer.deserialize(cell.getValueArray, cell.getValueOffset, cell.getValueLength)
          aggregateResult(sf, densityResult)
        }
        results.clear()
      } while (hasMore)

      val result: Array[Byte] = KryoLazyDensityUtils.encodeResult(densityResult)
      response = DensityResponse.newBuilder.setSf(ByteString.copyFrom(result)).build
    } catch {
      case ioe: IOException => {
        ResponseConverter.setControllerException(controller, ioe)
      }
      case cnfe: ClassNotFoundException => {
        logger.error(Throwables.getStackTraceAsString(cnfe))
      }
      case dse: DeserializationException => {
        logger.error(Throwables.getStackTraceAsString(dse))
      }
    } finally IOUtils.closeQuietly(scanner)
    done.run(response)
  }
}

object KryoLazyDensityCoprocessor{
  private val logger = LoggerFactory.getLogger(classOf[KryoLazyDensityCoprocessor])
  type DensityResult = mutable.Map[(Int, Int), Double]
  type GridIterator = (SimpleFeature) => Iterator[(Double, Double, Double)]

  // needs to be lazy to avoid class loading issues before init is called
  lazy val DENSITY_SFT = SimpleFeatureTypes.createType("density", "result:String,*geom:Point:srid=4326")

  val DEFAULT_PRIORITY = 25

  // configuration keys
  private val ENVELOPE_OPT = "envelope"
  private val GRID_OPT     = "grid"
  private val WEIGHT_OPT   = "weight"
  private val SFT_OPT = "sft"

  /**
    * Creates an iterator config for the kryo density iterator
    */
  def configure(sft: SimpleFeatureType,
                filter: Option[Filter],
                hints: Hints): mutable.Map[String, String] = {
    import org.locationtech.geomesa.index.conf.QueryHints.RichHints
    val envelope = hints.getDensityEnvelope.get
    val (width, height) = hints.getDensityBounds.get
    val weight = hints.getDensityWeight
    configure(sft, filter, envelope, width, height, weight)
  }

  protected def configure(sft: SimpleFeatureType,
                                     filter: Option[Filter],
                                     envelope: Envelope,
                                     gridWidth: Int,
                                     gridHeight: Int,
                                     weightAttribute: Option[String]): mutable.Map[String, String] = {
    val is = mutable.Map.empty[String, String]
    is.put(ENVELOPE_OPT, s"${envelope.getMinX},${envelope.getMaxX},${envelope.getMinY},${envelope.getMaxY}")
    is.put(GRID_OPT, s"$gridWidth,$gridHeight")
    weightAttribute.foreach(is.put(WEIGHT_OPT, _))
    is.put(SFT_OPT, SimpleFeatureTypes.encodeType(sft, false))
    is
  }

  @throws[IOException]
  def serializeOptions(map: mutable.Map[String, String]): Array[Byte] = {
    val fis = new ByteArrayOutputStream
    val ois = new ObjectOutputStream(fis)
    ois.writeObject(map)
    val output = fis.toByteArray
    ois.close()
    fis.close()
    output
  }

  def deserializeOptions(bytes: Array[Byte]) = {
    val byteIn = new ByteArrayInputStream(bytes)
    val in = new ObjectInputStream(byteIn)
    val map = in.readObject.asInstanceOf[mutable.Map[String, String]]
    map
  }

}