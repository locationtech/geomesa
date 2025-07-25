/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.parquet

import org.apache.avro.generic.GenericRecord
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.convert.avro.AvroPath
import org.locationtech.geomesa.convert2.transforms.Expression.LiteralString
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{Expression, TransformerFunction, TransformerFunctionFactory}
import org.locationtech.geomesa.utils.text.WKBUtils
import org.locationtech.jts.geom._

class ParquetFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = geometries

  private val geometries = Seq(
    new ParquetPointFn(),
    new ParquetMultiPointFn(),
    new ParquetLineStringFn(),
    new ParquetMultiLineStringFn(),
    new ParquetPolygonFn(),
    new ParquetMultiPolygonFn(),
  )

  private val gf = JTSFactoryFinder.getGeometryFactory

  /**
   * Zip x and y values into coordinates
   *
   * @param x x values
   * @param y corresponding y values
   * @return
   */
  private def zip(x: java.util.List[Double], y: java.util.List[Double]): Array[Coordinate] = {
    val result = Array.ofDim[Coordinate](x.size)
    var i = 0
    while (i < result.length) {
      result(i) = new Coordinate(x.get(i), y.get(i))
      i += 1
    }
    result
  }

  /**
   * Base class for handling parquet-encoded geometries
   *
   * @param name function name
   * @param path optional avro path
   * @tparam T geometry type
   * @tparam U column type
   */
  abstract class ParquetGeometryFn[T <: Geometry, U](name: String, path: Option[AvroPath])
      extends NamedTransformerFunction(Seq(name), pure = true) {

    import org.locationtech.geomesa.fs.storage.parquet.io.GeometrySchema.{GeometryColumnX, GeometryColumnY}

    override def apply(args: Array[AnyRef]): AnyRef = {
      val attribute = path match {
        case None => args.headOption
        case Some(p) => p.eval(args(0).asInstanceOf[GenericRecord])
      }
      attribute match {
        case Some(b: Array[Byte]) => WKBUtils.read(b).asInstanceOf[T]
        case Some(r: GenericRecord) => eval(r.get(GeometryColumnX).asInstanceOf[U], r.get(GeometryColumnY).asInstanceOf[U])
        case Some(g: Geometry) => g
        case _ => null
      }
    }

    protected def eval(x: U, y: U): T
  }

  class ParquetPointFn(path: Option[AvroPath] = None) extends ParquetGeometryFn[Point, Double]("parquetPoint", path) {
    override def getInstance(args: List[Expression]): ParquetPointFn = new ParquetPointFn(getPath(args))
    override protected def eval(x: Double, y: Double): Point = gf.createPoint(new Coordinate(x, y))
  }

  class ParquetMultiPointFn(path: Option[AvroPath] = None)
      extends ParquetGeometryFn[MultiPoint, java.util.List[Double]]("parquetMultiPoint", path) {
    override def getInstance(args: List[Expression]): ParquetMultiPointFn = new ParquetMultiPointFn(getPath(args))
    override protected def eval(x: java.util.List[Double], y: java.util.List[Double]): MultiPoint =
      gf.createMultiPointFromCoords(zip(x, y))
  }

  class ParquetLineStringFn(path: Option[AvroPath] = None)
      extends ParquetGeometryFn[LineString, java.util.List[Double]]("parquetLineString", path) {
    override def getInstance(args: List[Expression]): ParquetLineStringFn = new ParquetLineStringFn(getPath(args))
    override protected def eval(x: java.util.List[Double], y: java.util.List[Double]): LineString =
      gf.createLineString(zip(x, y))
  }

  type ParquetMultiLineString = ParquetGeometryFn[MultiLineString, java.util.List[java.util.List[Double]]]

  class ParquetMultiLineStringFn(path: Option[AvroPath] = None) extends ParquetMultiLineString("parquetMultiLineString", path) {
    override def getInstance(args: List[Expression]): ParquetMultiLineStringFn =
      new ParquetMultiLineStringFn(getPath(args))
    override protected def eval(
        x: java.util.List[java.util.List[Double]],
        y: java.util.List[java.util.List[Double]]): MultiLineString = {
      val lines = Array.tabulate(x.size()) { i =>
        gf.createLineString(zip(x.get(i), y.get(i)))
      }
      gf.createMultiLineString(lines)
    }
  }

  type ParquetPolygon = ParquetGeometryFn[Polygon, java.util.List[java.util.List[Double]]]

  class ParquetPolygonFn(path: Option[AvroPath] = None) extends ParquetPolygon("parquetPolygon", path) {
    override def getInstance(args: List[Expression]): ParquetPolygonFn = new ParquetPolygonFn(getPath(args))
    override protected def eval(
        x: java.util.List[java.util.List[Double]],
        y: java.util.List[java.util.List[Double]]): Polygon = {
      val shell = gf.createLinearRing(zip(x.get(0), y.get(0)))
      val holes = if (x.size < 2) { null } else {
        Array.tabulate(x.size() - 1) { i =>
          gf.createLinearRing(zip(x.get(i + 1), y.get(i + 1)))
        }
      }
      gf.createPolygon(shell, holes)
    }
  }

  type ParquetMultiPolygon = ParquetGeometryFn[MultiPolygon, java.util.List[java.util.List[java.util.List[Double]]]]

  class ParquetMultiPolygonFn(path: Option[AvroPath] = None) extends ParquetMultiPolygon("parquetMultiPolygon", path) {
    override def getInstance(args: List[Expression]): ParquetMultiPolygonFn = new ParquetMultiPolygonFn(getPath(args))
    override protected def eval(
        x: java.util.List[java.util.List[java.util.List[Double]]],
        y: java.util.List[java.util.List[java.util.List[Double]]]): MultiPolygon = {
      val polys = Array.tabulate(x.size) { j =>
        val shell = gf.createLinearRing(zip(x.get(j).get(0), y.get(j).get(0)))
        val holes = if (x.get(j).size < 2) { null } else {
          Array.tabulate(x.get(j).size() - 1) { i =>
            gf.createLinearRing(zip(x.get(j).get(i + 1), y.get(j).get(i + 1)))
          }
        }
        gf.createPolygon(shell, holes)
      }
      gf.createMultiPolygon(polys)
    }
  }

  private def getPath(args: List[Expression]): Option[AvroPath] = {
    args match {
      case _ :: LiteralString(s) :: _ => Some(AvroPath(s))
      case _ => None
    }
  }
}
