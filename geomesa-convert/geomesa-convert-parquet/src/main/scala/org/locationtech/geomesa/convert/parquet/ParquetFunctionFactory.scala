/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.convert.parquet

import org.apache.avro.generic.GenericRecord
import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.geomesa.convert.EvaluationContext
import org.locationtech.geomesa.convert.avro.AvroPath
import org.locationtech.geomesa.convert2.transforms.TransformerFunction.NamedTransformerFunction
import org.locationtech.geomesa.convert2.transforms.{TransformerFunction, TransformerFunctionFactory}
import org.locationtech.geomesa.parquet.io.{SimpleFeatureParquetSchema, SimpleFeatureReadSupport}
import org.locationtech.jts.geom._

class ParquetFunctionFactory extends TransformerFunctionFactory {

  override def functions: Seq[TransformerFunction] = geometries

  private val geometries = Seq(
    new ParquetPointFn,
    new ParquetMultiPointFn,
    new ParquetLineStringFn,
    new ParquetMultiLineStringFn,
    new ParquetPolygonFn,
    new ParquetMultiPolygonFn
  )

  private val gf = JTSFactoryFinder.getGeometryFactory

  /**
    * Base class for handling parquet-encoded geometries
    *
    * @param name function name
    * @tparam T geometry type
    * @tparam U column type
    */
  abstract class ParquetGeometryFn[T <: Geometry, U](name: String)
      extends NamedTransformerFunction(Seq(name), pure = true) {

    import SimpleFeatureParquetSchema.{GeometryColumnX, GeometryColumnY}
    private var path: AvroPath = _

    override def eval(args: Array[Any])(implicit ctx: EvaluationContext): Any = {
      if (path == null) {
        path = AvroPath(args(1).asInstanceOf[String])
      }
      path.eval(args(0).asInstanceOf[GenericRecord]).collect {
        case r: GenericRecord => eval(r.get(GeometryColumnX).asInstanceOf[U], r.get(GeometryColumnY).asInstanceOf[U])
      }.orNull
    }

    protected def eval(x: U, y: U): T
  }

  class ParquetPointFn extends ParquetGeometryFn[Point, Double]("parquetPoint") {
    override def getInstance: ParquetPointFn = new ParquetPointFn()
    override protected def eval(x: Double, y: Double): Point = gf.createPoint(new Coordinate(x, y))
  }

  class ParquetMultiPointFn extends ParquetGeometryFn[MultiPoint, java.util.List[Double]]("parquetMultiPoint") {
    override def getInstance: ParquetMultiPointFn = new ParquetMultiPointFn()
    override protected def eval(x: java.util.List[Double], y: java.util.List[Double]): MultiPoint =
      gf.createMultiPointFromCoords(SimpleFeatureReadSupport.zip(x, y))
  }

  class ParquetLineStringFn extends ParquetGeometryFn[LineString, java.util.List[Double]]("parquetLineString") {
    override def getInstance: ParquetLineStringFn = new ParquetLineStringFn()
    override protected def eval(x: java.util.List[Double], y: java.util.List[Double]): LineString =
      gf.createLineString(SimpleFeatureReadSupport.zip(x, y))
  }

  type ParquetMultiLineString = ParquetGeometryFn[MultiLineString, java.util.List[java.util.List[Double]]]

  class ParquetMultiLineStringFn extends ParquetMultiLineString("parquetMultiLineString") {
    override def getInstance: ParquetMultiLineStringFn = new ParquetMultiLineStringFn()
    override protected def eval(
        x: java.util.List[java.util.List[Double]],
        y: java.util.List[java.util.List[Double]]): MultiLineString = {
      val lines = Array.tabulate(x.size()) { i =>
        gf.createLineString(SimpleFeatureReadSupport.zip(x.get(i), y.get(i)))
      }
      gf.createMultiLineString(lines)
    }
  }

  type ParquetPolygon = ParquetGeometryFn[Polygon, java.util.List[java.util.List[Double]]]

  class ParquetPolygonFn extends ParquetPolygon("parquetPolygon") {
    override def getInstance: ParquetPolygonFn = new ParquetPolygonFn()
    override protected def eval(
        x: java.util.List[java.util.List[Double]],
        y: java.util.List[java.util.List[Double]]): Polygon = {
      val shell = gf.createLinearRing(SimpleFeatureReadSupport.zip(x.get(0), y.get(0)))
      val holes = if (x.size < 2) { null } else {
        Array.tabulate(x.size() - 1) { i =>
          gf.createLinearRing(SimpleFeatureReadSupport.zip(x.get(i + 1), y.get(i + 1)))
        }
      }
      gf.createPolygon(shell, holes)
    }
  }

  type ParquetMultiPolygon = ParquetGeometryFn[MultiPolygon, java.util.List[java.util.List[java.util.List[Double]]]]

  class ParquetMultiPolygonFn extends ParquetMultiPolygon("parquetMultiPolygon") {
    override def getInstance: ParquetMultiPolygonFn = new ParquetMultiPolygonFn()
    override protected def eval(
        x: java.util.List[java.util.List[java.util.List[Double]]],
        y: java.util.List[java.util.List[java.util.List[Double]]]): MultiPolygon = {
      val polys = Array.tabulate(x.size) { j =>
        val shell = gf.createLinearRing(SimpleFeatureReadSupport.zip(x.get(j).get(0), y.get(j).get(0)))
        val holes = if (x.get(j).size < 2) { null } else {
          Array.tabulate(x.get(j).size() - 1) { i =>
            gf.createLinearRing(SimpleFeatureReadSupport.zip(x.get(j).get(i + 1), y.get(j).get(i + 1)))
          }
        }
        gf.createPolygon(shell, holes)
      }
      gf.createMultiPolygon(polys)
    }
  }
}
