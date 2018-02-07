/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import java.{lang => jl}

import com.vividsolutions.jts.geom._
import org.apache.spark.sql.{Column, SQLContext, TypedColumn}
import org.locationtech.geomesa.spark.util.SQLFunctionHelper._
import org.locationtech.geomesa.spark.jts.encoders.SpatialEncoders._
import org.locationtech.geomesa.spark.jts.encoders.SparkDefaultEncoders._

object SQLSpatialAccessorFunctions {
  val ST_Boundary: Geometry => Geometry = nullableUDF(geom => geom.getBoundary)
  val ST_CoordDim: Geometry => Int = nullableUDF(geom => {
    val coord = geom.getCoordinate
    if (coord.z.isNaN) 2 else 3
  })
  val ST_Dimension: Geometry => Int = nullableUDF(geom => geom.getDimension)
  val ST_Envelope: Geometry => Geometry = nullableUDF(geom => geom.getEnvelope)
  val ST_ExteriorRing: Geometry => LineString = {
    case geom: Polygon => geom.getExteriorRing
    case _ => null
  }
  val ST_GeometryN: (Geometry, Int) => Geometry = nullableUDF((geom, n) => geom.getGeometryN(n-1))
  val ST_GeometryType: Geometry => String = nullableUDF(geom => geom.getGeometryType)
  val ST_InteriorRingN: (Geometry, Int) => Geometry = nullableUDF((geom, n) => {
    geom match {
      case geom: Polygon =>
        if (0 < n && n <= geom.getNumInteriorRing) {
          geom.getInteriorRingN(n-1)
        } else {
          null
        }
      case _ => null
    }
  })
  val ST_IsClosed: Geometry => jl.Boolean = nullableUDF({
    case geom: LineString => geom.isClosed
    case geom: MultiLineString => geom.isClosed
    case _ => true
  })
  val ST_IsCollection: Geometry => jl.Boolean = nullableUDF(geom => geom.isInstanceOf[GeometryCollection])
  val ST_IsEmpty: Geometry => jl.Boolean = nullableUDF(geom => geom.isEmpty)
  val ST_IsRing: Geometry => jl.Boolean = nullableUDF({
    case geom: LineString => geom.isClosed && geom.isSimple
    case geom: MultiLineString => geom.isClosed && geom.isSimple
    case geom => geom.isSimple
  })
  val ST_IsSimple: Geometry => jl.Boolean = nullableUDF(geom => geom.isSimple)
  val ST_IsValid: Geometry => jl.Boolean = nullableUDF(geom => geom.isValid)
  val ST_NumGeometries: Geometry => Int = nullableUDF(geom => geom.getNumGeometries)
  val ST_NumPoints: Geometry => Int = nullableUDF(geom => geom.getNumPoints)
  val ST_PointN: (Geometry, Int) => Point = nullableUDF((geom, n) => {
    geom match {
      case geom: LineString =>
        if (n < 0) {
          geom.getPointN(n + geom.getLength.toInt)
        } else {
          geom.getPointN(n-1)
        }
      case _ => null
    }
  })
  val ST_X: Geometry => jl.Float = {
    case geom: Point => geom.getX.toFloat
    case _ => null
  }
  val ST_Y: Geometry => jl.Float = {
    case geom: Point => geom.getY.toFloat
    case _ => null
  }

  private[geomesa] val namer = Map(
    ST_Boundary -> "st_boundary",
    ST_CoordDim -> "st_coordDim",
    ST_Dimension -> "st_dimension",
    ST_Envelope -> "st_envelope",
    ST_ExteriorRing -> "st_exteriorRing",
    ST_GeometryN -> "st_geometryN",
    ST_GeometryType -> "st_geometryType",
    ST_InteriorRingN -> "st_interiorRingN",
    ST_IsClosed -> "st_isClosed",
    ST_IsCollection -> "st_isCollection",
    ST_IsEmpty -> "st_isEmpty",
    ST_IsRing -> "st_isRing",
    ST_IsSimple -> "st_isSimple",
    ST_IsValid -> "st_isValid",
    ST_NumGeometries -> "st_numGeometries",
    ST_NumPoints -> "st_numPoints",
    ST_PointN -> "st_pointN",
    ST_X -> "st_x",
    ST_Y -> "st_y"
  )

  def st_boundary(geom: Column): TypedColumn[Any, Geometry] =
    udfToColumn(ST_Boundary, namer, geom)
  def st_boundary(geom: Geometry): TypedColumn[Any, Geometry] =
    udfToColumnLiterals(ST_Boundary, namer, geom)

  def st_coordDim(geom: Column): TypedColumn[Any, Int] =
    udfToColumn(ST_CoordDim, namer, geom)
  def st_coordDim(geom: Geometry): TypedColumn[Any, Int] =
    udfToColumnLiterals(ST_CoordDim, namer, geom)

  def st_dimension(geom: Column): TypedColumn[Any, Int] =
    udfToColumn(ST_Dimension, namer, geom)
  def st_dimension(geom: Geometry): TypedColumn[Any, Int] =
    udfToColumnLiterals(ST_Dimension, namer, geom)

  def st_envelope(geom: Column): TypedColumn[Any, Geometry] =
    udfToColumn(ST_Envelope, namer, geom)
  def st_envelope(geom: Geometry): TypedColumn[Any, Geometry] =
    udfToColumnLiterals(ST_Envelope, namer, geom)

  def st_exteriorRing(geom: Column): TypedColumn[Any, LineString] =
    udfToColumn(ST_ExteriorRing, namer, geom)
  def st_exteriorRing(geom: Geometry): TypedColumn[Any, LineString] =
    udfToColumnLiterals(ST_ExteriorRing, namer, geom)

  def st_geometryN(geom: Column, n: Column): TypedColumn[Any, Geometry] =
    udfToColumn(ST_GeometryN, namer, geom, n)
  def st_geometryN(geom: Geometry, n: Int): TypedColumn[Any, Geometry] =
    udfToColumnLiterals(ST_GeometryN, namer, geom, n)

  def st_geometryType(geom: Column): TypedColumn[Any, String] =
    udfToColumn(ST_GeometryType, namer, geom)
  def st_geometryType(geom: Geometry): TypedColumn[Any, String] =
    udfToColumnLiterals(ST_GeometryType, namer, geom)

  def st_interiorRingN(geom: Column, n: Column): TypedColumn[Any, Geometry] =
    udfToColumn(ST_InteriorRingN, namer, geom, n)
  def st_interiorRingN(geom: Geometry, n: Int): TypedColumn[Any, Geometry] =
    udfToColumnLiterals(ST_InteriorRingN, namer, geom, n)

  def st_isClosed(geom: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_IsClosed, namer, geom)
  def st_isClosed(geom: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_IsClosed, namer, geom)

  def st_isCollection(geom: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_IsCollection, namer, geom)
  def st_isCollection(geom: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_IsCollection, namer, geom)

  def st_isEmpty(geom: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_IsEmpty, namer, geom)
  def st_isEmpty(geom: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_IsEmpty, namer, geom)

  def st_isRing(geom: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_IsRing, namer, geom)
  def st_isRing(geom: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_IsRing, namer, geom)

  def st_isSimple(geom: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_IsSimple, namer, geom)
  def st_isSimple(geom: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_IsSimple, namer, geom)

  def st_isValid(geom: Column): TypedColumn[Any, jl.Boolean] =
    udfToColumn(ST_IsValid, namer, geom)
  def st_isValid(geom: Geometry): TypedColumn[Any, jl.Boolean] =
    udfToColumnLiterals(ST_IsValid, namer, geom)

  def st_numGeometries(geom: Column): TypedColumn[Any, Int] =
    udfToColumn(ST_NumGeometries, namer, geom)
  def st_numGeometries(geom: Geometry): TypedColumn[Any, Int] =
    udfToColumnLiterals(ST_NumGeometries, namer, geom)

  def st_numPoints(geom: Column): TypedColumn[Any, Int] =
    udfToColumn(ST_NumPoints, namer, geom)
  def st_numPoints(geom: Geometry): TypedColumn[Any, Int] =
    udfToColumnLiterals(ST_NumPoints, namer, geom)

  def st_pointN(geom: Column, n: Column): TypedColumn[Any, Point] =
    udfToColumn(ST_PointN, namer, geom, n)
  def st_pointN(geom: Geometry, n: Int): TypedColumn[Any, Point] =
    udfToColumnLiterals(ST_PointN, namer, geom, n)

  def st_x(geom: Column): TypedColumn[Any, jl.Float] =
    udfToColumn(ST_X, namer, geom)
  def st_x(geom: Geometry): TypedColumn[Any, jl.Float] =
    udfToColumnLiterals(ST_X, namer, geom)

  def st_y(geom: Column): TypedColumn[Any, jl.Float] =
    udfToColumn(ST_Y, namer, geom)
  def st_y(geom: Geometry): TypedColumn[Any, jl.Float] =
    udfToColumnLiterals(ST_Y, namer, geom)


  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register(namer(ST_Boundary), ST_Boundary)
    sqlContext.udf.register(namer(ST_CoordDim), ST_CoordDim)
    sqlContext.udf.register(namer(ST_Dimension), ST_Dimension)
    sqlContext.udf.register(namer(ST_Envelope), ST_Envelope)
    sqlContext.udf.register(namer(ST_ExteriorRing), ST_ExteriorRing)
    sqlContext.udf.register(namer(ST_GeometryN), ST_GeometryN)
    sqlContext.udf.register(namer(ST_GeometryType), ST_GeometryType)
    sqlContext.udf.register(namer(ST_InteriorRingN), ST_InteriorRingN)
    sqlContext.udf.register(namer(ST_IsClosed), ST_IsClosed)
    sqlContext.udf.register(namer(ST_IsCollection), ST_IsCollection)
    sqlContext.udf.register(namer(ST_IsEmpty), ST_IsEmpty)
    sqlContext.udf.register(namer(ST_IsRing), ST_IsRing)
    sqlContext.udf.register(namer(ST_IsSimple), ST_IsSimple)
    sqlContext.udf.register(namer(ST_IsValid), ST_IsValid)
    sqlContext.udf.register(namer(ST_NumGeometries), ST_NumGeometries)
    sqlContext.udf.register(namer(ST_NumPoints), ST_NumPoints)
    sqlContext.udf.register(namer(ST_PointN), ST_PointN)
    sqlContext.udf.register(namer(ST_X), ST_X)
    sqlContext.udf.register(namer(ST_Y), ST_Y)
  }
}
