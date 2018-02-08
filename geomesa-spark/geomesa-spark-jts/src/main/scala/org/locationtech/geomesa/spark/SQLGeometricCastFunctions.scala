/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.nio.charset.StandardCharsets

import com.vividsolutions.jts.geom._
import org.locationtech.geomesa.spark.SQLFunctionHelper._
import org.apache.spark.sql.{Column, SQLContext, TypedColumn}
import org.locationtech.geomesa.spark.SpatialEncoders._
import org.locationtech.geomesa.spark.SparkDefaultEncoders._

object SQLGeometricCastFunctions {
  val ST_CastToPoint:      Geometry => Point       = g => g.asInstanceOf[Point]
  val ST_CastToPolygon:    Geometry => Polygon     = g => g.asInstanceOf[Polygon]
  val ST_CastToLineString: Geometry => LineString  = g => g.asInstanceOf[LineString]
  val ST_ByteArray: (String) => Array[Byte] =
    nullableUDF((string) => string.getBytes(StandardCharsets.UTF_8))

  private[geomesa] val namer = Map(
    ST_CastToPoint -> "st_castToPoint",
    ST_CastToPolygon -> "st_castToPolygon",
    ST_CastToLineString -> "st_castToLineString",
    ST_ByteArray -> "st_byteArray"
  )

  def st_castToPoint(geom: Column): TypedColumn[Any, Point] =
    udfToColumn(ST_CastToPoint, namer, geom)
  def st_castToPoint(geom: Geometry): TypedColumn[Any, Point] =
    udfToColumnLiterals(ST_CastToPoint, namer, geom)

  def st_castToPolygon(geom: Column): TypedColumn[Any, Polygon] =
    udfToColumn(ST_CastToPolygon, namer, geom)
  def st_castToPolygon(geom: Geometry): TypedColumn[Any, Polygon] =
    udfToColumnLiterals(ST_CastToPolygon, namer, geom)

  def st_castToLineString(geom: Column): TypedColumn[Any, LineString] =
    udfToColumn(ST_CastToLineString, namer, geom)
  def st_castToLineString(geom: Geometry): TypedColumn[Any, LineString] =
    udfToColumnLiterals(ST_CastToLineString, namer, geom)

  def st_byteArray(str: Column): TypedColumn[Any, Array[Byte]] =
    udfToColumn(ST_ByteArray, namer, str)
  def st_byteArray(str: String): TypedColumn[Any, Array[Byte]] =
    udfToColumnLiterals(ST_ByteArray, namer, str)

  def registerFunctions(sqlContext: SQLContext): Unit = {
    // Register type casting functions
    sqlContext.udf.register(namer(ST_CastToPoint), ST_CastToPoint)
    sqlContext.udf.register(namer(ST_CastToPolygon), ST_CastToPolygon)
    sqlContext.udf.register(namer(ST_CastToLineString), ST_CastToLineString)
    sqlContext.udf.register(namer(ST_ByteArray), ST_ByteArray)
  }
}
