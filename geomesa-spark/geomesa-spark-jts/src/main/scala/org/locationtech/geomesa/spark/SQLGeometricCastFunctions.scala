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
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.locationtech.geomesa.spark.SpatialEncoders._
import org.locationtech.geomesa.spark.SparkDefaultEncoders._

object SQLGeometricCastFunctions {
  val ST_CastToPoint:      Geometry => Point       = g => g.asInstanceOf[Point]
  val ST_CastToPolygon:    Geometry => Polygon     = g => g.asInstanceOf[Polygon]
  val ST_CastToLineString: Geometry => LineString  = g => g.asInstanceOf[LineString]
  val ST_ByteArray: (String) => Array[Byte] =
    nullableUDF((string) => string.getBytes(StandardCharsets.UTF_8))

  def castToPoint(geom: Column) = udfToColumn(ST_CastToPoint, "castToPoint", geom).as[Point]
  def castToPoint(geom: Geometry) = udfToColumnLiterals(ST_CastToPoint, "castToPoint", geom).as[Point]

  def castToPolygon(geom: Column) = udfToColumn(ST_CastToPoint, "castToPolygon", geom).as[Polygon]
  def castToPolygon(geom: Geometry) = udfToColumnLiterals(ST_CastToPoint, "castToPolygon", geom).as[Polygon]

  def castToLineString(geom: Column) = udfToColumn(ST_CastToLineString, "castToLineString", geom).as[LineString]
  def castToLineString(geom: Geometry) = udfToColumnLiterals(ST_CastToLineString, "castToLineString", geom).as[LineString]

  def byteArray(str: Column) = udfToColumn(ST_ByteArray, "byteArray", str).as[Array[Byte]]
  def byteArray(str: String) = udfToColumnLiterals(ST_ByteArray, "byteArray", str).as[Array[Byte]]


  def registerFunctions(sqlContext: SQLContext): Unit = {
    // Register type casting functions
    sqlContext.udf.register("st_castToPoint", ST_CastToPoint)
    sqlContext.udf.register("st_castToPolygon", ST_CastToPolygon)
    sqlContext.udf.register("st_castToLineString", ST_CastToLineString)
    sqlContext.udf.register("st_byteArray", ST_ByteArray)
  }
}
