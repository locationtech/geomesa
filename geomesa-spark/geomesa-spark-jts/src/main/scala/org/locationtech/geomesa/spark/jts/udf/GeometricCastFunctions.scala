/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import java.nio.charset.StandardCharsets

import org.locationtech.jts.geom._
import org.apache.spark.sql.SQLContext
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper._

object GeometricCastFunctions {
  val ST_CastToPoint:      Geometry => Point       = g => g.asInstanceOf[Point]
  val ST_CastToPolygon:    Geometry => Polygon     = g => g.asInstanceOf[Polygon]
  val ST_CastToLineString: Geometry => LineString  = g => g.asInstanceOf[LineString]
  val ST_CastToGeometry:   Geometry => Geometry    = g => g
  val ST_ByteArray: (String) => Array[Byte] =
    nullableUDF((string) => string.getBytes(StandardCharsets.UTF_8))

  private[geomesa] val castingNames = Map(
    ST_CastToPoint -> "st_castToPoint",
    ST_CastToPolygon -> "st_castToPolygon",
    ST_CastToLineString -> "st_castToLineString",
    ST_CastToGeometry -> "st_castToGeometry",
    ST_ByteArray -> "st_byteArray"
  )

  private[jts] def registerFunctions(sqlContext: SQLContext): Unit = {
    // Register type casting functions
    sqlContext.udf.register(castingNames(ST_CastToPoint), ST_CastToPoint)
    sqlContext.udf.register(castingNames(ST_CastToPolygon), ST_CastToPolygon)
    sqlContext.udf.register(castingNames(ST_CastToLineString), ST_CastToLineString)
    sqlContext.udf.register(castingNames(ST_CastToGeometry), ST_CastToGeometry)
    sqlContext.udf.register(castingNames(ST_ByteArray), ST_ByteArray)
  }
}
