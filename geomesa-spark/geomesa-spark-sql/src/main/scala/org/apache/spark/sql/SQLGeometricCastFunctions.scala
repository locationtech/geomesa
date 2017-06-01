/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql

import java.nio.charset.StandardCharsets
import com.vividsolutions.jts.geom._
import org.apache.spark.sql.SQLFunctionHelper.nullableUDF

object SQLGeometricCastFunctions {
  val ST_CastToPoint:      Geometry => Point       = g => g.asInstanceOf[Point]
  val ST_CastToPolygon:    Geometry => Polygon     = g => g.asInstanceOf[Polygon]
  val ST_CastToLineString: Geometry => LineString  = g => g.asInstanceOf[LineString]
  val ST_ByteArray: (String) => Array[Byte] =
    nullableUDF((string) => string.getBytes(StandardCharsets.UTF_8))

  def registerFunctions(sqlContext: SQLContext): Unit = {
    // Register type casting functions
    sqlContext.udf.register("st_castToPoint", ST_CastToPoint)
    sqlContext.udf.register("st_castToPolygon", ST_CastToPolygon)
    sqlContext.udf.register("st_castToLineString", ST_CastToLineString)
    sqlContext.udf.register("st_byteArray", ST_ByteArray)
  }
}
