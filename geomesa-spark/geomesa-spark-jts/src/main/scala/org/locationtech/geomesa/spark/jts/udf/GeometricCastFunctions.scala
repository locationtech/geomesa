/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import java.nio.charset.StandardCharsets

import org.apache.spark.sql.SQLContext
import org.locationtech.geomesa.spark.jts.udf.NullableUDF._
import org.locationtech.geomesa.spark.jts.udf.UDFFactory.Registerable
import org.locationtech.jts.geom._

object GeometricCastFunctions extends UDFFactory {

  class ST_CastToPoint extends NullableUDF1[Geometry, Point](_.asInstanceOf[Point])
  class ST_CastToPolygon extends NullableUDF1[Geometry, Polygon](_.asInstanceOf[Polygon])
  class ST_CastToLineString extends NullableUDF1[Geometry, LineString](_.asInstanceOf[LineString])
  class ST_CastToGeometry extends NullableUDF1[Geometry, Geometry](g => g)
  class ST_ByteArray extends NullableUDF1[String, Array[Byte]](_.getBytes(StandardCharsets.UTF_8))

  val ST_CastToPoint = new ST_CastToPoint()
  val ST_CastToPolygon = new ST_CastToPolygon()
  val ST_CastToLineString = new ST_CastToLineString()
  val ST_CastToGeometry = new ST_CastToGeometry()
  val ST_ByteArray = new ST_ByteArray()

  override def udfs: Seq[Registerable] =
    Seq(
      ST_CastToPoint,
      ST_CastToPolygon,
      ST_CastToLineString,
      ST_CastToGeometry,
      ST_ByteArray
    )
}
