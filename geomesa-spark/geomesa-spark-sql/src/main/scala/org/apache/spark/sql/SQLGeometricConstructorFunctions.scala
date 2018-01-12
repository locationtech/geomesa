/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.apache.spark.sql

import com.vividsolutions.jts.geom._
import org.geotools.geometry.jts.JTS
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeoHash}
import org.locationtech.geomesa.spark.SQLFunctionHelper.nullableUDF

object SQLGeometricConstructorFunctions {

  val ST_GeomFromGeoHash: (String, Int) => Geometry = nullableUDF((hash, prec) => GeoHash(hash, prec).geom)
  val ST_MakeBox2D: (Point, Point) => Geometry = nullableUDF((lowerLeft, upperRight) =>
    JTS.toGeometry(new Envelope(lowerLeft.getX, upperRight.getX, lowerLeft.getY, upperRight.getY)))
  val ST_MakeBBOX: (Double, Double, Double, Double) => Geometry = nullableUDF((lowerX, lowerY, upperX, upperY) =>
    JTS.toGeometry(BoundingBox(lowerX, upperX, lowerY, upperY)))
  val ST_PointFromGeoHash: (String, Int) => Point = nullableUDF((hash, prec) => GeoHash(hash, prec).getPoint)

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_box2DFromGeoHash"  , ST_GeomFromGeoHash)
    sqlContext.udf.register("st_geomFromGeoHash"   , ST_GeomFromGeoHash)
    sqlContext.udf.register("st_makeBBOX"          , ST_MakeBBOX)
    sqlContext.udf.register("st_makeBox2D"         , ST_MakeBox2D)
    sqlContext.udf.register("st_pointFromGeoHash"  , ST_PointFromGeoHash)
  }
}
