/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import com.vividsolutions.jts.geom._
import SQLFunctionHelper.nullableUDF
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.jts.SQLTypes
import org.geotools.geometry.jts.JTS

object SQLGeometricConstructorFunctions {

  val ST_GeomFromWKT: String => Geometry = nullableUDF(text => WKTUtils.read(text))
  val ST_GeomFromWKB: Array[Byte] => Geometry = nullableUDF(array => WKBUtils.read(array))
  val ST_LineFromText: String => LineString = nullableUDF(text => WKTUtils.read(text).asInstanceOf[LineString])
  val ST_MakeBox2D: (Point, Point) => Geometry = nullableUDF((lowerLeft, upperRight) =>
    JTS.toGeometry(new Envelope(lowerLeft.getX, upperRight.getX, lowerLeft.getY, upperRight.getY)))
  val ST_MakeBBOX: (Double, Double, Double, Double) => Geometry = nullableUDF((lowerX, lowerY, upperX, upperY) =>
    JTS.toGeometry(new Envelope(lowerX, upperX, lowerY, upperY)))
  val ST_MakePolygon: LineString => Polygon = nullableUDF(shell => {
    val ring = SQLTypes.geomFactory.createLinearRing(shell.getCoordinateSequence)
    SQLTypes.geomFactory.createPolygon(ring)
  })
  val ST_MakePoint: (Double, Double) => Point = nullableUDF((x, y) => SQLTypes.geomFactory.createPoint(new Coordinate(x, y)))
  val ST_MakeLine: Seq[Point] => LineString = nullableUDF(s => SQLTypes.geomFactory.createLineString(s.map(_.getCoordinate).toArray))
  val ST_MakePointM: (Double, Double, Double) => Point = nullableUDF((x, y, m) =>
    WKTUtils.read(s"POINT($x $y $m)").asInstanceOf[Point])
  val ST_MLineFromText: String => MultiLineString = nullableUDF(text => WKTUtils.read(text).asInstanceOf[MultiLineString])
  val ST_MPointFromText: String => MultiPoint = nullableUDF(text => WKTUtils.read(text).asInstanceOf[MultiPoint])
  val ST_MPolyFromText: String => MultiPolygon = nullableUDF(text => WKTUtils.read(text).asInstanceOf[MultiPolygon])
  val ST_Point: (Double, Double) => Point = (x, y) => ST_MakePoint(x, y)
  val ST_PointFromText: String => Point = nullableUDF(text => WKTUtils.read(text).asInstanceOf[Point])
  val ST_PointFromWKB: Array[Byte] => Point = array => ST_GeomFromWKB(array).asInstanceOf[Point]
  val ST_Polygon: LineString => Polygon = shell => ST_MakePolygon(shell)
  val ST_PolygonFromText: String => Polygon = nullableUDF(text => WKTUtils.read(text).asInstanceOf[Polygon])

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_geomFromText"      , ST_GeomFromWKT)
    sqlContext.udf.register("st_geomFromWKB"       , ST_GeomFromWKB)
    sqlContext.udf.register("st_geomFromWKT"       , ST_GeomFromWKT)
    sqlContext.udf.register("st_geometryFromText"  , ST_GeomFromWKT)
    sqlContext.udf.register("st_lineFromText"      , ST_LineFromText)
    sqlContext.udf.register("st_mLineFromText"     , ST_MLineFromText)
    sqlContext.udf.register("st_mPointFromText"    , ST_MPointFromText)
    sqlContext.udf.register("st_mPolyFromText"     , ST_MPolyFromText)
    sqlContext.udf.register("st_makeBBOX"          , ST_MakeBBOX)
    sqlContext.udf.register("st_makeBox2D"         , ST_MakeBox2D)
    sqlContext.udf.register("st_makeLine"          , ST_MakeLine)
    sqlContext.udf.register("st_makePoint"         , ST_MakePoint)
    sqlContext.udf.register("st_makePointM"        , ST_MakePointM)
    sqlContext.udf.register("st_makePolygon"       , ST_MakePolygon)
    sqlContext.udf.register("st_point"             , ST_Point)
    sqlContext.udf.register("st_pointFromText"     , ST_PointFromText)
    sqlContext.udf.register("st_pointFromWKB"      , ST_PointFromWKB)
    sqlContext.udf.register("st_polygon"           , ST_Polygon)
    sqlContext.udf.register("st_polygonFromText"   , ST_PolygonFromText)
  }
}
