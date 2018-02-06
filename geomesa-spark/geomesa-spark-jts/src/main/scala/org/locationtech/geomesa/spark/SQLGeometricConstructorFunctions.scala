/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import com.vividsolutions.jts.geom._
import org.locationtech.geomesa.spark.SQLFunctionHelper._
import org.apache.spark.sql.{Column, SQLContext}
import org.apache.spark.sql.jts.JTSTypes
import org.geotools.geometry.jts.JTS
import org.locationtech.geomesa.spark.SpatialEncoders._

object SQLGeometricConstructorFunctions {

  val ST_GeomFromWKT: String => Geometry = nullableUDF(text => WKTUtils.read(text))
  val ST_GeomFromWKB: Array[Byte] => Geometry = nullableUDF(array => WKBUtils.read(array))
  val ST_LineFromText: String => LineString = nullableUDF(text => WKTUtils.read(text).asInstanceOf[LineString])
  val ST_MakeBox2D: (Point, Point) => Geometry = nullableUDF((lowerLeft, upperRight) =>
    JTS.toGeometry(new Envelope(lowerLeft.getX, upperRight.getX, lowerLeft.getY, upperRight.getY)))
  val ST_MakeBBOX: (Double, Double, Double, Double) => Geometry = nullableUDF((lowerX, lowerY, upperX, upperY) =>
    JTS.toGeometry(new Envelope(lowerX, upperX, lowerY, upperY)))
  val ST_MakePolygon: LineString => Polygon = nullableUDF(shell => {
    val ring = JTSTypes.geomFactory.createLinearRing(shell.getCoordinateSequence)
    JTSTypes.geomFactory.createPolygon(ring)
  })
  val ST_MakePoint: (Double, Double) => Point = nullableUDF((x, y) => JTSTypes.geomFactory.createPoint(new Coordinate(x, y)))
  val ST_MakeLine: Seq[Point] => LineString = nullableUDF(s => JTSTypes.geomFactory.createLineString(s.map(_.getCoordinate).toArray))
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

  def st_geomFromWKT(wkt: Column) = udfToColumn(ST_GeomFromWKT, "st_geomFromWKT", wkt).as[Geometry]
  def st_geomFromWKT(wkt: String) = udfToColumnLiterals(ST_GeomFromWKT, "st_geomFromWKT", wkt).as[Geometry]


  def st_geomFromWKB(wkb: Column) = udfToColumn(ST_GeomFromWKB, "st_geomFromWKB", wkb).as[Geometry]
  def st_geomFromWKB(wkb: Array[Byte]) = udfToColumnLiterals(ST_GeomFromWKB, "st_geomFromWKB", wkb).as[Geometry]

  def st_lineFromText(wkt: Column) = udfToColumn(ST_LineFromText, "st_lineFromText", wkt).as[LineString]
  def st_lineFromText(wkt: String) = udfToColumnLiterals(ST_LineFromText, "st_lineFromText", wkt).as[LineString]

  def st_makeBox2D(lowerLeft: Column, upperRight: Column) =
    udfToColumn(ST_MakeBox2D, "st_makeBox2D", lowerLeft, upperRight).as[Geometry]
  def st_makeBox2D(lowerLeft: Point, upperRight: Point) =
    udfToColumnLiterals(ST_MakeBox2D, "st_makeBox2D", lowerLeft, upperRight).as[Geometry]

  def st_makeBBOX(lowerX: Column, upperX: Column, lowerY: Column, upperY: Column) =
    udfToColumn(ST_MakeBBOX, "st_makeBBOX", lowerX, upperX, lowerY, upperY).as[Geometry]
  def st_makeBBOX(lowerX: Double, upperX: Double, lowerY: Double, upperY: Double) =
    udfToColumnLiterals(ST_MakeBBOX, "st_makeBBOX", lowerX, upperX, lowerY, upperY).as[Geometry]

  def st_makePolygon(lineString: Column) = udfToColumn(ST_MakePolygon, "st_makePolygon", lineString).as[Polygon]
  def st_makePolygon(lineString: LineString) = udfToColumnLiterals(ST_MakePolygon, "st_makePolygon", lineString).as[Polygon]

  def st_makePoint(x: Column, y: Column) = udfToColumn(ST_MakePoint, "st_makePoint", x, y).as[Point]
  def st_makePoint(x: Double, y: Double) = udfToColumnLiterals(ST_MakePoint, "st_makePoint", x, y).as[Point]

  def st_makeLine(pointSeq: Column) = udfToColumn(ST_MakeLine, "st_makeLine", pointSeq).as[LineString]
  def st_makeLine(pointSeq: Seq[Point]) = udfToColumnLiterals(ST_MakeLine, "st_makeLine", pointSeq).as[LineString]

  def st_makePointM(x: Column, y: Column, m: Column) = udfToColumn(ST_MakePointM, "st_makePointM", x, y, m).as[Point]
  def st_makePointM(x: Double, y: Double, m: Double) = udfToColumnLiterals(ST_MakePointM, "st_makePointM", x, y, m).as[Point]

  def st_mLineFromText(wkt: Column) = udfToColumn(ST_MLineFromText, "st_mLineFromText", wkt).as[MultiLineString]
  def st_mLineFromText(wkt: String) = udfToColumnLiterals(ST_MLineFromText, "st_mLineFromText", wkt).as[MultiLineString]

  def st_mPointFromText(wkt: Column) = udfToColumn(ST_MPointFromText, "st_mPointFromText", wkt).as[MultiPoint]
  def st_mPointFromText(wkt: String) = udfToColumnLiterals(ST_MPointFromText, "st_mPointFromText", wkt).as[MultiPoint]

  def st_mPolyFromText(wkt: Column) = udfToColumn(ST_MPolyFromText, "st_mPolyFromText", wkt).as[MultiPolygon]
  def st_mPolyFromText(wkt: String) = udfToColumnLiterals(ST_MPolyFromText, "st_mPolyFromText", wkt).as[MultiPolygon]

  def st_point(x: Column, y: Column) = udfToColumn(ST_Point, "st_point", x, y).as[Point]
  def st_point(x: Double, y: Double) = udfToColumnLiterals(ST_Point, "st_point", x, y).as[Point]

  def st_pointFromText(wkt: Column) = udfToColumn(ST_PointFromText, "st_pointFromText", wkt).as[Point]
  def st_pointFromText(wkt: String) = udfToColumnLiterals(ST_PointFromText, "st_pointFromText", wkt).as[Point]

  def st_pointFromWKB(wkb: Column) = udfToColumn(ST_PointFromWKB, "st_pointFromWKB", wkb).as[Point]
  def st_pointFromWKB(wkb: Array[Byte]) = udfToColumnLiterals(ST_PointFromWKB, "st_pointFromWKB", wkb).as[Point]

  def st_polygon(lineString: Column) = udfToColumn(ST_Polygon, "st_polygon", lineString).as[Polygon]
  def st_polygon(lineString: LineString) = udfToColumnLiterals(ST_Polygon, "st_polygon", lineString).as[Polygon]

  def st_polygonFromText(wkt: Column) = udfToColumn(ST_PolygonFromText, "st_polygonFromText", wkt).as[Polygon]
  def st_polygonFromText(wkt: String) = udfToColumnLiterals(ST_PolygonFromText, "st_polygonFromText", wkt).as[Polygon]


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
