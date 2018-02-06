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
import org.apache.spark.sql.{Column, SQLContext, TypedColumn}
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

  private[geomesa] val namer = Map(
    ST_GeomFromWKT -> "st_geomFromWKT",
    ST_GeomFromWKB -> "st_geomFromWKB",
    ST_LineFromText -> "st_lineFromText",
    ST_MakeBox2D -> "st_makeBox2D",
    ST_MakeBBOX -> "st_makeBBOX",
    ST_MakePolygon -> "st_makePolygon",
    ST_MakePoint -> "st_makePoint",
    ST_MakeLine -> "st_makeLine",
    ST_MakePointM -> "st_makePointM",
    ST_MLineFromText -> "st_mLineFromText",
    ST_MPointFromText -> "st_mPointFromText",
    ST_MPolyFromText -> "st_mPolyFromText",
    ST_Point -> "st_point",
    ST_PointFromText -> "st_pointFromText",
    ST_PointFromWKB -> "st_pointFromWKB",
    ST_Polygon -> "st_polygon",
    ST_PolygonFromText  -> "st_polygonFromText"
  )

  def st_geomFromWKT(wkt: Column): TypedColumn[Any, Geometry] =
    udfToColumn(ST_GeomFromWKT, namer, wkt)
  def st_geomFromWKT(wkt: String): TypedColumn[Any, Geometry] =
    udfToColumnLiterals(ST_GeomFromWKT, namer, wkt)

  def st_geomFromWKB(wkb: Column): TypedColumn[Any, Geometry] =
    udfToColumn(ST_GeomFromWKB, namer, wkb)
  def st_geomFromWKB(wkb: Array[Byte]): TypedColumn[Any, Geometry] =
    udfToColumnLiterals(ST_GeomFromWKB, namer, wkb)

  def st_lineFromText(wkt: Column): TypedColumn[Any, LineString] =
    udfToColumn(ST_LineFromText, namer, wkt)
  def st_lineFromText(wkt: String): TypedColumn[Any, LineString] =
    udfToColumnLiterals(ST_LineFromText, namer, wkt)

  def st_makeBox2D(lowerLeft: Column, upperRight: Column): TypedColumn[Any, Geometry] =
    udfToColumn(ST_MakeBox2D, namer, lowerLeft, upperRight)
  def st_makeBox2D(lowerLeft: Point, upperRight: Point): TypedColumn[Any, Geometry] =
    udfToColumnLiterals(ST_MakeBox2D, namer, lowerLeft, upperRight)

  def st_makeBBOX(lowerX: Column, upperX: Column, lowerY: Column, upperY: Column): TypedColumn[Any, Geometry] =
    udfToColumn(ST_MakeBBOX, namer, lowerX, upperX, lowerY, upperY)
  def st_makeBBOX(lowerX: Double, upperX: Double, lowerY: Double, upperY: Double): TypedColumn[Any, Geometry] =
    udfToColumnLiterals(ST_MakeBBOX, namer, lowerX, upperX, lowerY, upperY)

  def st_makePolygon(lineString: Column): TypedColumn[Any, Polygon] =
    udfToColumn(ST_MakePolygon, namer, lineString)
  def st_makePolygon(lineString: LineString): TypedColumn[Any, Polygon] =
    udfToColumnLiterals(ST_MakePolygon, namer, lineString)

  def st_makePoint(x: Column, y: Column): TypedColumn[Any, Point] =
    udfToColumn(ST_MakePoint, namer, x, y)
  def st_makePoint(x: Double, y: Double): TypedColumn[Any, Point] =
    udfToColumnLiterals(ST_MakePoint, namer, x, y)

  def st_makeLine(pointSeq: Column): TypedColumn[Any, LineString] =
    udfToColumn(ST_MakeLine, namer, pointSeq)
  def st_makeLine(pointSeq: Seq[Point]): TypedColumn[Any, LineString] =
    udfToColumnLiterals(ST_MakeLine, namer, pointSeq)

  def st_makePointM(x: Column, y: Column, m: Column): TypedColumn[Any, Point] =
    udfToColumn(ST_MakePointM, namer, x, y, m)
  def st_makePointM(x: Double, y: Double, m: Double): TypedColumn[Any, Point] =
    udfToColumnLiterals(ST_MakePointM, namer, x, y, m)

  def st_mLineFromText(wkt: Column): TypedColumn[Any, MultiLineString] =
    udfToColumn(ST_MLineFromText, namer, wkt)
  def st_mLineFromText(wkt: String): TypedColumn[Any, MultiLineString] =
    udfToColumnLiterals(ST_MLineFromText, namer, wkt)

  def st_mPointFromText(wkt: Column): TypedColumn[Any, MultiPoint] =
    udfToColumn(ST_MPointFromText, namer, wkt)
  def st_mPointFromText(wkt: String): TypedColumn[Any, MultiPoint] =
    udfToColumnLiterals(ST_MPointFromText, namer, wkt)

  def st_mPolyFromText(wkt: Column): TypedColumn[Any, MultiPolygon] =
    udfToColumn(ST_MPolyFromText, namer, wkt)
  def st_mPolyFromText(wkt: String): TypedColumn[Any, MultiPolygon] =
    udfToColumnLiterals(ST_MPolyFromText, namer, wkt)

  def st_point(x: Column, y: Column): TypedColumn[Any, Point] =
    udfToColumn(ST_Point, namer, x, y)
  def st_point(x: Double, y: Double): TypedColumn[Any, Point] =
    udfToColumnLiterals(ST_Point, namer, x, y)

  def st_pointFromText(wkt: Column): TypedColumn[Any, Point] =
    udfToColumn(ST_PointFromText, namer, wkt)
  def st_pointFromText(wkt: String): TypedColumn[Any, Point] =
    udfToColumnLiterals(ST_PointFromText, namer, wkt)

  def st_pointFromWKB(wkb: Column): TypedColumn[Any, Point] =
    udfToColumn(ST_PointFromWKB, namer, wkb)
  def st_pointFromWKB(wkb: Array[Byte]): TypedColumn[Any, Point] =
    udfToColumnLiterals(ST_PointFromWKB, namer, wkb)

  def st_polygon(lineString: Column): TypedColumn[Any, Polygon] =
    udfToColumn(ST_Polygon, namer, lineString)
  def st_polygon(lineString: LineString): TypedColumn[Any, Polygon] =
    udfToColumnLiterals(ST_Polygon, namer, lineString)

  def st_polygonFromText(wkt: Column): TypedColumn[Any, Polygon] =
    udfToColumn(ST_PolygonFromText, namer, wkt)
  def st_polygonFromText(wkt: String): TypedColumn[Any, Polygon] =
    udfToColumnLiterals(ST_PolygonFromText, namer, wkt)


  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_geomFromText", ST_GeomFromWKT)
    sqlContext.udf.register("st_geometryFromText", ST_GeomFromWKT)
    sqlContext.udf.register(namer(ST_GeomFromWKT), ST_GeomFromWKT)
    sqlContext.udf.register(namer(ST_GeomFromWKB), ST_GeomFromWKB)
    sqlContext.udf.register(namer(ST_LineFromText), ST_LineFromText)
    sqlContext.udf.register(namer(ST_MLineFromText), ST_MLineFromText)
    sqlContext.udf.register(namer(ST_MPointFromText), ST_MPointFromText)
    sqlContext.udf.register(namer(ST_MPolyFromText), ST_MPolyFromText)
    sqlContext.udf.register(namer(ST_MakeBBOX), ST_MakeBBOX)
    sqlContext.udf.register(namer(ST_MakeBox2D), ST_MakeBox2D)
    sqlContext.udf.register(namer(ST_MakeLine), ST_MakeLine)
    sqlContext.udf.register(namer(ST_MakePoint), ST_MakePoint)
    sqlContext.udf.register(namer(ST_MakePointM), ST_MakePointM)
    sqlContext.udf.register(namer(ST_MakePolygon), ST_MakePolygon)
    sqlContext.udf.register(namer(ST_Point), ST_Point)
    sqlContext.udf.register(namer(ST_PointFromText), ST_PointFromText)
    sqlContext.udf.register(namer(ST_PointFromWKB), ST_PointFromWKB)
    sqlContext.udf.register(namer(ST_Polygon), ST_Polygon)
    sqlContext.udf.register(namer(ST_PolygonFromText), ST_PolygonFromText)
  }
}
