/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import org.apache.spark.sql.SQLContext
import org.locationtech.geomesa.spark.jts.util.GeoHashUtils._
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper._
import org.locationtech.geomesa.spark.jts.util.{GeometryUtils, WKBUtils, WKTUtils}
import org.locationtech.jts.geom._

object GeometricConstructorFunctions {

  @transient
  private val geomFactory: GeometryFactory = new GeometryFactory()

  val ST_GeomFromGeoHash: (String, Int) => Geometry = nullableUDF((hash, prec) => decode(hash, prec))
  val ST_GeomFromWKT: String => Geometry = nullableUDF(text => WKTUtils.read(text))
  val ST_GeomFromWKB: Array[Byte] => Geometry = nullableUDF(array => WKBUtils.read(array))
  val ST_LineFromText: String => LineString = nullableUDF(text => WKTUtils.read(text).asInstanceOf[LineString])
  val ST_MakeBox2D: (Point, Point) => Geometry = nullableUDF((lowerLeft, upperRight) =>
    geomFactory.toGeometry(new Envelope(lowerLeft.getX, upperRight.getX, lowerLeft.getY, upperRight.getY)))
  val ST_MakeBBOX: (Double, Double, Double, Double) => Geometry = nullableUDF((lowerX, lowerY, upperX, upperY) =>
    GeometryUtils.addWayPointsToBBOX(geomFactory.toGeometry(new Envelope(lowerX, upperX, lowerY, upperY))))
  val ST_MakePolygon: LineString => Polygon = nullableUDF(shell => {
    val ring = geomFactory.createLinearRing(shell.getCoordinateSequence)
    geomFactory.createPolygon(ring)
  })
  val ST_MakePoint: (Double, Double) => Point = nullableUDF((x, y) => geomFactory.createPoint(new Coordinate(x, y)))
  val ST_MakeLine: Seq[Point] => LineString = nullableUDF(s => geomFactory.createLineString(s.map(_.getCoordinate).toArray))
  val ST_MakePointM: (Double, Double, Double) => Point = nullableUDF((x, y, m) =>
    WKTUtils.read(s"POINT($x $y $m)").asInstanceOf[Point])
  val ST_MLineFromText: String => MultiLineString = nullableUDF(text => WKTUtils.read(text).asInstanceOf[MultiLineString])
  val ST_MPointFromText: String => MultiPoint = nullableUDF(text => WKTUtils.read(text).asInstanceOf[MultiPoint])
  val ST_MPolyFromText: String => MultiPolygon = nullableUDF(text => WKTUtils.read(text).asInstanceOf[MultiPolygon])
  val ST_Point: (Double, Double) => Point = (x, y) => ST_MakePoint(x, y)
  val ST_PointFromGeoHash: (String, Int) => Point = nullableUDF((hash, prec) => decode(hash, prec).getInteriorPoint)
  val ST_PointFromText: String => Point = nullableUDF(text => WKTUtils.read(text).asInstanceOf[Point])
  val ST_PointFromWKB: Array[Byte] => Point = array => ST_GeomFromWKB(array).asInstanceOf[Point]
  val ST_Polygon: LineString => Polygon = shell => ST_MakePolygon(shell)
  val ST_PolygonFromText: String => Polygon = nullableUDF(text => WKTUtils.read(text).asInstanceOf[Polygon])

  private[geomesa] val constructorNames = Map(
    ST_GeomFromGeoHash -> "st_geomFromGeoHash",
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
    ST_PointFromGeoHash -> "st_pointFromGeoHash",
    ST_PointFromText -> "st_pointFromText",
    ST_PointFromWKB -> "st_pointFromWKB",
    ST_Polygon -> "st_polygon",
    ST_PolygonFromText  -> "st_polygonFromText"
  )

  private[jts] def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_box2DFromGeoHash", ST_GeomFromGeoHash)
    sqlContext.udf.register(constructorNames(ST_GeomFromGeoHash), ST_GeomFromGeoHash)
    sqlContext.udf.register("st_geomFromText", ST_GeomFromWKT)
    sqlContext.udf.register("st_geometryFromText", ST_GeomFromWKT)
    sqlContext.udf.register(constructorNames(ST_GeomFromWKT), ST_GeomFromWKT)
    sqlContext.udf.register(constructorNames(ST_GeomFromWKB), ST_GeomFromWKB)
    sqlContext.udf.register(constructorNames(ST_LineFromText), ST_LineFromText)
    sqlContext.udf.register(constructorNames(ST_MLineFromText), ST_MLineFromText)
    sqlContext.udf.register(constructorNames(ST_MPointFromText), ST_MPointFromText)
    sqlContext.udf.register(constructorNames(ST_MPolyFromText), ST_MPolyFromText)
    sqlContext.udf.register(constructorNames(ST_MakeBBOX), ST_MakeBBOX)
    sqlContext.udf.register(constructorNames(ST_MakeBox2D), ST_MakeBox2D)
    sqlContext.udf.register(constructorNames(ST_MakeLine), ST_MakeLine)
    sqlContext.udf.register(constructorNames(ST_MakePoint), ST_MakePoint)
    sqlContext.udf.register(constructorNames(ST_MakePointM), ST_MakePointM)
    sqlContext.udf.register(constructorNames(ST_MakePolygon), ST_MakePolygon)
    sqlContext.udf.register(constructorNames(ST_Point), ST_Point)
    sqlContext.udf.register(constructorNames(ST_PointFromGeoHash), ST_PointFromGeoHash)
    sqlContext.udf.register(constructorNames(ST_PointFromText), ST_PointFromText)
    sqlContext.udf.register(constructorNames(ST_PointFromWKB), ST_PointFromWKB)
    sqlContext.udf.register(constructorNames(ST_Polygon), ST_Polygon)
    sqlContext.udf.register(constructorNames(ST_PolygonFromText), ST_PolygonFromText)
  }
}
