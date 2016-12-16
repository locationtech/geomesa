/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.apache.spark.sql

import com.vividsolutions.jts.geom._
import org.geotools.geometry.jts.JTS
import org.locationtech.geomesa.utils.geohash.{BoundingBox, GeoHash}
import org.locationtech.geomesa.utils.text.{WKBUtils, WKTUtils}
import org.apache.spark.sql.SQLContext

object SQLGeometricConstructorFunctions {
  val ST_GeomFromGeoHash: (String, Int) => Geometry = (hash, prec) => GeoHash(hash, prec).geom
  val ST_Box2DFromGeoHash: (String, Int) => Geometry = (hash, prec) => ST_GeomFromGeoHash(hash, prec)
  val ST_GeomFromWKT: String => Geometry = text => WKTUtils.read(text)
  val ST_GeomFromWKB: Array[Byte] => Geometry = array => WKBUtils.read(array)
  val ST_MakeBox2D: (Point, Point) => Polygon = (ll, ur) =>
    JTS.toGeometry(new Envelope(ll.getX, ur.getX, ll.getY, ur.getY))
  val ST_MakeBBOX: (Double, Double, Double, Double) => Geometry = (lx, ly, ux, uy) =>
    JTS.toGeometry(BoundingBox(lx, ux, ly, uy))
  val ST_MakePolygon: LineString => Polygon = shell => {
    val factory = new GeometryFactory()
    val ring = factory.createLinearRing(shell.getCoordinateSequence)
    new GeometryFactory().createPolygon(ring)
  }
  val ST_MakePoint: (Double, Double) => Point = (x, y) => WKTUtils.read(s"POINT($x $y)").asInstanceOf[Point]
  val ST_MakePointM: (Double, Double, Double) => Point = (x, y, m) =>
    WKTUtils.read(s"POINT($x $y $m)").asInstanceOf[Point]
  val ST_MLineFromText: String => MultiLineString = (text) => WKTUtils.read(text).asInstanceOf[MultiLineString]
  val ST_MPointFromText: String => MultiPoint = (text) => WKTUtils.read(text).asInstanceOf[MultiPoint]
  val ST_MPolyFromText: String => MultiPolygon = (text) => WKTUtils.read(text).asInstanceOf[MultiPolygon]
  val ST_Point: (Double, Double) => Point = (x, y) => ST_MakePoint(x, y)
  val ST_PointFromGeoHash: (String, Int) => Point = (hash, prec) => GeoHash(hash, prec).getPoint
  val ST_PointFromText: String => Point = text => WKTUtils.read(text).asInstanceOf[Point]
  val ST_PointFromWKB: Array[Byte] => Point = array => ST_GeomFromWKB(array).asInstanceOf[Point]
  val ST_Polygon: LineString => Polygon = shell => ST_MakePolygon(shell)
  val ST_PolygonFromText: String => Polygon = text => WKTUtils.read(text).asInstanceOf[Polygon]

  def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("st_box2DFromGeoHash"  , ST_GeomFromGeoHash)
    sqlContext.udf.register("st_geomFromGeoHash"   , ST_GeomFromGeoHash)
    sqlContext.udf.register("st_geomFromWKT"       , ST_GeomFromWKT)
    sqlContext.udf.register("st_geometryFromText"  , ST_GeomFromWKT)
    sqlContext.udf.register("st_geomFromWKB"       , ST_GeomFromWKB)
    sqlContext.udf.register("st_makeBox2D"         , ST_MakeBox2D)
    sqlContext.udf.register("st_makeBBOX"          , ST_MakeBBOX)
    sqlContext.udf.register("st_makePolygon"       , ST_MakePolygon)
    sqlContext.udf.register("st_makePoint"         , ST_MakePoint)
    sqlContext.udf.register("st_makePointM"        , ST_MakePointM)
    sqlContext.udf.register("st_mLineFromText"     , ST_MLineFromText)
    sqlContext.udf.register("st_mPointFromText"    , ST_MPointFromText)
    sqlContext.udf.register("st_mPolyFromText"     , ST_MPolyFromText)
    sqlContext.udf.register("ST_point"             , ST_Point)
    sqlContext.udf.register("ST_pointFromGeoHash"  , ST_PointFromGeoHash)
    sqlContext.udf.register("st_pointFromText"     , ST_PointFromText)
    sqlContext.udf.register("st_pointFromWKB"      , ST_PointFromWKB)
    sqlContext.udf.register("st_polygon"           , ST_Polygon)
    sqlContext.udf.register("st_polygonFromText"   , ST_PolygonFromText)
  }
}
