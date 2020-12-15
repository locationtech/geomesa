/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import org.apache.spark.sql.{Encoder, Encoders, SQLContext}
import org.locationtech.geomesa.spark.jts.encoders.{SparkDefaultEncoders, SpatialEncoders}
import org.locationtech.geomesa.spark.jts.util.SQLFunctionHelper._
import org.locationtech.geomesa.spark.jts.util.{GeoHashUtils, GeometryUtils, WKBUtils, WKTUtils}
import org.locationtech.jts.geom._

object GeometricConstructorFunctions extends SparkDefaultEncoders with SpatialEncoders {

  implicit def integerEncoder: Encoder[Integer] = Encoders.INT

  @transient
  private val geomFactory: GeometryFactory = new GeometryFactory()

  class ST_GeomFromGeoHash extends NullableUDF2[String, Int, Geometry](GeoHashUtils.decode)
  class ST_GeomFromWKT extends NullableUDF1[String, Geometry](WKTUtils.read)
  class ST_GeomFromWKB extends NullableUDF1[Array[Byte], Geometry](WKBUtils.read)
  class ST_LineFromText extends NullableUDF1[String, LineString](text => WKTUtils.read(text).asInstanceOf[LineString])
  class ST_MakeBox2D extends NullableUDF2[Point, Point, Geometry]((lowerLeft, upperRight) =>
    geomFactory.toGeometry(new Envelope(lowerLeft.getX, upperRight.getX, lowerLeft.getY, upperRight.getY))
  )
  class ST_MakeBBOX extends NullableUDF4[Double, Double, Double, Double, Geometry]((xmin, ymin, xmax, ymax) =>
    GeometryUtils.addWayPointsToBBOX(geomFactory.toGeometry(new Envelope(xmin, xmax, ymin, ymax)))
  )
  class ST_MakePolygon extends NullableUDF1[LineString, Polygon](shell =>
    geomFactory.createPolygon(geomFactory.createLinearRing(shell.getCoordinateSequence))
  )
  class ST_Polygon extends ST_MakePolygon
  class ST_MakePoint extends NullableUDF2[Double, Double, Point]((x, y) =>
    geomFactory.createPoint(new Coordinate(x, y))
  )
  class ST_Point extends ST_MakePoint
  class ST_MakeLine extends NullableUDF1[Seq[Point], LineString](s =>
    geomFactory.createLineString(s.map(_.getCoordinate).toArray)
  )
  class ST_MakePointM extends NullableUDF3[Double, Double, Double, Point]((x, y, m) =>
    WKTUtils.read(s"POINT($x $y $m)").asInstanceOf[Point]
  )
  class ST_MLineFromText extends NullableUDF1[String, MultiLineString](text =>
    WKTUtils.read(text).asInstanceOf[MultiLineString]
  )
  class ST_MPointFromText extends NullableUDF1[String, MultiPoint](text =>
    WKTUtils.read(text).asInstanceOf[MultiPoint]
  )
  class ST_MPolyFromText extends NullableUDF1[String, MultiPolygon](text =>
    WKTUtils.read(text).asInstanceOf[MultiPolygon]
  )
  class ST_PointFromGeoHash extends NullableUDF2[String, Int, Point]((hash, prec) =>
    GeoHashUtils.decode(hash, prec).getInteriorPoint
  )
  class ST_PointFromText extends NullableUDF1[String, Point](text => WKTUtils.read(text).asInstanceOf[Point])
  class ST_PointFromWKB extends NullableUDF1[Array[Byte], Point](array => WKBUtils.read(array).asInstanceOf[Point])
  class ST_PolygonFromText extends NullableUDF1[String, Polygon](text => WKTUtils.read(text).asInstanceOf[Polygon])

  val ST_GeomFromGeoHash  = new ST_GeomFromGeoHash()
  val ST_GeomFromWKT      = new ST_GeomFromWKT()
  val ST_GeomFromWKB      = new ST_GeomFromWKB()
  val ST_LineFromText     = new ST_LineFromText()
  val ST_MakeBox2D        = new ST_MakeBox2D()
  val ST_MakeBBOX         = new ST_MakeBBOX()
  val ST_MakePolygon      = new ST_MakePolygon()
  val ST_Polygon          = new ST_Polygon()
  val ST_MakePoint        = new ST_MakePoint()
  val ST_Point            = new ST_Point()
  val ST_MakeLine         = new ST_MakeLine()
  val ST_MakePointM       = new ST_MakePointM()
  val ST_MLineFromText    = new ST_MLineFromText()
  val ST_MPointFromText   = new ST_MPointFromText()
  val ST_MPolyFromText    = new ST_MPolyFromText()
  val ST_PointFromGeoHash = new ST_PointFromGeoHash()
  val ST_PointFromText    = new ST_PointFromText()
  val ST_PointFromWKB     = new ST_PointFromWKB()
  val ST_PolygonFromText  = new ST_PolygonFromText()

  private[jts] def registerFunctions(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register(ST_GeomFromGeoHash.name, ST_GeomFromGeoHash)
    sqlContext.udf.register(ST_GeomFromWKT.name, ST_GeomFromWKT)
    sqlContext.udf.register(ST_GeomFromWKB.name, ST_GeomFromWKB)
    sqlContext.udf.register(ST_LineFromText.name, ST_LineFromText)
    sqlContext.udf.register(ST_MLineFromText.name, ST_MLineFromText)
    sqlContext.udf.register(ST_MPointFromText.name, ST_MPointFromText)
    sqlContext.udf.register(ST_MPolyFromText.name, ST_MPolyFromText)
    sqlContext.udf.register(ST_MakeBBOX.name, ST_MakeBBOX)
    sqlContext.udf.register(ST_MakeBox2D.name, ST_MakeBox2D)
    sqlContext.udf.register(ST_MakeLine.name, ST_MakeLine)
    sqlContext.udf.register(ST_MakePoint.name, ST_MakePoint)
    sqlContext.udf.register(ST_MakePointM.name, ST_MakePointM)
    sqlContext.udf.register(ST_MakePolygon.name, ST_MakePolygon)
    sqlContext.udf.register(ST_PointFromGeoHash.name, ST_PointFromGeoHash)
    sqlContext.udf.register(ST_PointFromText.name, ST_PointFromText)
    sqlContext.udf.register(ST_PointFromWKB.name, ST_PointFromWKB)
    sqlContext.udf.register(ST_PolygonFromText.name, ST_PolygonFromText)
    sqlContext.udf.register(ST_Point.name, ST_Point)
    sqlContext.udf.register(ST_Polygon.name, ST_Polygon)

    sqlContext.udf.register("st_box2DFromGeoHash", ST_GeomFromGeoHash)
    sqlContext.udf.register("st_geomFromText", ST_GeomFromWKT)
    sqlContext.udf.register("st_geometryFromText", ST_GeomFromWKT)
  }
}
