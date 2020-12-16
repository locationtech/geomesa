/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import org.apache.spark.sql.SQLContext
import org.locationtech.geomesa.spark.jts.udf.NullableUDF._
import org.locationtech.geomesa.spark.jts.udf.UDFFactory.Registerable
import org.locationtech.geomesa.spark.jts.util.{GeoHashUtils, GeometryUtils, WKBUtils, WKTUtils}
import org.locationtech.jts.geom._

object GeometricConstructorFunctions extends UDFFactory {

  @transient
  private val geomFactory: GeometryFactory = new GeometryFactory()

  class ST_GeomFromGeoHash extends NullableUDF2[String, Int, Geometry](GeoHashUtils.decode)
  class ST_Box2DFromGeoHash extends ST_GeomFromGeoHash
  class ST_GeomFromWKT extends NullableUDF1[String, Geometry](WKTUtils.read)
  class ST_GeomFromText extends ST_GeomFromWKT
  class ST_GeometryFromText extends ST_GeomFromWKT
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
  val ST_Box2DFromGeoHash = new ST_Box2DFromGeoHash()
  val ST_GeomFromWKT      = new ST_GeomFromWKT()
  val ST_GeomFromText     = new ST_GeomFromText()
  val ST_GeometryFromText = new ST_GeometryFromText()
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

  override def udfs: Seq[Registerable] =
    Seq(
      ST_GeomFromGeoHash,
      ST_Box2DFromGeoHash,
      ST_GeomFromWKT,
      ST_GeomFromText,
      ST_GeometryFromText,
      ST_GeomFromWKB,
      ST_LineFromText,
      ST_MLineFromText,
      ST_MPointFromText,
      ST_MPolyFromText,
      ST_MakeBBOX,
      ST_MakeBox2D,
      ST_MakeLine,
      ST_MakePoint,
      ST_MakePointM,
      ST_MakePolygon,
      ST_PointFromGeoHash,
      ST_PointFromText,
      ST_PointFromWKB,
      ST_PolygonFromText,
      ST_Point,
      ST_Polygon
    )
}
