/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import org.locationtech.jts.geom._
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.locationtech.geomesa.spark.jts._
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.locationtech.geomesa.spark.jts.util.util.GeometryContainer
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeometricConstructorFunctionsTest extends Specification with TestEnvironment {

  "sql geometry constructors" should {
    sequential

    // before
    step {
      // Trigger initialization of spark session
      val _ = spark
    }

    "st_box2DFromGeoHash" >> {
      sc.sql("select st_box2DFromGeoHash(null, null)").collect.head(0) must beNull

      val r = sc.sql(
        s"""
           |select st_box2DFromGeoHash('ezs42', 25)
          """.stripMargin
      )

      val boxCoords = r.collect().head.getAs[Geometry](0).getCoordinates
      val ll = boxCoords(0)
      val ur = boxCoords(2)
      boxCoords.length mustEqual 5
      ll.x must beCloseTo(-5.625, .022) // lon
      ll.y must beCloseTo(42.583, .022) // lat
      ur.x must beCloseTo(-5.581, .022) // lon
      ur.y must beCloseTo(42.627, .022) // lat
    }

    "st_geomFromGeoHash" >> {
      sc.sql("select st_geomFromGeoHash(null, null)").collect.head(0) must beNull
      dfBlank.select(st_geomFromGeoHash(lit(null), lit(null))).first must beNull

      val geohash = "ezs42"
      val precision = 25
      val r = sc.sql(
        s"""
           |select st_geomFromGeoHash('$geohash', $precision)
          """.stripMargin
      )

      val (minLon, minLat, maxLon, maxLat) = (-5.625, 42.583, -5.581, 42.627)
      val delta = .022

      val dfBoxCoords = dfBlank.select(st_geomFromGeoHash(lit(geohash), lit(precision))).first.getCoordinates
      val dfLowerLeft = dfBoxCoords(0)
      val dfUpperRight = dfBoxCoords(2)
      dfLowerLeft.x must beCloseTo(minLon, delta)
      dfLowerLeft.y must beCloseTo(minLat, delta)
      dfUpperRight .x must beCloseTo(maxLon, delta)
      dfUpperRight .y must beCloseTo(maxLat, delta)

      val geomboxCoords = r.collect().head.getAs[Geometry](0).getCoordinates
      val ll = geomboxCoords(0)
      val ur = geomboxCoords(2)
      geomboxCoords.length mustEqual 5
      ll.x must beCloseTo(minLon, delta)
      ll.y must beCloseTo(minLat, delta)
      ur.x must beCloseTo(maxLon, delta)
      ur.y must beCloseTo(maxLat, delta)
    }

    "st_pointFromGeoHash" >> {
      sc.sql("select st_pointFromGeoHash(null, null)").collect.head(0) must beNull
      dfBlank.select(st_pointFromGeoHash(lit(null), lit(null))).first must beNull

      val geohash = "ezs42"
      val precision = 25
      val r = sc.sql(
        s"""
           |select st_pointFromGeoHash('$geohash', $precision)
        """.stripMargin
      )

      val (x, y) = (-5.603, 42.605)
      val delta = .022

      val dfPoint = dfBlank.select(st_pointFromGeoHash(lit(geohash), lit(precision))).first
      dfPoint.getX must beCloseTo(x, delta)
      dfPoint.getY must beCloseTo(y, delta)

      val point = r.collect().head.getAs[Point](0)
      point.getX must beCloseTo(x, delta)
      point.getY must beCloseTo(y, delta)
    }

    "st_geomFromWKT" >> {
      sc.sql("select st_geomFromWKT(null)").collect.head(0) must beNull
      dfBlank.select(st_geomFromWKT(lit(null))).first must beNull

      val point = "POINT(0 0)"
      val r = sc.sql(
        s"""
          |select st_geomFromWKT('$point')
        """.stripMargin
      )

      val expected = WKTUtils.read(point)

      r.collect().head.getAs[Geometry](0) mustEqual expected

      dfBlank.select(st_geomFromWKT(point)).first mustEqual expected
    }

    "st_geometryFromText" >> {
      sc.sql("select st_geometryFromText(null)").collect.head(0) must beNull

      val r = sc.sql(
        """
          |select st_geometryFromText('POINT(0 0)')
        """.stripMargin
      )

      r.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_geomFromWKB" >> {
      sc.sql("select st_geomFromWKB(null)").collect.head(0) must beNull
      dfBlank.select(st_geomFromWKB(lit(null))).first must beNull

      val geomArr = Array[Byte](0,
        0, 0, 0, 3,
        0, 0, 0, 1,
        0, 0, 0, 5,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        64, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
      )
      val r = sc.sql(
        s"""select st_geomFromWKB(st_byteArray('${new String(geomArr)}'))"""
      )
      val expected = WKTUtils.read("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))")
      r.collect().head.getAs[Geometry](0) mustEqual expected

      dfBlank.select(st_geomFromWKB(st_byteArray(lit(new String(geomArr))))).first mustEqual expected
      dfBlank.select(st_geomFromWKB(geomArr)).first mustEqual expected
    }

    "st_lineFromText" >> {
      sc.sql("select st_lineFromText(null)").collect.head(0) must beNull
      dfBlank.select(st_lineFromText(lit(null))).first must beNull

      val line = "LINESTRING(0 0, 1 1, 2 2)"
      val r = sc.sql(
        s"""
          |select st_lineFromText('$line')
        """.stripMargin
      )
      val expected = WKTUtils.read(line)
      r.collect().head.getAs[LineString](0) mustEqual expected
      dfBlank.select(st_lineFromText(line)).first mustEqual expected
    }

    "st_makeBBOX" >> {
      sc.sql("select st_makeBBOX(null, null, null, null)").collect.head(0) must beNull
      dfBlank.select(st_makeBBOX(lit(null), lit(null), lit(null), lit(null))).first must beNull

      val r = sc.sql(
        """
          |select st_makeBBOX(0.0, 0.0, 2.0, 2.0)
        """.stripMargin
      )
      val fact = new GeometryFactory()

      val expected = fact.toGeometry(new Envelope(0, 2, 0, 2))
      r.collect().head.getAs[Geometry](0) mustEqual expected

      dfBlank.select(st_makeBBOX(0.0, 0.0, 2.0, 2.0)).first mustEqual expected
    }

    "st_makeBox2D" >> {
      sc.sql("select st_makeBox2D(null, null)").collect.head(0) must beNull
      dfBlank.select(st_makeBox2D(lit(null), lit(null))).first must beNull

      val r = sc.sql(
        """
          |select st_makeBox2D(st_castToPoint(st_geomFromWKT('POINT(0 0)')),
          |                    st_castToPoint(st_geomFromWKT('POINT(2 2)')))
        """.stripMargin
      )
      val expected = WKTUtils.read("POLYGON((0.0 0.0, 0.0 2.0, 2.0 2.0, 2.0 0.0, 0.0 0.0))")
      r.collect().head.getAs[Geometry](0) mustEqual expected

      dfBlank.select(st_makeBox2D(
        st_castToPoint(st_geomFromWKT("POINT(0 0)")),
        st_castToPoint(st_geomFromWKT("POINT(2 2)"))
      )).first mustEqual expected

      val p1 = GeometricConstructorFunctions.ST_MakePoint(0, 0)
      val p2 = GeometricConstructorFunctions.ST_MakePoint(2, 2)

      dfBlank.select(st_makeBox2D(p1, p2)).first mustEqual expected
    }

    "st_makePolygon" >> {
      sc.sql("select st_makePolygon(null)").collect.head(0) must beNull
      dfBlank.select(st_makePolygon(lit(null))).first must beNull

      val line = "LINESTRING(0 0, 2 2, 5 4, 7 2, 5 2, 3 0, 0 0)"
      val r = sc.sql(
        s"""
           |select st_makePolygon(st_castToLineString(
           |    st_geomFromWKT('$line')))
        """.stripMargin
      )
      val expected = WKTUtils.read("POLYGON((0 0, 2 2, 5 4, 7 2, 5 2, 3 0, 0 0))")
      r.collect().head.getAs[Polygon](0) mustEqual expected
      dfBlank.select(st_makePolygon(st_castToLineString(st_geomFromWKT(line)))).first mustEqual expected

      val lineInst = WKTUtils.read(line).asInstanceOf[LineString]
      dfBlank.select(st_makePolygon(lineInst)).first mustEqual expected
    }

    "st_makePoint" >> {
      sc.sql("select st_makePoint(null, null)").collect.head(0) must beNull
      dfBlank.select(st_makePoint(lit(null), lit(null))).first must beNull

      val r = sc.sql(
        """
          |select st_makePoint(0, 1) geom
        """.stripMargin
      )
      val expected = WKTUtils.read("POINT(0 1)")
      r.collect().head.getAs[Point](0) mustEqual expected
      dfBlank.select(st_makePoint(0, 1)).first mustEqual expected

      // it would be nice if this worked (GEOMESA-2454); check that it doesn't so we know if it does in the future
      import spark.implicits._
      r.as[GeometryContainer].head must haveClass[GeometryContainer] must throwAn[AnalysisException]
    }

    "st_makePointM" >> {
      sc.sql("select st_makePointM(null, null, null)").collect.head(0) must beNull
      dfBlank.select(st_makePointM(lit(null), lit(null), lit(null))).first must beNull

      val r = sc.sql(
        """
          |select st_makePointM(0, 0, 1)
        """.stripMargin
      )
      val expected = WKTUtils.read("POINT(0 0 1)")
      r.collect().head.getAs[Point](0) mustEqual expected
      dfBlank.select(st_makePointM(0, 0, 1)).first mustEqual expected
    }

    "st_makeLine" >> {
      sc.sql("select st_makeLine(null)").collect.head(0) must beNull
      dfBlank.select(st_makeLine(lit(null))).first must beNull

      val p1 = GeometricConstructorFunctions.ST_MakePoint(0, 0)
      val p2 = GeometricConstructorFunctions.ST_MakePoint(2, 2)
      val p3 = GeometricConstructorFunctions.ST_MakePoint(5, 2)
      val expected = WKTUtils.read("LINESTRING(0 0, 2 2, 5 2)")

      dfBlank.select(st_makeLine(Seq(p1, p2, p3))).first mustEqual expected

      dfBlank.select(st_makeLine(array(pointLit(p1), pointLit(p2), pointLit(p3)))).first mustEqual expected
    }

    "st_mLineFromText" >> {
      sc.sql("select st_mLineFromText(null)").collect.head(0) must beNull
      dfBlank.select(st_mLineFromText(lit(null))).first must beNull

      val line = "MULTILINESTRING((0 0, 1 1, 2 2), (0 1, 1 2, 2 3))"
      val r = sc.sql(
        s"""
          |select st_mLineFromText('$line')
        """.stripMargin
      )
      val expected = WKTUtils.read("MULTILINESTRING((0 0, 1 1, 2 2), (0 1, 1 2, 2 3))")
      r.collect().head.getAs[MultiLineString](0) mustEqual expected
      dfBlank.select(st_mLineFromText(line)).first mustEqual expected
    }

    "st_mPointFromText" >> {
      sc.sql("select st_mPointFromText(null)").collect.head(0) must beNull
      dfBlank.select(st_mPointFromText(lit(null))).first must beNull

      val point = "MULTIPOINT((0 0), (1 1))"
      val r = sc.sql(
        s"""
          |select st_mPointFromText('$point')
        """.stripMargin
      )
      val expected = WKTUtils.read("MULTIPOINT((0 0), (1 1))")
      r.collect().head.getAs[MultiPoint](0) mustEqual expected
      dfBlank.select(st_mPointFromText(point)).first mustEqual expected
    }

    "st_mPolyFromText" >> {
      sc.sql("select st_mPolyFromText(null)").collect.head(0) must beNull
      dfBlank.select(st_mPolyFromText(lit(null))).first must beNull

      val poly = "MULTIPOLYGON((( -1 -1, 0 1, 1 -1, -1 -1 )),((-4 4, 4 4, 4 -4, -4 -4, -4 4), (2 2, -2 2, -2 -2, 2 -2, 2 2)))"
      val r = sc.sql(
        s"""
          |select st_mPolyFromText('$poly')
        """.stripMargin
      )

      val expected = WKTUtils.read(poly)
      r.collect().head.getAs[MultiPolygon](0) mustEqual expected
      dfBlank.select(st_mPolyFromText(poly)).first mustEqual expected
    }

    "st_point" >> {
      sc.sql("select st_point(null, null)").collect.head(0) must beNull
      dfBlank.select(st_point(lit(null), lit(null))).first must beNull

      val r = sc.sql(
        """
          |select st_point(0, 0)
        """.stripMargin
      )
      val expected = WKTUtils.read("POINT(0 0)")
      r.collect().head.getAs[Point](0) mustEqual expected
      dfBlank.select(st_point(0, 0)).first mustEqual expected
    }

    "st_pointFromText" >> {
      sc.sql("select st_pointFromText(null)").collect.head(0) must beNull
      dfBlank.select(st_pointFromText(lit(null))).first must beNull

      val point = "Point(0 0)"
      val r = sc.sql(
        s"""
          |select st_pointFromText('$point')
        """.stripMargin
      )
      val expected = WKTUtils.read(point)
      r.collect().head.getAs[Point](0) mustEqual expected
      dfBlank.select(st_pointFromText(point)).first mustEqual expected
    }

    "st_pointFromWKB" >> {
      sc.sql("select st_pointFromWKB(null)").collect.head(0) must beNull
      dfBlank.select(st_pointFromWKB(lit(null))).first must beNull

      val pointArr = Array[Byte](0, 0, 0, 0, 1,
        0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0)
      val r = sc.sql(
        s"""
           |select st_pointFromWKB(st_byteArray('${new String(pointArr)}'))
        """.stripMargin
      )
      val expected = WKTUtils.read("POINT(0 0)")
      r.collect().head.getAs[Point](0) mustEqual expected
      dfBlank.select(st_pointFromWKB(pointArr)).first mustEqual expected
    }

    "st_polygon" >> {
      sc.sql("select st_polygon(null)").collect.head(0) must beNull
      dfBlank.select(st_polygon(lit(null))).first must beNull

      val line = "LINESTRING(0 0, 2 2, 5 2, 3 0, 0 0)"
      val r = sc.sql(
        s"""
           |select st_polygon(st_castToLineString(
           |    st_geomFromWKT('$line')))
        """.stripMargin
      )
      val expected = WKTUtils.read("POLYGON((0 0, 2 2, 5 2, 3 0, 0 0))")
      r.collect().head.getAs[Polygon](0) mustEqual expected
      dfBlank.select(st_polygon(st_castToLineString(st_geomFromWKT(line)))).first mustEqual expected

      val lineInst = WKTUtils.read(line).asInstanceOf[LineString]
      dfBlank.select(st_makePolygon(lineInst)).first mustEqual expected
    }

    "st_polygonFromText" >> {
      sc.sql("select st_polygonFromText(null)").collect.head(0) must beNull
      dfBlank.select(st_polygonFromText(lit(null))).first must beNull

      val poly = "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"
      val r = sc.sql(
        s"""
          |select st_polygonFromText('$poly')
        """.stripMargin
      )

      val expected =  WKTUtils.read("POLYGON((0.0 0.0, 2.0 0.0, 2.0 2.0, 0.0 2.0, 0.0 0.0))")
      r.collect().head.getAs[Polygon](0) mustEqual expected
      dfBlank.select(st_polygonFromText(poly)).first mustEqual expected
    }

    "geometry literals" >> {
      val fact = new GeometryFactory()
      val c1 = new Coordinate(1, 2)
      val c2 = new Coordinate(3, 4)
      val c3 = new Coordinate(5, 6)
      val point = fact.createPoint(c1)
      val line = fact.createLineString(Array(c1, c2))
      val poly = fact.createPolygon(Array(c1, c2, c3, c1))
      val mpoint = fact.createMultiPoint(Array(point, point, point))
      val mline = fact.createMultiLineString(Array(line, line, line))
      val mpoly = fact.createMultiPolygon(Array(poly, poly, poly))
      val coll = fact.createGeometryCollection(Array(point, line, poly, mpoint, mline, mpoly))

      dfBlank.select(pointLit(point)).first mustEqual point
      dfBlank.select(lineLit(line)).first mustEqual line
      dfBlank.select(polygonLit(poly)).first mustEqual poly
      dfBlank.select(mPointLit(mpoint)).first mustEqual mpoint
      dfBlank.select(mLineLit(mline)).first mustEqual mline
      dfBlank.select(mPolygonLit(mpoly)).first mustEqual mpoly
      dfBlank.select(geomCollLit(coll)).first mustEqual coll
      dfBlank.select(geomLit(coll)).first mustEqual coll
    }

    // after
    step {
      spark.stop()
    }
  }
}
