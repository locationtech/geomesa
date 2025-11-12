/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import org.apache.spark.sql.functions._
import org.locationtech.geomesa.spark.jts._
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.locationtech.jts.geom._

class GeometricUdfTest extends TestWithSpark  {

  "sql geometry accessors" should {

    "st_boundary" in {
      sc.sql("select st_boundary(null)").collect.head(0) must beNull
      dfBlank().select(st_boundary(lit(null))).first must beNull

      val line = "LINESTRING(1 1, 0 0, -1 1)"
      val result = sc.sql(
        s"""
          |select st_boundary(st_geomFromWKT('$line'))
        """.stripMargin
      )
      val expected = WKTUtils.read("MULTIPOINT(1 1, -1 1)")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank().select(st_boundary(st_geomFromWKT(line))).first mustEqual expected

    }

    "st_coordDim" in {
      sc.sql("select st_coordDim(null)").collect.head(0) must beNull
      dfBlank().select(st_coordDim(lit(null))).first must beNull

      val result = sc.sql(
        """
          |select st_coordDim(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      result.collect().head.getAs[Int](0) mustEqual 2

      dfBlank().select(st_coordDim(st_geomFromWKT("POINT(0 0)"))).first mustEqual 2
    }

    "st_dimension with null" in {
      sc.sql("select st_dimension(null)").collect.head(0) must beNull
      dfBlank().select(st_dimension(lit(null))).first must beNull
    }

    "st_dimension with point" in {
      val result = sc.sql(
        """
          |select st_dimension(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      result.collect().head.getAs[Int](0) mustEqual 0
      dfBlank().select(st_dimension(st_geomFromWKT("POINT(0 0)"))).first mustEqual 0
    }

    "st_dimension with linestring" in {
      val line = "LINESTRING(1 1, 0 0, -1 1)"
      val result = sc.sql(
        s"""
          |select st_dimension(st_geomFromWKT('$line'))
        """.stripMargin
      )
      result.collect().head.getAs[Int](0) mustEqual 1

      dfBlank().select(st_dimension(st_geomFromWKT(line))).first mustEqual 1
    }

    "st_dimension with polygon" in {
      val poly = "POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))"
      val result = sc.sql(
        s"""
          |select st_dimension(st_geomFromWKT('$poly'))
        """.stripMargin
      )
      result.collect().head.getAs[Int](0) mustEqual 2
      dfBlank().select(st_dimension(st_geomFromWKT(poly))).first mustEqual 2
    }

    "st_dimension with geometrycollection" in {
      val geom = "GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))"
      val result = sc.sql(
        s"""
           |select st_dimension(st_geomFromWKT('$geom'))
        """.stripMargin
      )
      result.collect().head.getAs[Int](0) mustEqual 1
      dfBlank().select(st_dimension(st_geomFromWKT(geom))).first mustEqual 1
    }

    "st_envelope with null" in {
      sc.sql("select st_envelope(null)").collect.head(0) must beNull
      dfBlank().select(st_envelope(lit(null))).first must beNull
    }

    "st_envelope with point" in {
      val result = sc.sql(
        """
          |select st_envelope(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      val expected = WKTUtils.read("POINT(0 0)")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank().select(st_envelope(st_geomFromWKT("POINT(0 0)"))).first mustEqual expected
    }

    "st_envelope with linestring" in {
      val result = sc.sql(
        """
          |select st_envelope(st_geomFromWKT('LINESTRING(0 0, 1 3)'))
        """.stripMargin
      )
      val expected = WKTUtils.read("POLYGON((0 0,0 3,1 3,1 0,0 0))")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank().select(st_envelope(st_geomFromWKT("LINESTRING(0 0, 1 3)"))).first mustEqual expected
    }

    "st_envelope with polygon" in {
      val poly = "POLYGON((0 0, 0 1, 1.0000001 1, 1.0000001 0, 0 0))"
      val result = sc.sql(
        s"""
          |select st_envelope(st_geomFromWKT('$poly'))
        """.stripMargin
      )
      val expected = WKTUtils.read("POLYGON((0 0, 0 1, 1.0000001 1, 1.0000001 0, 0 0))")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank().select(st_envelope(st_geomFromWKT(poly))).first mustEqual expected
    }

    "st_exteriorRing with null" in {
      sc.sql("select st_exteriorRing(null)").collect.head(0) must beNull
      dfBlank().select(st_exteriorRing(lit(null))).first must beNull
    }

    "st_exteriorRing with point" in {
      val result = sc.sql(
        """
          |select st_exteriorRing(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      result.collect().head.getAs[Geometry](0) must beNull
      dfBlank().select(st_exteriorRing(st_geomFromWKT("POINT(0 0)"))).first must beNull
    }

    "st_exteriorRing with polygon without an interior ring" in {
      val poly = "POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))"
      val result = sc.sql(
        s"""
           |select st_exteriorRing(st_geomFromWKT('$poly'))
        """.stripMargin
      )
      val expected = WKTUtils.read("LINESTRING(30 10, 40 40, 20 40, 10 20, 30 10)")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank().select(st_exteriorRing(st_geomFromWKT(poly))).first mustEqual expected
    }

    "st_exteriorRing with polygon with an interior ring" in {
      val poly = "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))"
      val result = sc.sql(
        s"""
          |select st_exteriorRing(st_geomFromWKT('$poly'))
        """.stripMargin
      )
      val expected = WKTUtils.read("LINESTRING(35 10, 45 45, 15 40, 10 20, 35 10)")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank().select(st_exteriorRing(st_geomFromWKT(poly))).first mustEqual expected
    }

    "st_geometryN with null" in {
      sc.sql("select st_geometryN(null, null)").collect.head(0) must beNull
      dfBlank().select(st_geometryN(lit(null), lit(null))).first must beNull
    }

    "st_geometryN with point" in {
      val result = sc.sql(
        """
          |select st_geometryN(st_geomFromWKT('POINT(0 0)'), 1)
        """.stripMargin
      )
      val expected = WKTUtils.read("POINT(0 0)")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank().select(st_geometryN(st_geomFromWKT("POINT(0 0)"), lit(1))).first mustEqual expected
    }

    "st_geometryN with multilinestring" in {
      val geom = "MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))"
      val result = sc.sql(
        s"""
           |select st_geometryN(st_geomFromWKT('$geom'), 1)
        """.stripMargin
      )
      val expected = WKTUtils.read("LINESTRING(10 10, 20 20, 10 40)")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank().select(st_geometryN(st_geomFromWKT(geom), lit(1))).first mustEqual expected
    }

    "st_geometryN with geometrycollection" in {
      val geom = "GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))"
      val result = sc.sql(
        s"""
          |select st_geometryN(st_geomFromWKT('$geom'), 1)
        """.stripMargin
      )
      val expected = WKTUtils.read("LINESTRING(1 1,0 0)")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank().select(st_geometryN(st_geomFromWKT(geom), lit(1))).first mustEqual expected
    }

    "st_geometryType with null" in {
      sc.sql("select st_geometryType(null)").collect.head(0) must beNull
      dfBlank().select(st_geomFromWKT(lit(null))).first must beNull
    }

    "st_geometryType with point" in {
      val result = sc.sql(
        """
          |select st_geometryType(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      result.collect().head.getAs[String](0) mustEqual "Point"
      dfBlank().select(st_geometryType(st_geomFromWKT("POINT(0 0)"))).first mustEqual "Point"
    }

    "st_geometryType with linestring" in {
      val result = sc.sql(
        """
          |select st_geometryType(st_geomFromWKT('LINESTRING(0 0, 1 3)'))
        """.stripMargin
      )
      result.collect().head.getAs[String](0) mustEqual "LineString"
      dfBlank().select(st_geometryType(st_geomFromWKT("LINESTRING(0 0, 1 3)"))).first mustEqual "LineString"
    }

    "st_geometryType with geometrycollection" in {
      val geom = "GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))"
      val result = sc.sql(
        s"""
          |select st_geometryType(st_geomFromWKT('$geom'))
        """.stripMargin
      )
      result.collect().head.getAs[String](0) mustEqual "GeometryCollection"
      dfBlank().select(st_geometryType(st_geomFromWKT(geom))).first mustEqual "GeometryCollection"
    }

    "st_interiorRingN with null" in {
      sc.sql("select st_interiorRingN(null, null)").collect.head(0) must beNull
      dfBlank().select(st_interiorRingN(lit(null), lit(null))).first must beNull
    }

    "st_interiorRingN with point" in {
      val result = sc.sql(
        """
          |select st_interiorRingN(st_geomFromWKT('POINT(0 0)'), 1)
        """.stripMargin
      )
      result.collect().head.getAs[Geometry](0) must beNull
      dfBlank().select(st_interiorRingN(st_geomFromWKT("POINT(0 0)"), lit(1))).first must beNull
    }

    "st_interiorRingN with polygon with a valid int" in {
      val poly = "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30))"
      val result = sc.sql(
        s"""
          |select st_interiorRingN(st_geomFromWKT('$poly'), 1)
        """.stripMargin
      )
      val expected = WKTUtils.read("LINESTRING(20 30, 35 35, 30 20, 20 30)")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank().select(st_interiorRingN(st_geomFromWKT(poly), lit(1))).first mustEqual expected
    }

    "st_interiorRingN with polygon with an invalid int" in {
      val poly = "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))"
      val result = sc.sql(
        s"""
          |select st_interiorRingN(st_geomFromWKT('$poly'), 5)
        """.stripMargin
      )
      result.collect().head.getAs[Geometry](0) must beNull
      dfBlank().select(st_interiorRingN(st_geomFromWKT(poly), lit(5))).first must beNull
    }

    "st_isClosed with null" in {
      sc.sql("select st_isClosed(null)").collect.head(0) must beNull
      dfBlank().select(st_isClosed(lit(null))).first must beNull
    }

    "st_isClosed with open linestring" in {
      val line = "LINESTRING(0 0, 1 1)"
      val result = sc.sql(
        s"""
          |select st_isClosed(st_geomFromWKT('$line'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beFalse
      dfBlank().select(st_isClosed(st_geomFromWKT(line))).first.booleanValue() must beFalse
    }

    "st_isClosed with closed linestring" in {
      val line = "LINESTRING(0 0, 0 1, 1 1, 0 0)"
      val result = sc.sql(
        s"""
          |select st_isClosed(st_geomFromWKT('$line'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beTrue
      dfBlank().select(st_isClosed(st_geomFromWKT(line))).first.booleanValue must beTrue
    }

    "st_isClosed with open multilinestring" in {
      val line = "MULTILINESTRING((0 0, 0 1, 1 1, 0 0),(0 0, 1 1))"
      val result = sc.sql(
        s"""
           |select st_isClosed(st_geomFromWKT('$line'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beFalse
      dfBlank().select(st_isClosed(st_geomFromWKT(line))).first.booleanValue must beFalse
    }

    "st_isClosed with closed multilinestring" in {
      val line = "MULTILINESTRING((0 0, 0 1, 1 1, 0 0),(0 0, 1 1, 0 0))"
      val result = sc.sql(
        s"""
           |select st_isClosed(st_geomFromWKT('$line'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beTrue
      dfBlank().select(st_isClosed(st_geomFromWKT(line))).first.booleanValue() must beTrue
    }

    "st_isCollection with null" in {
      sc.sql("select st_isCollection(null)").collect.head(0) must beNull
      dfBlank().select(st_isCollection(lit(null))).first must beNull
    }

    "st_isCollection with point" in {
      val result = sc.sql(
        """
          |select st_isCollection(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beFalse
      dfBlank().select(st_isCollection(st_geomFromWKT("POINT(0 0)"))).first.booleanValue() must beFalse
    }

    "st_isCollection with multipoint" in {
      val point = "MULTIPOINT((0 0), (42 42))"
      val result = sc.sql(
        s"""
           |select st_isCollection(st_geomFromWKT('$point'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beTrue
      dfBlank().select(st_isCollection(st_geomFromWKT(point))).first.booleanValue() must beTrue
    }

    "st_isCollection with geometrycollection" in {
      val geom = "GEOMETRYCOLLECTION(POINT(0 0))"
      val result = sc.sql(
        s"""
          |select st_isCollection(st_geomFromWKT('$geom'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beTrue
      dfBlank().select(st_isCollection(st_geomFromWKT(geom))).first.booleanValue() must beTrue
    }

    "st_isEmpty with null" in {
      sc.sql("select st_isEmpty(null)").collect.head(0) must beNull
    }

    "st_isEmpty with empty geometrycollection" in {
      val result = sc.sql(
        """
          |select st_isEmpty(st_geomFromWKT('GEOMETRYCOLLECTION EMPTY'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beTrue
      dfBlank().select(st_isEmpty(st_geomFromWKT("GEOMETRYCOLLECTION EMPTY"))).first.booleanValue() must beTrue
    }

    "st_isEmpty with non-empty point" in {
      val result = sc.sql(
        """
          |select st_isEmpty(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beFalse
      dfBlank().select(st_isEmpty(st_geomFromWKT("POINT(0 0)"))).first.booleanValue() must beFalse
    }

    "st_isRing with null" in {
      sc.sql("select st_isRing(null)").collect.head(0) must beNull
      dfBlank().select(st_isRing(lit(null))).first must beNull
    }

    "st_isRing with closed and simple linestring" in {
      val line = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"
      val result = sc.sql(
        s"""
           |select st_isRing(st_geomFromWKT('$line'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beTrue
      dfBlank().select(st_isRing(st_geomFromWKT(line))).first.booleanValue() must beTrue
    }

    "st_isRing with closed and non-simple linestring" in {
      val line = "LINESTRING(0 0, 0 1, 1 0, 1 1, 0 0)"
      val result = sc.sql(
        s"""
           |select st_isRing(st_geomFromWKT('$line'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beFalse
      dfBlank().select(st_isRing(st_geomFromWKT(line))).first.booleanValue() must beFalse
    }

    "st_isSimple with null" in {
      sc.sql("select st_isSimple(null)").collect.head(0) must beNull
      dfBlank().select(st_isSimple(lit(null))).first must beNull
    }

    "st_isSimple with simple point" in {
      val result = sc.sql(
        """
          |select st_isSimple(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beTrue
      dfBlank().select(st_isSimple(st_geomFromWKT("POINT(0 0)"))).first.booleanValue() must beTrue
    }

    "st_isSimple with simple linestring" in {
      val line = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"
      val result = sc.sql(
        s"""
           |select st_isSimple(st_geomFromWKT('$line'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beTrue
      dfBlank().select(st_isSimple(st_geomFromWKT(line))).first.booleanValue() must beTrue
    }

    "st_isSimple with non-simple linestring" in {
      val line = "LINESTRING(1 1,2 2,2 3.5,1 3,1 2,2 1)"
      val result = sc.sql(
        s"""
           |select st_isSimple(st_geomFromWKT('$line'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beFalse
      dfBlank().select(st_isSimple(st_geomFromWKT(line))).first.booleanValue() must beFalse
    }

    "st_isSimple with non-simple polygon" in {
      val poly = "POLYGON((1 2, 3 4, 5 6, 1 2))"
      val result = sc.sql(
        s"""
          |select st_isSimple(st_geomFromWKT('$poly'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beFalse
      dfBlank().select(st_isSimple(st_geomFromWKT(poly))).first.booleanValue() must beFalse
    }

    "st_isValid with null" in {
      sc.sql("select st_isValid(null)").collect.head(0) must beNull
      dfBlank().select(st_isValid(lit(null))).first must beNull
    }

    "st_isValid with valid linestring" in {
      val line = "LINESTRING(0 0, 1 1)"
      val result = sc.sql(
        s"""
           |select st_isValid(st_geomFromWKT('$line'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beTrue
      dfBlank().select(st_isValid(st_geomFromWKT(line))).first.booleanValue() must beTrue
    }

    "st_isValid with invalid polygon" in {
      val poly = "POLYGON((0 0, 1 1, 1 2, 1 1, 0 0))"
      val result = sc.sql(
        s"""
          |select st_isValid(st_geomFromWKT('$poly'))
        """.stripMargin
      )
      result.collect().head.getAs[Boolean](0) must beFalse
      dfBlank().select(st_isValid(st_geomFromWKT(poly))).first.booleanValue() must beFalse
    }

    "st_numGeometries with null" in {
      sc.sql("select st_numGeometries(null)").collect.head(0) must beNull
      dfBlank().select(st_numGeometries(lit(null))).first must beNull
    }

    "st_numGeometries with point" in {
      val result = sc.sql(
        """
          |select st_numGeometries(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      result.collect().head.getAs[Int](0) mustEqual 1
      dfBlank().select(st_numGeometries(st_geomFromWKT("POINT(0 0)"))).first mustEqual 1
    }

    "st_numGeometries with linestring" in {
      val line = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"
      val result = sc.sql(
        s"""
           |select st_numGeometries(st_geomFromWKT('$line'))
        """.stripMargin
      )
      result.collect().head.getAs[Int](0) mustEqual 1
      dfBlank().select(st_numGeometries(st_geomFromWKT(line))).first mustEqual 1
    }

    "st_numGeometries with geometrycollection" in {
      val geom = "GEOMETRYCOLLECTION(MULTIPOINT(-2 3,-2 2), LINESTRING(5 5,10 10), POLYGON((-7 4.2,-7.1 5,-7.1 4.3,-7 4.2)))"
      val result = sc.sql(
        s"""
           |select st_numGeometries(st_geomFromWKT('$geom'))
        """.stripMargin
      )
      result.collect().head.getAs[Int](0) mustEqual 3
      dfBlank().select(st_numGeometries(st_geomFromWKT(geom))).first mustEqual 3
    }

    "st_numPoints with null" in {
      sc.sql("select st_numPoints(null)").collect.head(0) must beNull
      dfBlank().select(st_numPoints(lit(null))).first must beNull
    }

    "st_numPoints with point" in {
      val result = sc.sql(
        """
          |select st_numPoints(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      result.collect().head.getAs[Int](0) mustEqual 1
    }

    "st_numPoints with multipoint" in {
      val point = "MULTIPOINT(-2 3,-2 2)"
      val result = sc.sql(
        s"""
           |select st_numPoints(st_geomFromWKT('$point'))
        """.stripMargin
      )
      result.collect().head.getAs[Int](0) mustEqual 2
      dfBlank().select(st_numPoints(st_geomFromWKT(point))).first mustEqual 2
    }

    "st_numPoints with linestring" in {
      val geom = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"
      val result = sc.sql(
        s"""
           |select st_numPoints(st_geomFromWKT('$geom'))
        """.stripMargin
      )
      result.collect().head.getAs[Int](0) mustEqual 5
      dfBlank().select(st_numPoints(st_geomFromWKT(geom))).first mustEqual 5
    }

    "st_pointN with null" in {
      sc.sql("select st_pointN(null, null)").collect.head(0) must beNull
      dfBlank().select(st_pointN(lit(null), lit(null))).first must beNull
    }

    "st_pointN with first point" in {
      val line = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 2)"
      val result = sc.sql(
        s"""
           |select st_pointN(st_geomFromWKT('$line'), 1)
        """.stripMargin
      )
      val expected = WKTUtils.read("POINT(0 0)")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank().select(st_pointN(st_geomFromWKT(line), lit(1))).first mustEqual expected
    }

    "st_pointN with last point" in {
      val line = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 2)"
      val result = sc.sql(
        s"""
          |select st_pointN(st_geomFromWKT('$line'), 5)
        """.stripMargin
      )
      val expected = WKTUtils.read("POINT(0 2)")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank().select(st_pointN(st_geomFromWKT(line), lit(5))).first mustEqual expected
    }

    "st_pointN with first point using a negative index" in {
      val line = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 2)"
      val result = sc.sql(
        s"""
          |select st_pointN(st_geomFromWKT('$line'), -5)
        """.stripMargin
      )
      val expected = WKTUtils.read("POINT(0 0)")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank().select(st_pointN(st_geomFromWKT(line), lit(-5))).first mustEqual expected
    }

    "st_pointN with last point using a negative index" in {
      val line = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 2)"
      val result = sc.sql(
        s"""
           |select st_pointN(st_geomFromWKT('$line'), -1)
        """.stripMargin
      )
      val expected = WKTUtils.read("POINT(0 2)")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank().select(st_pointN(st_geomFromWKT(line), lit(-1))).first mustEqual expected
    }

    "st_x with null" in {
      sc.sql("select st_x(null)").collect.head(0) must beNull
      dfBlank().select(st_x(lit(null))).first must beNull
    }

    "st_x with point" in {
      val result = sc.sql(
        """
          |select st_x(st_geomFromWKT('POINT(0 1)'))
        """.stripMargin
      )
      result.collect().head.getAs[java.lang.Float](0) mustEqual 0f
      dfBlank().select(st_x(st_geomFromWKT("POINT(0 1)"))).first mustEqual 0f

    }

    "st_x with non-point" in {
      val result = sc.sql(
        """
          |select st_x(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'))
        """.stripMargin
      )
      result.collect().head.getAs[java.lang.Float](0) must beNull
      dfBlank().select(st_x(st_geomFromWKT("LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"))).first must beNull
    }

    "st_y with null" in {
      sc.sql("select st_y(null)").collect.head(0) must beNull
      dfBlank().select(st_y(lit(null))).first must beNull
    }

    "st_y with point" in {
      val result = sc.sql(
        """
          |select st_y(st_geomFromWKT('POINT(0 1)'))
        """.stripMargin
      )
      result.collect().head.getAs[java.lang.Float](0) mustEqual 1f
      dfBlank().select(st_y(st_geomFromWKT("POINT(0 1)"))).first mustEqual 1f

    }

    "st_y with non-point" in {
      val result = sc.sql(
        """
          |select st_y(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'))
        """.stripMargin
      )
      result.collect().head.getAs[java.lang.Float](0) must beNull
      dfBlank().select(st_y(st_geomFromWKT("LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"))).first must beNull
    }
  }

  "sql geometry constructors" should {

    "st_bufferPoint should handle nulls" in {
      sc.sql("select st_bufferPoint(null, null)").collect.head(0) must beNull
      dfBlank().select(st_bufferPoint(lit(null), lit(null))).first must beNull
    }

    "st_bufferPoint should return a point buffered in meters" in {
      val buf = sc.sql("select st_bufferPoint(st_makePoint(0, 0), 10)").collect().head.get(0)
      val dfBuf = dfBlank().select(st_bufferPoint(st_makePoint(lit(0), lit(0)), lit(10))).first

      val bufferedPoly = WKTUtils.read(
        """
          |POLYGON ((0.0000899320367762 0, 0.0000897545764446 0.0000056468793115, 0.0000892228958048 0.0000112714729702, 0.0000883390931573 0.0000168515832745, 0.0000871066564674 0.0000223651880784, 0.0000855304495997 0.0000277905277026, 0.0000836166931225 0.0000331061908102, 0.0000813729397584 0.0000382911989076, 0.0000788080445769 0.0000433250891364, 0.0000759321300474 0.0000481879950317, 0.0000727565460907 0.0000528607249257, 0.0000692938252858 0.0000573248376881, 0.0000655576334099 0.0000615627155054, 0.0000615627155054 0.0000655576334099, 0.0000573248376881 0.0000692938252858, 0.0000528607249257 0.0000727565460907, 0.0000481879950317 0.0000759321300474, 0.0000433250891364 0.0000788080445769, 0.0000382911989076 0.0000813729397584, 0.0000331061908102 0.0000836166931225, 0.0000277905277026 0.0000855304495997, 0.0000223651880784 0.0000871066564674, 0.0000168515832745 0.0000883390931573, 0.0000112714729702 0.0000892228958048, 0.0000056468793115 0.0000897545764446, -0 0.0000899320367762, -0.0000056468793115 0.0000897545764446, -0.0000112714729702 0.0000892228958048, -0.0000168515832745 0.0000883390931573, -0.0000223651880784 0.0000871066564674, -0.0000277905277026 0.0000855304495997, -0.0000331061908102 0.0000836166931225, -0.0000382911989076 0.0000813729397584, -0.0000433250891364 0.0000788080445769, -0.0000481879950317 0.0000759321300474, -0.0000528607249257 0.0000727565460907, -0.0000573248376881 0.0000692938252858, -0.0000615627155054 0.0000655576334099, -0.0000655576334099 0.0000615627155054, -0.0000692938252858 0.0000573248376881, -0.0000727565460907 0.0000528607249257, -0.0000759321300474 0.0000481879950317, -0.0000788080445769 0.0000433250891364, -0.0000813729397584 0.0000382911989076, -0.0000836166931225 0.0000331061908102, -0.0000855304495997 0.0000277905277026, -0.0000871066564674 0.0000223651880784, -0.0000883390931573 0.0000168515832745, -0.0000892228958048 0.0000112714729702, -0.0000897545764446 0.0000056468793115, -0.0000899320367762 -0, -0.0000897545764446 -0.0000056468793115, -0.0000892228958048 -0.0000112714729702, -0.0000883390931573 -0.0000168515832745, -0.0000871066564674 -0.0000223651880784, -0.0000855304495997 -0.0000277905277026, -0.0000836166931225 -0.0000331061908102, -0.0000813729397584 -0.0000382911989076, -0.0000788080445769 -0.0000433250891364, -0.0000759321300474 -0.0000481879950317, -0.0000727565460907 -0.0000528607249257, -0.0000692938252858 -0.0000573248376881, -0.0000655576334099 -0.0000615627155054, -0.0000615627155054 -0.0000655576334099, -0.0000573248376881 -0.0000692938252858, -0.0000528607249257 -0.0000727565460907, -0.0000481879950317 -0.0000759321300474, -0.0000433250891364 -0.0000788080445769, -0.0000382911989076 -0.0000813729397584, -0.0000331061908102 -0.0000836166931225, -0.0000277905277026 -0.0000855304495997, -0.0000223651880784 -0.0000871066564674, -0.0000168515832745 -0.0000883390931573, -0.0000112714729702 -0.0000892228958048, -0.0000056468793115 -0.0000897545764446, -0 -0.0000899320367762, 0.0000056468793115 -0.0000897545764446, 0.0000112714729702 -0.0000892228958048, 0.0000168515832745 -0.0000883390931573, 0.0000223651880784 -0.0000871066564674, 0.0000277905277026 -0.0000855304495997, 0.0000331061908102 -0.0000836166931225, 0.0000382911989076 -0.0000813729397584, 0.0000433250891364 -0.0000788080445769, 0.0000481879950317 -0.0000759321300474, 0.0000528607249257 -0.0000727565460907, 0.0000573248376881 -0.0000692938252858, 0.0000615627155054 -0.0000655576334099, 0.0000655576334099 -0.0000615627155054, 0.0000692938252858 -0.0000573248376881, 0.0000727565460907 -0.0000528607249257, 0.0000759321300474 -0.0000481879950317, 0.0000788080445769 -0.0000433250891364, 0.0000813729397584 -0.0000382911989076, 0.0000836166931225 -0.0000331061908102, 0.0000855304495997 -0.0000277905277026, 0.0000871066564674 -0.0000223651880784, 0.0000883390931573 -0.0000168515832745, 0.0000892228958048 -0.0000112714729702, 0.0000897545764446 -0.0000056468793115, 0.0000899320367762 0))
      """.stripMargin)

      buf.asInstanceOf[Polygon].equalsExact(bufferedPoly, 0.000001) must beTrue
      dfBuf.asInstanceOf[Polygon].equalsExact(bufferedPoly, 0.000001) must beTrue
    }

    "st_bufferPoint should handle antimeridian" in {
      val buf = sc.sql("select st_bufferPoint(st_makePoint(-180, 50), 100000)").first.getAs[Geometry](0)
      val dfBuf = dfBlank().select(st_bufferPoint(st_makePoint(lit(-180), lit(50)), lit(100000))).first
      // check points on both sides of antimeridian
      buf.contains(WKTUtils.read("POINT(-179.9 50)")) mustEqual true
      buf.contains(WKTUtils.read("POINT(179.9 50)")) mustEqual true

      dfBuf.contains(WKTUtils.read("POINT(-179.9 50)")) mustEqual true
      dfBuf.contains(WKTUtils.read("POINT(179.9 50)")) mustEqual true
    }

    "st_antimeridianSafeGeom should handle nulls" in {
      sc.sql("select st_antimeridianSafeGeom(null)").collect.head(0) must beNull
      dfBlank().select(st_antimeridianSafeGeom(lit(null))).first must beNull
    }

    "st_antimeridianSafeGeom should split a geom that spans the antimeridian" in {

      val wkt = "POLYGON((-190 50, -190 60, -170 60, -170 50, -190 50))"
      val geom = s"st_geomFromWKT('$wkt')"
      val decomposed = sc.sql(s"select st_antimeridianSafeGeom($geom)").first.getAs[Geometry](0)
      val dfDecomposed = dfBlank().select(st_antimeridianSafeGeom(st_geomFromWKT(lit(wkt)))).first

      val expected = WKTUtils.read(
        "MULTIPOLYGON (((-180 50, -180 60, -170 60, -170 50, -180 50)), ((180 60, 180 50, 170 50, 170 60, 180 60)))")
      decomposed mustEqual expected
      dfDecomposed mustEqual expected
    }

    "st_makeValid should handle nulls" in {
      sc.sql("select st_makeValid(null)").collect.head(0) must beNull
      dfBlank().select(st_makeValid(lit(null))).first must beNull
    }

    "st_makeValid should return valid geometry" in {

      val wkt = "POLYGON((0 0, 0 1, 2 1, 2 2, 1 2, 1 0, 0 0))"
      val geom = s"st_geomFromWKT('$wkt')"
      val decomposed = sc.sql(s"select st_isValid($geom), st_isValid(st_makeValid($geom))").first
      val dfDecomposed = dfBlank().select(st_isValid(st_geomFromWKT(lit(wkt))), st_isValid(st_makeValid(st_geomFromWKT(lit(wkt))))).first

      val expected = (false, true)

      (decomposed.getBoolean(0), decomposed.getBoolean(1)) mustEqual expected
      dfDecomposed mustEqual expected

    }

    "st_makeValid should repair geometry" in {

      // these polygons are taken from the PostGIS docs: https://postgis.net/docs/ST_MakeValid.html
      val input_wkt = Seq("POLYGON ((0 0, 0 1, 2 1, 2 2, 1 2, 1 0, 0 0))",
        "MULTIPOLYGON (((186 194, 187 194, 188 195, 189 195, 190 195, 191 195, 192 195, 193 194, 194 194, 194 193, 195 192, 195 191, 195 190, 195 189, 195 188, 194 187, 194 186, 14 6, 13 6, 12 5, 11 5, 10 5, 9 5, 8 5, 7 6, 6 6, 6 7, 5 8, 5 9, 5 10, 5 11, 5 12, 6 13, 6 14, 186 194)), ((150 90, 149 80, 146 71, 142 62, 135 55, 128 48, 119 44, 110 41, 100 40, 90 41, 81 44, 72 48, 65 55, 58 62, 54 71, 51 80, 50 90, 51 100, 54 109, 58 118, 65 125, 72 132, 81 136, 90 139, 100 140, 110 139, 119 136, 128 132, 135 125, 142 118, 146 109, 149 100, 150 90)))",
        "MULTIPOLYGON (((91 50, 79 22, 51 10, 23 22, 11 50, 23 78, 51 90, 79 78, 91 50)), ((91 100, 79 72, 51 60, 23 72, 11 100, 23 128, 51 140, 79 128, 91 100)), ((91 150, 79 122, 51 110, 23 122, 11 150, 23 178, 51 190, 79 178, 91 150)), ((141 50, 129 22, 101 10, 73 22, 61 50, 73 78, 101 90, 129 78, 141 50)), ((141 100, 129 72, 101 60, 73 72, 61 100, 73 128, 101 140, 129 128, 141 100)), ((141 150, 129 122, 101 110, 73 122, 61 150, 73 178, 101 190, 129 178, 141 150)))",
        "LINESTRING (0 0, 0 0)")
      val expected_wkt = Seq("MULTIPOLYGON (((0 0, 0 1, 1 1, 1 0, 0 0)), ((1 1, 1 2, 2 2, 2 1, 1 1)))",
        "MULTIPOLYGON (((149 80, 146 71, 142 62, 135 55, 128 48, 119 44, 110 41, 100 40, 90 41, 81 44, 72 48, 65 55, 64 56, 14 6, 13 6, 12 5, 11 5, 10 5, 9 5, 8 5, 7 6, 6 6, 6 7, 5 8, 5 9, 5 10, 5 11, 5 12, 6 13, 6 14, 56.76923076923077 64.76923076923077, 54 71, 51 80, 50 90, 51 100, 54 109, 58 118, 65 125, 72 132, 81 136, 90 139, 100 140, 110 139, 119 136, 125.23076923076923 133.23076923076923, 186 194, 187 194, 188 195, 189 195, 190 195, 191 195, 192 195, 193 194, 194 194, 194 193, 195 192, 195 191, 195 190, 195 189, 195 188, 194 187, 194 186, 134 126, 135 125, 142 118, 146 109, 149 100, 150 90, 149 80)))",
        "MULTIPOLYGON (((51 10, 23 22, 11 50, 21.714285714285715 75, 11 100, 21.714285714285715 125, 11 150, 23 178, 51 190, 76 179.28571428571428, 101 190, 129 178, 141 150, 130.28571428571428 125, 141 100, 130.28571428571428 75, 141 50, 129 22, 101 10, 76 20.714285714285715, 51 10)))",
        "LINESTRING EMPTY")

      input_wkt.zip(expected_wkt).foreach { case (wkt, expected) =>
        val geom = s"st_geomFromWKT('$wkt')"
        val decomposed = sc.sql(s"select st_asText(st_makeValid($geom))").first.getAs[String](0)
        val dfDecomposed = dfBlank().select(st_asText(st_makeValid(st_geomFromWKT(lit(wkt))))).first

        decomposed mustEqual expected
        dfDecomposed mustEqual expected
      }
      true mustEqual true // happy ending for JUnitRunner
    }
  }

  "sql geometry constructors" should {

    "st_asBinary" in {
      sc.sql("select st_asBinary(null)").collect.head(0) must beNull
      dfBlank().select(st_asBinary(lit(null))).first must beNull

      val r = sc.sql(
        """
          |select st_asBinary(st_geomFromWKT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))
        """.stripMargin
      )
      val expected = Array[Byte](0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0,
        64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0
      )

      r.collect().head.getAs[Array[Byte]](0) mustEqual expected

      dfBlank().select(st_asBinary(st_geomFromWKT("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))).first mustEqual expected
    }

    "st_asBinary with 3 dimensions" in {

      val r = sc.sql(
        """
          |select st_asBinary(st_geomFromWKT('POLYGON((0 0 1, 2 0 1, 2 2 1, 0 2 1, 0 0 1))'))
        """.stripMargin
      )
      val expected = Array[Byte](0, -128, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 63, -16, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 63, -16, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 63, -16, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 63, -16, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 63, -16, 0, 0, 0, 0, 0, 0
      )

      r.collect().head.getAs[Array[Byte]](0) mustEqual expected

      dfBlank().select(st_asBinary(st_geomFromWKT("POLYGON((0 0 1, 2 0 1, 2 2 1, 0 2 1, 0 0 1))"))).first mustEqual expected
    }

    "st_asGeoJSON with null" in {
      sc.sql("select st_asGeoJSON(null)").collect.head(0) must beNull
      dfBlank().select(st_asGeoJSON(lit(null))).first must beNull
    }

    "st_asGeoJSON with point" in {
      val r = sc.sql(
        """
          |select st_asGeoJSON(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      val expected = """{"type":"Point","coordinates":[0.0,0.0]}"""
      r.collect().head.getAs[String](0) mustEqual expected
      dfBlank().select(st_asGeoJSON(st_geomFromWKT("POINT(0 0)"))).first mustEqual expected
    }

    "st_asGeoJSON with lineString" in {
      val line = "LINESTRING(0 0, 1 1, 2 2)"
      val r = sc.sql(
        s"""
           |select st_asGeoJSON(st_geomFromWKT('$line'))
        """.stripMargin
      )
      val expected = """{"type":"LineString","coordinates":[[0.0,0.0],[1,1],[2,2]]}"""
      r.collect().head.getAs[String](0) mustEqual expected
      dfBlank().select(st_asGeoJSON(st_geomFromWKT(line))).first mustEqual expected

    }

    "st_asGeoJSON with polygon" in {
      val poly = "POLYGON((0.45 0.75, 1.15 0.75, 1.15 1.45, 0.45 1.45, 0.45 0.75))"
      val r = sc.sql(
        s"""
           |select st_asGeoJSON(st_geomFromWKT('$poly'))
        """.stripMargin
      )
      val expected = """{"type":"Polygon","coordinates":[[[0.45,0.75],[1.15,0.75],[1.15,1.45],[0.45,1.45],[0.45,0.75]]]}"""
      r.collect().head.getAs[String](0) mustEqual expected

      dfBlank().select(st_asGeoJSON(st_geomFromWKT(poly)))
        .first mustEqual expected
    }

    "st_asGeoJSON with multiPoint" in {
      val point = "MULTIPOINT((0 0), (1 1))"
      val r = sc.sql(
        s"""
           |select st_asGeoJSON(st_geomFromWKT('$point'))
        """.stripMargin
      )
      val expected = """{"type":"MultiPoint","coordinates":[[0.0,0.0],[1,1]]}"""
      r.collect().head.getAs[String](0) mustEqual expected

      dfBlank().select(st_asGeoJSON(st_geomFromWKT(point))).first mustEqual expected
    }

    "st_asGeoJSON with multiLineString" in {
      val line = "MULTILINESTRING((0 0, 1 1, 2 2), (-3 -3, -2 -2, -1 -1))"
      val r = sc.sql(
        s"""
           |select st_asGeoJSON(st_geomFromWKT('$line'))
        """.stripMargin
      )
      val expected = """{"type":"MultiLineString","coordinates":[[[0.0,0.0],[1,1],[2,2]],[[-3,-3],[-2,-2],[-1,-1]]]}"""
      r.collect().head.getAs[String](0) mustEqual expected

    }

    "st_asGeoJSON with multiPolygon" in {
      val poly = "MULTIPOLYGON(((0.45 0.75, 1.15 0.75, 1.15 1.45, 0.45 1.45, 0.45 0.75)),((0 0, 1 0, 1 1, 0 1, 0 0)))"
      val r = sc.sql(
        s"""
           |select st_asGeoJSON(st_geomFromWKT('$poly'))
        """.stripMargin
      )
      val expected = """{"type":"MultiPolygon","coordinates":[[[[0.45,0.75],[1.15,0.75],[1.15,1.45],[0.45,1.45],""" +
        """[0.45,0.75]]],[[[0.0,0.0],[1,0.0],[1,1],[0.0,1],[0.0,0.0]]]]}"""

      r.collect().head.getAs[String](0) mustEqual expected
      dfBlank().select(st_asGeoJSON(st_geomFromWKT(poly))).first mustEqual expected
    }

    "st_asGeoJSON with geometryCollection" in {
      val geom = "GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1, 2 2))"
      val r = sc.sql(
        s"""
           |select st_asGeoJSON(st_geomFromWKT('$geom'))
        """.stripMargin
      )
      val expected = """{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[0.0,0.0]},""" +
        """{"type":"LineString","coordinates":[[0.0,0.0],[1,1],[2,2]]}]}"""

      r.collect().head.getAs[String](0) mustEqual expected
      dfBlank().select(st_asGeoJSON(st_geomFromWKT(geom))).first mustEqual expected
    }

    "st_asLatLonText" in {
      sc.sql("select st_asLatLonText(null)").collect.head(0) must beNull
      import org.apache.spark.sql.functions.col
      val gf = new GeometryFactory()
      val df = sc.createDataset(Seq(gf.createPoint(new Coordinate(-76.5, 38.5)))).toDF()
      val r = df.select(st_asLatLonText(col("value")))

      r.collect().head mustEqual """38°30'0.000"N 77°30'0.000"W"""
    }

    "st_asText" in {
      sc.sql("select st_asText(null)").collect.head(0) must beNull
      dfBlank().select(st_asText(lit(null))).first must beNull

      val point = "POINT (-76.5 38.5)"
      val r = sc.sql(
        s"""
           |select st_asText(st_geomFromWKT('$point'))
        """.stripMargin
      )
      val expected = "POINT (-76.5 38.5)"
      r.collect().head.getAs[String](0) mustEqual expected

      dfBlank().select(st_asText(st_geomFromWKT(point))).first mustEqual expected
    }

    "st_geoHash" in {
      sc.sql("select st_geoHash(null, null)").collect.head(0) must beNull
      dfBlank().select(st_geoHash(lit(null), lit(null))).first must beNull


      val point = "POINT (-76.5 38.5)"
      val precision = 25
      val r = sc.sql(
        s"""
           |select st_geoHash(st_geomFromWKT('$point'), $precision)
        """.stripMargin
      )

      val expected = "dqce5"


      r.collect().head.getAs[String](0) mustEqual expected
      dfBlank().select(st_geoHash(st_geomFromWKT(lit(point)), lit(precision))).first mustEqual expected
    }
  }

  "sql geometry constructors" should {

    "st_box2DFromGeoHash" in {
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

    "st_geomFromGeoHash" in {
      sc.sql("select st_geomFromGeoHash(null, null)").collect.head(0) must beNull
      dfBlank().select(st_geomFromGeoHash(lit(null), lit(null))).first must beNull

      val geohash = "ezs42"
      val precision = 25
      val r = sc.sql(
        s"""
           |select st_geomFromGeoHash('$geohash', $precision)
          """.stripMargin
      )

      val (minLon, minLat, maxLon, maxLat) = (-5.625, 42.583, -5.581, 42.627)
      val delta = .022

      val dfBoxCoords = dfBlank().select(st_geomFromGeoHash(lit(geohash), lit(precision))).first.getCoordinates
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

    "st_geomFromGeoJSON" in {
      sc.sql("select st_geomFromGeoJSON(null)").collect.head(0) must beNull
      dfBlank().select(st_geomFromWKT(lit(null))).first must beNull

      val point = "POINT(-37.23456 18.12345)"
      val pointInGeoJson = """{"type":"Point","coordinates":[-37.23456, 18.12345]}"""
      val r = sc.sql(
        s"""
           |select st_geomFromGeoJSON('$pointInGeoJson')
        """.stripMargin
      )

      val expected = WKTUtils.read(point)

      r.collect().head.getAs[Geometry](0) mustEqual expected

      dfBlank().select(st_geomFromGeoJSON(pointInGeoJson)).first mustEqual expected
    }

    "st_pointFromGeoHash" in {
      sc.sql("select st_pointFromGeoHash(null, null)").collect.head(0) must beNull
      dfBlank().select(st_pointFromGeoHash(lit(null), lit(null))).first must beNull

      val geohash = "ezs42"
      val precision = 25
      val r = sc.sql(
        s"""
           |select st_pointFromGeoHash('$geohash', $precision)
        """.stripMargin
      )

      val (x, y) = (-5.603, 42.605)
      val delta = .022

      val dfPoint = dfBlank().select(st_pointFromGeoHash(lit(geohash), lit(precision))).first
      dfPoint.getX must beCloseTo(x, delta)
      dfPoint.getY must beCloseTo(y, delta)

      val point = r.collect().head.getAs[Point](0)
      point.getX must beCloseTo(x, delta)
      point.getY must beCloseTo(y, delta)
    }

    "st_geomFromWKT" in {
      sc.sql("select st_geomFromWKT(null)").collect.head(0) must beNull
      dfBlank().select(st_geomFromWKT(lit(null))).first must beNull

      val point = "POINT(0 0)"
      val r = sc.sql(
        s"""
           |select st_geomFromWKT('$point')
        """.stripMargin
      )

      val expected = WKTUtils.read(point)

      r.collect().head.getAs[Geometry](0) mustEqual expected

      dfBlank().select(st_geomFromWKT(point)).first mustEqual expected
    }

    "st_geomFromWKT With Z Value" in {

      val point = "POINT(1 1 1)"
      val r = sc.sql(
        s"""
           |select st_geomFromWKT('$point')
        """.stripMargin
      )

      foreach(Seq(r.collect().head.getAs[Geometry](0), dfBlank().select(st_geomFromWKT(point)).first)) { actual =>
        actual must beAnInstanceOf[Point]
        actual.asInstanceOf[Point].getCoordinate.getZ mustEqual 1
      }
    }

    "st_geometryFromText" in {
      sc.sql("select st_geometryFromText(null)").collect.head(0) must beNull

      val r = sc.sql(
        """
          |select st_geometryFromText('POINT(0 0)')
        """.stripMargin
      )

      r.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_geomFromWKB" in {
      sc.sql("select st_geomFromWKB(null)").collect.head(0) must beNull
      dfBlank().select(st_geomFromWKB(lit(null))).first must beNull

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

      dfBlank().select(st_geomFromWKB(st_byteArray(lit(new String(geomArr))))).first mustEqual expected
      dfBlank().select(st_geomFromWKB(geomArr)).first mustEqual expected
    }

    "st_lineFromText" in {
      sc.sql("select st_lineFromText(null)").collect.head(0) must beNull
      dfBlank().select(st_lineFromText(lit(null))).first must beNull

      val line = "LINESTRING(0 0, 1 1, 2 2)"
      val r = sc.sql(
        s"""
           |select st_lineFromText('$line')
        """.stripMargin
      )
      val expected = WKTUtils.read(line)
      r.collect().head.getAs[LineString](0) mustEqual expected
      dfBlank().select(st_lineFromText(line)).first mustEqual expected
    }

    "st_makeBBOX" in {
      sc.sql("select st_makeBBOX(null, null, null, null)").collect.head(0) must beNull
      dfBlank().select(st_makeBBOX(lit(null), lit(null), lit(null), lit(null))).first must beNull

      val r = sc.sql(
        """
          |select st_makeBBOX(0.0, 0.0, 2.0, 2.0)
        """.stripMargin
      )
      val fact = new GeometryFactory()

      val expected = fact.toGeometry(new Envelope(0, 2, 0, 2))
      r.collect().head.getAs[Geometry](0) mustEqual expected

      dfBlank().select(st_makeBBOX(0.0, 0.0, 2.0, 2.0)).first mustEqual expected
    }

    "st_makeBox2D" in {
      sc.sql("select st_makeBox2D(null, null)").collect.head(0) must beNull
      dfBlank().select(st_makeBox2D(lit(null), lit(null))).first must beNull

      val r = sc.sql(
        """
          |select st_makeBox2D(st_castToPoint(st_geomFromWKT('POINT(0 0)')),
          |                    st_castToPoint(st_geomFromWKT('POINT(2 2)')))
        """.stripMargin
      )
      val expected = WKTUtils.read("POLYGON((0.0 0.0, 0.0 2.0, 2.0 2.0, 2.0 0.0, 0.0 0.0))")
      r.collect().head.getAs[Geometry](0) mustEqual expected

      dfBlank().select(st_makeBox2D(
        st_castToPoint(st_geomFromWKT("POINT(0 0)")),
        st_castToPoint(st_geomFromWKT("POINT(2 2)"))
      )).first mustEqual expected

      val p1 = GeometricConstructorFunctions.ST_MakePoint(0, 0)
      val p2 = GeometricConstructorFunctions.ST_MakePoint(2, 2)

      dfBlank().select(st_makeBox2D(p1, p2)).first mustEqual expected
    }

    "st_makePolygon" in {
      sc.sql("select st_makePolygon(null)").collect.head(0) must beNull
      dfBlank().select(st_makePolygon(lit(null))).first must beNull

      val line = "LINESTRING(0 0, 2 2, 5 4, 7 2, 5 2, 3 0, 0 0)"
      val r = sc.sql(
        s"""
           |select st_makePolygon(st_castToLineString(
           |    st_geomFromWKT('$line')))
        """.stripMargin
      )
      val expected = WKTUtils.read("POLYGON((0 0, 2 2, 5 4, 7 2, 5 2, 3 0, 0 0))")
      r.collect().head.getAs[Polygon](0) mustEqual expected
      dfBlank().select(st_makePolygon(st_castToLineString(st_geomFromWKT(line)))).first mustEqual expected

      val lineInst = WKTUtils.read(line).asInstanceOf[LineString]
      dfBlank().select(st_makePolygon(lineInst)).first mustEqual expected
    }

    "st_makePoint" in {
      sc.sql("select st_makePoint(null, null)").collect.head(0) must beNull
      dfBlank().select(st_makePoint(lit(null), lit(null))).first must beNull

      val r = sc.sql(
        """
          |select st_makePoint(0, 1) geom
        """.stripMargin
      )
      val expected = WKTUtils.read("POINT(0 1)")
      r.collect().head.getAs[Point](0) mustEqual expected
      dfBlank().select(st_makePoint(0, 1)).first mustEqual expected

      import spark.implicits._
      r.as[GeometryContainer].head must haveClass[GeometryContainer]
    }

    "st_makePointM" in {
      sc.sql("select st_makePointM(null, null, null)").collect.head(0) must beNull
      dfBlank().select(st_makePointM(lit(null), lit(null), lit(null))).first must beNull

      val r = sc.sql(
        """
          |select st_makePointM(0, 0, 1)
        """.stripMargin
      )
      val expected = WKTUtils.read("POINT(0 0 1)")
      r.collect().head.getAs[Point](0) mustEqual expected
      dfBlank().select(st_makePointM(0, 0, 1)).first mustEqual expected
    }

    "st_makeLine" in {
      sc.sql("select st_makeLine(null)").collect.head(0) must beNull
      dfBlank().select(st_makeLine(lit(null))).first must beNull

      val p1 = GeometricConstructorFunctions.ST_MakePoint(0, 0)
      val p2 = GeometricConstructorFunctions.ST_MakePoint(2, 2)
      val p3 = GeometricConstructorFunctions.ST_MakePoint(5, 2)
      val expected = WKTUtils.read("LINESTRING(0 0, 2 2, 5 2)")

      dfBlank().select(st_makeLine(Seq(p1, p2, p3))).first mustEqual expected

      dfBlank().select(st_makeLine(array(pointLit(p1), pointLit(p2), pointLit(p3)))).first mustEqual expected
    }

    "st_mLineFromText" in {
      sc.sql("select st_mLineFromText(null)").collect.head(0) must beNull
      dfBlank().select(st_mLineFromText(lit(null))).first must beNull

      val line = "MULTILINESTRING((0 0, 1 1, 2 2), (0 1, 1 2, 2 3))"
      val r = sc.sql(
        s"""
           |select st_mLineFromText('$line')
        """.stripMargin
      )
      val expected = WKTUtils.read("MULTILINESTRING((0 0, 1 1, 2 2), (0 1, 1 2, 2 3))")
      r.collect().head.getAs[MultiLineString](0) mustEqual expected
      dfBlank().select(st_mLineFromText(line)).first mustEqual expected
    }

    "st_mPointFromText" in {
      sc.sql("select st_mPointFromText(null)").collect.head(0) must beNull
      dfBlank().select(st_mPointFromText(lit(null))).first must beNull

      val point = "MULTIPOINT((0 0), (1 1))"
      val r = sc.sql(
        s"""
           |select st_mPointFromText('$point')
        """.stripMargin
      )
      val expected = WKTUtils.read("MULTIPOINT((0 0), (1 1))")
      r.collect().head.getAs[MultiPoint](0) mustEqual expected
      dfBlank().select(st_mPointFromText(point)).first mustEqual expected
    }

    "st_mPolyFromText" in {
      sc.sql("select st_mPolyFromText(null)").collect.head(0) must beNull
      dfBlank().select(st_mPolyFromText(lit(null))).first must beNull

      val poly = "MULTIPOLYGON((( -1 -1, 0 1, 1 -1, -1 -1 )),((-4 4, 4 4, 4 -4, -4 -4, -4 4), (2 2, -2 2, -2 -2, 2 -2, 2 2)))"
      val r = sc.sql(
        s"""
           |select st_mPolyFromText('$poly')
        """.stripMargin
      )

      val expected = WKTUtils.read(poly)
      r.collect().head.getAs[MultiPolygon](0) mustEqual expected
      dfBlank().select(st_mPolyFromText(poly)).first mustEqual expected
    }

    "st_point" in {
      sc.sql("select st_point(null, null)").collect.head(0) must beNull
      dfBlank().select(st_point(lit(null), lit(null))).first must beNull

      val r = sc.sql(
        """
          |select st_point(0, 0)
        """.stripMargin
      )
      val expected = WKTUtils.read("POINT(0 0)")
      r.collect().head.getAs[Point](0) mustEqual expected
      dfBlank().select(st_point(0, 0)).first mustEqual expected
    }

    "st_pointFromText" in {
      sc.sql("select st_pointFromText(null)").collect.head(0) must beNull
      dfBlank().select(st_pointFromText(lit(null))).first must beNull

      val point = "Point(0 0)"
      val r = sc.sql(
        s"""
           |select st_pointFromText('$point')
        """.stripMargin
      )
      val expected = WKTUtils.read(point)
      r.collect().head.getAs[Point](0) mustEqual expected
      dfBlank().select(st_pointFromText(point)).first mustEqual expected
    }

    "st_pointFromWKB" in {
      sc.sql("select st_pointFromWKB(null)").collect.head(0) must beNull
      dfBlank().select(st_pointFromWKB(lit(null))).first must beNull

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
      dfBlank().select(st_pointFromWKB(pointArr)).first mustEqual expected
    }

    "st_polygon" in {
      sc.sql("select st_polygon(null)").collect.head(0) must beNull
      dfBlank().select(st_polygon(lit(null))).first must beNull

      val line = "LINESTRING(0 0, 2 2, 5 2, 3 0, 0 0)"
      val r = sc.sql(
        s"""
           |select st_polygon(st_castToLineString(
           |    st_geomFromWKT('$line')))
        """.stripMargin
      )
      val expected = WKTUtils.read("POLYGON((0 0, 2 2, 5 2, 3 0, 0 0))")
      r.collect().head.getAs[Polygon](0) mustEqual expected
      dfBlank().select(st_polygon(st_castToLineString(st_geomFromWKT(line)))).first mustEqual expected

      val lineInst = WKTUtils.read(line).asInstanceOf[LineString]
      dfBlank().select(st_makePolygon(lineInst)).first mustEqual expected
    }

    "st_polygonFromText" in {
      sc.sql("select st_polygonFromText(null)").collect.head(0) must beNull
      dfBlank().select(st_polygonFromText(lit(null))).first must beNull

      val poly = "POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"
      val r = sc.sql(
        s"""
           |select st_polygonFromText('$poly')
        """.stripMargin
      )

      val expected =  WKTUtils.read("POLYGON((0.0 0.0, 2.0 0.0, 2.0 2.0, 0.0 2.0, 0.0 0.0))")
      r.collect().head.getAs[Polygon](0) mustEqual expected
      dfBlank().select(st_polygonFromText(poly)).first mustEqual expected
    }

    "geometry literals" in {
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

      dfBlank().select(pointLit(point)).first mustEqual point
      dfBlank().select(lineLit(line)).first mustEqual line
      dfBlank().select(polygonLit(poly)).first mustEqual poly
      dfBlank().select(mPointLit(mpoint)).first mustEqual mpoint
      dfBlank().select(mLineLit(mline)).first mustEqual mline
      dfBlank().select(mPolygonLit(mpoly)).first mustEqual mpoly
      dfBlank().select(geomCollLit(coll)).first mustEqual coll
      dfBlank().select(geomLit(coll)).first mustEqual coll
    }
  }

  "sql geometry accessors" should {

    "st_castToPoint with null" in {
      sc.sql("select st_castToPoint(null)").collect.head(0) must beNull
      dfBlank().select(st_castToPoint(lit(null))).first must beNull

      import spark.implicits._
      dfBlank().select(st_castToPoint(lit(null)) as 'geom).as[PointContainer].head must haveClass[PointContainer]
    }

    "st_castToPoint with point" in {
      val pointTxt = "POINT(1 1)"
      val point = s"st_geomFromWKT('$pointTxt')"
      val df = sc.sql(s"select st_castToPoint($point) geom")
      df.collect.head(0).asInstanceOf[AnyRef] must haveClass[Point]
      dfBlank().select(st_castToPoint(st_geomFromWKT(pointTxt))).first must haveClass[Point]

      import spark.implicits._
      df.as[PointContainer].head must haveClass[PointContainer]
    }

    "st_castToPolygon with null" in {
      sc.sql("select st_castToPolygon(null)").collect.head(0) must beNull
      dfBlank().select(st_castToPolygon(lit(null))).first must beNull

      import spark.implicits._
      dfBlank().select(st_castToPolygon(lit(null)) as 'geom).as[PolygonContainer].head must haveClass[PolygonContainer]
    }

    "st_castToPolygon with polygon" in {
      val polygonTxt = "POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))"
      val polygon = s"st_geomFromWKT('$polygonTxt')"
      val df = sc.sql(s"select st_castToPolygon($polygon) geom")
      df.collect.head(0).asInstanceOf[AnyRef] must haveClass[Polygon]
      dfBlank().select(st_castToPolygon(st_geomFromWKT(polygonTxt))).first must haveClass[Polygon]

      import spark.implicits._
      df.as[PolygonContainer].head must haveClass[PolygonContainer]
    }

    "st_castToLineString with null" in {
      sc.sql("select st_castToLineString(null)").collect.head(0) must beNull
      dfBlank().select(st_castToLineString(lit(null))).first must beNull

      import spark.implicits._
      dfBlank().select(st_castToLineString(lit(null)) as 'geom).as[LineStringContainer].head must haveClass[LineStringContainer]
    }

    "st_castToLineString with linestring" in {
      val lineTxt = "LINESTRING(1 1, 2 2)"
      val line = s"st_geomFromWKT('$lineTxt')"
      val df = sc.sql(s"select st_castToLineString($line) geom")
      df.collect.head(0).asInstanceOf[AnyRef] must haveClass[LineString]
      dfBlank().select(st_castToLineString(st_geomFromWKT(lineTxt))).first must haveClass[LineString]

      import spark.implicits._
      df.as[LineStringContainer].head must haveClass[LineStringContainer]
    }

    "st_castToGeometry with null" in {
      sc.sql("select st_castToGeometry(null)").collect.head(0) must beNull
      dfBlank().select(st_castToGeometry(lit(null))).first must beNull

      import spark.implicits._
      dfBlank().select(st_castToGeometry(lit(null)) as 'geom).as[GeometryContainer].head must haveClass[GeometryContainer]
    }

    "st_castToGeometry with point" in {
      val pointTxt = "POINT(1 1)"
      val point = s"st_geomFromWKT('$pointTxt')"
      val df = sc.sql(s"select st_castToGeometry($point) geom")
      df.collect.head(0).asInstanceOf[AnyRef] must haveClass[Point]
      dfBlank().select(st_castToGeometry(st_geomFromWKT(pointTxt))).first must haveClass[Point]

      import spark.implicits._
      df.as[GeometryContainer].head must haveClass[GeometryContainer]
    }

    "st_castToGeometry with polygon" in {
      val polygonTxt = "POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))"
      val polygon = s"st_geomFromWKT('$polygonTxt')"
      val df = sc.sql(s"select st_castToGeometry($polygon) geom")
      df.collect.head(0).asInstanceOf[AnyRef] must haveClass[Polygon]
      dfBlank().select(st_castToGeometry(st_geomFromWKT(polygonTxt))).first must haveClass[Polygon]

      import spark.implicits._
      df.as[GeometryContainer].head must haveClass[GeometryContainer]
    }

    "st_castToGeometry with linestring" in {
      val lineTxt = "LINESTRING(1 1, 2 2)"
      val line = s"st_geomFromWKT('$lineTxt')"
      val df = sc.sql(s"select st_castToGeometry($line) geom")
      df.collect.head(0).asInstanceOf[AnyRef] must haveClass[LineString]
      dfBlank().select(st_castToGeometry(st_geomFromWKT(lineTxt))).first must haveClass[LineString]

      import spark.implicits._
      df.as[GeometryContainer].head must haveClass[GeometryContainer]
    }

    "st_bytearray with null" in {
      sc.sql("select st_byteArray(null)").collect.head(0) must beNull
      dfBlank().select(st_byteArray(lit(null))).first must beNull
    }

    "st_bytearray with bytearray" in {
      val df = sc.sql(s"select st_byteArray('foo')")
      val expected = "foo".toArray.map(_.toByte)
      df.collect.head(0) mustEqual expected
      dfBlank().select(st_byteArray(lit("foo"))).first mustEqual expected
    }
  }
}
