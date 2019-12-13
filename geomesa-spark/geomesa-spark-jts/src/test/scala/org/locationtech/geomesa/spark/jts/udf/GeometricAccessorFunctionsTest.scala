/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import org.locationtech.jts.geom.Geometry
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.locationtech.geomesa.spark.jts._
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeometricAccessorFunctionsTest extends Specification with TestEnvironment  {

  sequential

  // before
  step {
    // Trigger initialization of spark session
    val _ = spark
  }

  "sql geometry accessors" should {

    "st_boundary" >> {
      sc.sql("select st_boundary(null)").collect.head(0) must beNull
      dfBlank.select(st_boundary(lit(null))).first must beNull

      val line = "LINESTRING(1 1, 0 0, -1 1)"
      val result = sc.sql(
        s"""
          |select st_boundary(st_geomFromWKT('$line'))
        """.stripMargin
      )
      val expected = WKTUtils.read("MULTIPOINT(1 1, -1 1)")
      result.collect().head.getAs[Geometry](0) mustEqual expected
      dfBlank.select(st_boundary(st_geomFromWKT(line))).first mustEqual expected

    }

    "st_coordDim" >> {
      sc.sql("select st_coordDim(null)").collect.head(0) must beNull
      dfBlank.select(st_coordDim(lit(null))).first must beNull

      val result = sc.sql(
        """
          |select st_coordDim(st_geomFromWKT('POINT(0 0)'))
        """.stripMargin
      )
      result.collect().head.getAs[Int](0) mustEqual 2

      dfBlank.select(st_coordDim(st_geomFromWKT("POINT(0 0)"))).first mustEqual 2
    }

    "st_dimension" >> {
      "null" >> {
        sc.sql("select st_dimension(null)").collect.head(0) must beNull
        dfBlank.select(st_dimension(lit(null))).first must beNull
      }

      "point" >> {
        val result = sc.sql(
          """
            |select st_dimension(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 0
        dfBlank.select(st_dimension(st_geomFromWKT("POINT(0 0)"))).first mustEqual 0
      }

      "linestring" >> {
        val line = "LINESTRING(1 1, 0 0, -1 1)"
        val result = sc.sql(
          s"""
            |select st_dimension(st_geomFromWKT('$line'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 1

        dfBlank.select(st_dimension(st_geomFromWKT(line))).first mustEqual 1
      }

      "polygon" >> {
        val poly = "POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))"
        val result = sc.sql(
          s"""
            |select st_dimension(st_geomFromWKT('$poly'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 2
        dfBlank.select(st_dimension(st_geomFromWKT(poly))).first mustEqual 2
      }

      "geometrycollection" >> {
        val geom = "GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))"
        val result = sc.sql(
          s"""
             |select st_dimension(st_geomFromWKT('$geom'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 1
        dfBlank.select(st_dimension(st_geomFromWKT(geom))).first mustEqual 1
      }
    }

    "st_envelope" >> {
      "null" >> {
        sc.sql("select st_envelope(null)").collect.head(0) must beNull
        dfBlank.select(st_envelope(lit(null))).first must beNull
      }

      "point" >> {
        val result = sc.sql(
          """
            |select st_envelope(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        val expected = WKTUtils.read("POINT(0 0)")
        result.collect().head.getAs[Geometry](0) mustEqual expected
        dfBlank.select(st_envelope(st_geomFromWKT("POINT(0 0)"))).first mustEqual expected
      }

      "linestring" >> {
        val result = sc.sql(
          """
            |select st_envelope(st_geomFromWKT('LINESTRING(0 0, 1 3)'))
          """.stripMargin
        )
        val expected = WKTUtils.read("POLYGON((0 0,0 3,1 3,1 0,0 0))")
        result.collect().head.getAs[Geometry](0) mustEqual expected
        dfBlank.select(st_envelope(st_geomFromWKT("LINESTRING(0 0, 1 3)"))).first mustEqual expected
      }

      "polygon" >> {
        val poly = "POLYGON((0 0, 0 1, 1.0000001 1, 1.0000001 0, 0 0))"
        val result = sc.sql(
          s"""
            |select st_envelope(st_geomFromWKT('$poly'))
          """.stripMargin
        )
        val expected = WKTUtils.read("POLYGON((0 0, 0 1, 1.0000001 1, 1.0000001 0, 0 0))")
        result.collect().head.getAs[Geometry](0) mustEqual expected
        dfBlank.select(st_envelope(st_geomFromWKT(poly))).first mustEqual expected
      }
    }

    "st_exteriorRing" >> {
      "null" >> {
        sc.sql("select st_exteriorRing(null)").collect.head(0) must beNull
        dfBlank.select(st_exteriorRing(lit(null))).first must beNull
      }

      "point" >> {
        val result = sc.sql(
          """
            |select st_exteriorRing(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) must beNull
        dfBlank.select(st_exteriorRing(st_geomFromWKT("POINT(0 0)"))).first must beNull
      }

      "polygon without an interior ring" >> {
        val poly = "POLYGON((30 10, 40 40, 20 40, 10 20, 30 10))"
        val result = sc.sql(
          s"""
             |select st_exteriorRing(st_geomFromWKT('$poly'))
          """.stripMargin
        )
        val expected = WKTUtils.read("LINESTRING(30 10, 40 40, 20 40, 10 20, 30 10)")
        result.collect().head.getAs[Geometry](0) mustEqual expected
        dfBlank.select(st_exteriorRing(st_geomFromWKT(poly))).first mustEqual expected
      }

      "polygon with an interior ring" >> {
        val poly = "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))"
        val result = sc.sql(
          s"""
            |select st_exteriorRing(st_geomFromWKT('$poly'))
          """.stripMargin
        )
        val expected = WKTUtils.read("LINESTRING(35 10, 45 45, 15 40, 10 20, 35 10)")
        result.collect().head.getAs[Geometry](0) mustEqual expected
        dfBlank.select(st_exteriorRing(st_geomFromWKT(poly))).first mustEqual expected
      }
    }

    "st_geometryN" >> {
      "null" >> {
        sc.sql("select st_geometryN(null, null)").collect.head(0) must beNull
        dfBlank.select(st_geometryN(lit(null), lit(null))).first must beNull
      }

      "point" >> {
        val result = sc.sql(
          """
            |select st_geometryN(st_geomFromWKT('POINT(0 0)'), 1)
          """.stripMargin
        )
        val expected = WKTUtils.read("POINT(0 0)")
        result.collect().head.getAs[Geometry](0) mustEqual expected
        dfBlank.select(st_geometryN(st_geomFromWKT("POINT(0 0)"), lit(1))).first mustEqual expected
      }

      "multilinestring" >> {
        val geom = "MULTILINESTRING ((10 10, 20 20, 10 40),(40 40, 30 30, 40 20, 30 10))"
        val result = sc.sql(
          s"""
             |select st_geometryN(st_geomFromWKT('$geom'), 1)
          """.stripMargin
        )
        val expected = WKTUtils.read("LINESTRING(10 10, 20 20, 10 40)")
        result.collect().head.getAs[Geometry](0) mustEqual expected
        dfBlank.select(st_geometryN(st_geomFromWKT(geom), lit(1))).first mustEqual expected
      }

      "geometrycollection" >> {
        val geom = "GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))"
        val result = sc.sql(
          s"""
            |select st_geometryN(st_geomFromWKT('$geom'), 1)
          """.stripMargin
        )
        val expected = WKTUtils.read("LINESTRING(1 1,0 0)")
        result.collect().head.getAs[Geometry](0) mustEqual expected
        dfBlank.select(st_geometryN(st_geomFromWKT(geom), lit(1))).first mustEqual expected
      }
    }

    "st_geometryType" >> {
      "null" >> {
        sc.sql("select st_geometryType(null)").collect.head(0) must beNull
        dfBlank.select(st_geomFromWKT(lit(null))).first must beNull
      }

      "point" >> {
        val result = sc.sql(
          """
            |select st_geometryType(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[String](0) mustEqual "Point"
        dfBlank.select(st_geometryType(st_geomFromWKT("POINT(0 0)"))).first mustEqual "Point"
      }

      "linestring" >> {
        val result = sc.sql(
          """
            |select st_geometryType(st_geomFromWKT('LINESTRING(0 0, 1 3)'))
          """.stripMargin
        )
        result.collect().head.getAs[String](0) mustEqual "LineString"
        dfBlank.select(st_geometryType(st_geomFromWKT("LINESTRING(0 0, 1 3)"))).first mustEqual "LineString"
      }

      "geometrycollection" >> {
        val geom = "GEOMETRYCOLLECTION(LINESTRING(1 1,0 0),POINT(0 0))"
        val result = sc.sql(
          s"""
            |select st_geometryType(st_geomFromWKT('$geom'))
          """.stripMargin
        )
        result.collect().head.getAs[String](0) mustEqual "GeometryCollection"
        dfBlank.select(st_geometryType(st_geomFromWKT(geom))).first mustEqual "GeometryCollection"
      }

    }

    "st_interiorRingN" >> {
      "null" >> {
        sc.sql("select st_interiorRingN(null, null)").collect.head(0) must beNull
        dfBlank.select(st_interiorRingN(lit(null), lit(null))).first must beNull
      }

      "point" >> {
        val result = sc.sql(
          """
            |select st_interiorRingN(st_geomFromWKT('POINT(0 0)'), 1)
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) must beNull
        dfBlank.select(st_interiorRingN(st_geomFromWKT("POINT(0 0)"), lit(1))).first must beNull
      }

      "polygon with a valid int" >> {
        val poly = "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10),(20 30, 35 35, 30 20, 20 30))"
        val result = sc.sql(
          s"""
            |select st_interiorRingN(st_geomFromWKT('$poly'), 1)
          """.stripMargin
        )
        val expected = WKTUtils.read("LINESTRING(20 30, 35 35, 30 20, 20 30)")
        result.collect().head.getAs[Geometry](0) mustEqual expected
        dfBlank.select(st_interiorRingN(st_geomFromWKT(poly), lit(1))).first mustEqual expected
      }

      "polygon with an invalid int" >> {
        val poly = "POLYGON ((35 10, 45 45, 15 40, 10 20, 35 10), (20 30, 35 35, 30 20, 20 30))"
        val result = sc.sql(
          s"""
            |select st_interiorRingN(st_geomFromWKT('$poly'), 5)
          """.stripMargin
        )
        result.collect().head.getAs[Geometry](0) must beNull
        dfBlank.select(st_interiorRingN(st_geomFromWKT(poly), lit(5))).first must beNull
      }
    }

    "st_isClosed" >> {
      "null" >> {
        sc.sql("select st_isClosed(null)").collect.head(0) must beNull
        dfBlank.select(st_isClosed(lit(null))).first must beNull
      }

      "open linestring" >> {
        val line = "LINESTRING(0 0, 1 1)"
        val result = sc.sql(
          s"""
            |select st_isClosed(st_geomFromWKT('$line'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
        dfBlank.select(st_isClosed(st_geomFromWKT(line))).first.booleanValue() must beFalse
      }

      "closed linestring" >> {
        val line = "LINESTRING(0 0, 0 1, 1 1, 0 0)"
        val result = sc.sql(
          s"""
            |select st_isClosed(st_geomFromWKT('$line'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
        dfBlank.select(st_isClosed(st_geomFromWKT(line))).first.booleanValue must beTrue
      }

      "open multilinestring" >> {
        val line = "MULTILINESTRING((0 0, 0 1, 1 1, 0 0),(0 0, 1 1))"
        val result = sc.sql(
          s"""
             |select st_isClosed(st_geomFromWKT('$line'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
        dfBlank.select(st_isClosed(st_geomFromWKT(line))).first.booleanValue must beFalse
      }

      "closed multilinestring" >> {
        val line = "MULTILINESTRING((0 0, 0 1, 1 1, 0 0),(0 0, 1 1, 0 0))"
        val result = sc.sql(
          s"""
             |select st_isClosed(st_geomFromWKT('$line'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
        dfBlank.select(st_isClosed(st_geomFromWKT(line))).first.booleanValue() must beTrue
      }
    }

    "st_isCollection" >> {
      "null" >> {
        sc.sql("select st_isCollection(null)").collect.head(0) must beNull
        dfBlank.select(st_isCollection(lit(null))).first must beNull
      }

      "point" >> {
        val result = sc.sql(
          """
            |select st_isCollection(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
        dfBlank.select(st_isCollection(st_geomFromWKT("POINT(0 0)"))).first.booleanValue() must beFalse
      }

      "multipoint" >> {
        val point = "MULTIPOINT((0 0), (42 42))"
        val result = sc.sql(
          s"""
             |select st_isCollection(st_geomFromWKT('$point'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
        dfBlank.select(st_isCollection(st_geomFromWKT(point))).first.booleanValue() must beTrue
      }

      "geometrycollection" >> {
        val geom = "GEOMETRYCOLLECTION(POINT(0 0))"
        val result = sc.sql(
          s"""
            |select st_isCollection(st_geomFromWKT('$geom'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
        dfBlank.select(st_isCollection(st_geomFromWKT(geom))).first.booleanValue() must beTrue
      }
    }

    "st_isEmpty" >> {
      "null" >> {
        sc.sql("select st_isEmpty(null)").collect.head(0) must beNull
      }

      "empty geometrycollection" >> {
        val result = sc.sql(
          """
            |select st_isEmpty(st_geomFromWKT('GEOMETRYCOLLECTION EMPTY'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
        dfBlank.select(st_isEmpty(st_geomFromWKT("GEOMETRYCOLLECTION EMPTY"))).first.booleanValue() must beTrue
      }

      "non-empty point" >> {
        val result = sc.sql(
          """
            |select st_isEmpty(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
        dfBlank.select(st_isEmpty(st_geomFromWKT("POINT(0 0)"))).first.booleanValue() must beFalse
      }
    }

    "st_isRing" >> {
      "null" >> {
        sc.sql("select st_isRing(null)").collect.head(0) must beNull
        dfBlank.select(st_isRing(lit(null))).first must beNull
      }

      "closed and simple linestring" >> {
        val line = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"
        val result = sc.sql(
          s"""
             |select st_isRing(st_geomFromWKT('$line'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
        dfBlank.select(st_isRing(st_geomFromWKT(line))).first.booleanValue() must beTrue
      }

      "closed and non-simple linestring" >> {
        val line = "LINESTRING(0 0, 0 1, 1 0, 1 1, 0 0)"
        val result = sc.sql(
          s"""
             |select st_isRing(st_geomFromWKT('$line'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
        dfBlank.select(st_isRing(st_geomFromWKT(line))).first.booleanValue() must beFalse
      }
    }

    "st_isSimple" >> {
      "null" >> {
        sc.sql("select st_isSimple(null)").collect.head(0) must beNull
        dfBlank.select(st_isSimple(lit(null))).first must beNull
      }

      "simple point" >> {
        val result = sc.sql(
          """
            |select st_isSimple(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
        dfBlank.select(st_isSimple(st_geomFromWKT("POINT(0 0)"))).first.booleanValue() must beTrue
      }

      "simple linestring" >> {
        val line = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"
        val result = sc.sql(
          s"""
             |select st_isSimple(st_geomFromWKT('$line'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
        dfBlank.select(st_isSimple(st_geomFromWKT(line))).first.booleanValue() must beTrue
      }

      "non-simple linestring" >> {
        val line = "LINESTRING(1 1,2 2,2 3.5,1 3,1 2,2 1)"
        val result = sc.sql(
          s"""
             |select st_isSimple(st_geomFromWKT('$line'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
        dfBlank.select(st_isSimple(st_geomFromWKT(line))).first.booleanValue() must beFalse
      }

      "non-simple polygon" >> {
        val poly = "POLYGON((1 2, 3 4, 5 6, 1 2))"
        val result = sc.sql(
          s"""
            |select st_isSimple(st_geomFromWKT('$poly'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
        dfBlank.select(st_isSimple(st_geomFromWKT(poly))).first.booleanValue() must beFalse
      }
    }

    "st_isValid" >> {
      "null" >> {
        sc.sql("select st_isValid(null)").collect.head(0) must beNull
        dfBlank.select(st_isValid(lit(null))).first must beNull
      }

      "valid linestring" >> {
        val line = "LINESTRING(0 0, 1 1)"
        val result = sc.sql(
          s"""
             |select st_isValid(st_geomFromWKT('$line'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beTrue
        dfBlank.select(st_isValid(st_geomFromWKT(line))).first.booleanValue() must beTrue
      }

      "invalid polygon" >> {
        val poly = "POLYGON((0 0, 1 1, 1 2, 1 1, 0 0))"
        val result = sc.sql(
          s"""
            |select st_isValid(st_geomFromWKT('$poly'))
          """.stripMargin
        )
        result.collect().head.getAs[Boolean](0) must beFalse
        dfBlank.select(st_isValid(st_geomFromWKT(poly))).first.booleanValue() must beFalse
      }
    }

    "st_numGeometries" >> {
      "null" >> {
        sc.sql("select st_numGeometries(null)").collect.head(0) must beNull
        dfBlank.select(st_numGeometries(lit(null))).first must beNull
      }

      "point" >> {
        val result = sc.sql(
          """
            |select st_numGeometries(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 1
        dfBlank.select(st_numGeometries(st_geomFromWKT("POINT(0 0)"))).first mustEqual 1
      }

      "linestring" >> {
        val line = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"
        val result = sc.sql(
          s"""
             |select st_numGeometries(st_geomFromWKT('$line'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 1
        dfBlank.select(st_numGeometries(st_geomFromWKT(line))).first mustEqual 1
      }

      "geometrycollection" >> {
        val geom = "GEOMETRYCOLLECTION(MULTIPOINT(-2 3,-2 2), LINESTRING(5 5,10 10), POLYGON((-7 4.2,-7.1 5,-7.1 4.3,-7 4.2)))"
        val result = sc.sql(
          s"""
             |select st_numGeometries(st_geomFromWKT('$geom'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 3
        dfBlank.select(st_numGeometries(st_geomFromWKT(geom))).first mustEqual 3
      }
    }

    "st_numPoints" >> {
      "null" >> {
        sc.sql("select st_numPoints(null)").collect.head(0) must beNull
        dfBlank.select(st_numPoints(lit(null))).first must beNull
      }

      "point" >> {
        val result = sc.sql(
          """
            |select st_numPoints(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 1
      }

      "multipoint" >> {
        val point = "MULTIPOINT(-2 3,-2 2)"
        val result = sc.sql(
          s"""
             |select st_numPoints(st_geomFromWKT('$point'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 2
        dfBlank.select(st_numPoints(st_geomFromWKT(point))).first mustEqual 2
      }

      "multipoint" >> {
        val geom = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"
        val result = sc.sql(
          s"""
             |select st_numPoints(st_geomFromWKT('$geom'))
          """.stripMargin
        )
        result.collect().head.getAs[Int](0) mustEqual 5
        dfBlank.select(st_numPoints(st_geomFromWKT(geom))).first mustEqual 5
      }
    }

    "st_pointN" >> {
      "null" >> {
        sc.sql("select st_pointN(null, null)").collect.head(0) must beNull
        dfBlank.select(st_pointN(lit(null), lit(null))).first must beNull
      }

      "first point" >> {
        val line = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 2)"
        val result = sc.sql(
          s"""
             |select st_pointN(st_geomFromWKT('$line'), 1)
          """.stripMargin
        )
        val expected = WKTUtils.read("POINT(0 0)")
        result.collect().head.getAs[Geometry](0) mustEqual expected
        dfBlank.select(st_pointN(st_geomFromWKT(line), lit(1))).first mustEqual expected
      }

      "last point" >> {
        val line = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 2)"
        val result = sc.sql(
          s"""
            |select st_pointN(st_geomFromWKT('$line'), 5)
          """.stripMargin
        )
        val expected = WKTUtils.read("POINT(0 2)")
        result.collect().head.getAs[Geometry](0) mustEqual expected
        dfBlank.select(st_pointN(st_geomFromWKT(line), lit(5))).first mustEqual expected
      }

      "first point using a negative index" >> {
        val line = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 2)"
        val result = sc.sql(
          s"""
            |select st_pointN(st_geomFromWKT('$line'), -5)
          """.stripMargin
        )
        val expected = WKTUtils.read("POINT(0 0)")
        result.collect().head.getAs[Geometry](0) mustEqual expected
        dfBlank.select(st_pointN(st_geomFromWKT(line), lit(-5))).first mustEqual expected
      }

      "last point using a negative index" >> {
        val line = "LINESTRING(0 0, 0 1, 1 1, 1 0, 0 2)"
        val result = sc.sql(
          s"""
             |select st_pointN(st_geomFromWKT('$line'), -1)
          """.stripMargin
        )
        val expected = WKTUtils.read("POINT(0 2)")
        result.collect().head.getAs[Geometry](0) mustEqual expected
        dfBlank.select(st_pointN(st_geomFromWKT(line), lit(-1))).first mustEqual expected
      }
    }

    "st_x" >> {
      "null" >> {
        sc.sql("select st_x(null)").collect.head(0) must beNull
        dfBlank.select(st_x(lit(null))).first must beNull
      }

      "point" >> {
        val result = sc.sql(
          """
            |select st_x(st_geomFromWKT('POINT(0 1)'))
          """.stripMargin
        )
        result.collect().head.getAs[java.lang.Float](0) mustEqual 0f
        dfBlank.select(st_x(st_geomFromWKT("POINT(0 1)"))).first mustEqual 0f

      }

      "non-point" >> {
        val result = sc.sql(
          """
            |select st_x(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[java.lang.Float](0) must beNull
        dfBlank.select(st_x(st_geomFromWKT("LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"))).first must beNull
      }
    }

    "st_y" >> {
      "null" >> {
        sc.sql("select st_y(null)").collect.head(0) must beNull
        dfBlank.select(st_y(lit(null))).first must beNull
      }

      "point" >> {
        val result = sc.sql(
          """
            |select st_y(st_geomFromWKT('POINT(0 1)'))
          """.stripMargin
        )
        result.collect().head.getAs[java.lang.Float](0) mustEqual 1f
        dfBlank.select(st_y(st_geomFromWKT("POINT(0 1)"))).first mustEqual 1f

      }

      "non-point" >> {
        val result = sc.sql(
          """
            |select st_y(st_geomFromWKT('LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)'))
          """.stripMargin
        )
        result.collect().head.getAs[java.lang.Float](0) must beNull
        dfBlank.select(st_y(st_geomFromWKT("LINESTRING(0 0, 0 1, 1 1, 1 0, 0 0)"))).first must beNull

      }
    }
  }

  // after
  step {
    spark.stop()
  }
}
