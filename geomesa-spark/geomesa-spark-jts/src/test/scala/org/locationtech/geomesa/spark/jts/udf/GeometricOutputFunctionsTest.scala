/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import org.locationtech.jts.geom.{Coordinate, GeometryFactory}
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.locationtech.geomesa.spark.jts._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class GeometricOutputFunctionsTest extends Specification with TestEnvironment {

  "sql geometry constructors" should {
    sequential

    // before
    step {
      // Trigger initialization of spark session
      val _ = spark
    }

    "st_asBinary" >> {
      sc.sql("select st_asBinary(null)").collect.head(0) must beNull
      dfBlank.select(st_asBinary(lit(null))).first must beNull

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

      dfBlank.select(st_asBinary(st_geomFromWKT("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))"))).first mustEqual expected
    }

    "st_asGeoJSON" >> {
      "null" >> {
        sc.sql("select st_asGeoJSON(null)").collect.head(0) must beNull
        dfBlank.select(st_asGeoJSON(lit(null))).first must beNull
      }

      "point" >> {
        val r = sc.sql(
          """
            |select st_asGeoJSON(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        val expected = """{"type":"Point","coordinates":[0.0,0.0]}"""
        r.collect().head.getAs[String](0) mustEqual expected
        dfBlank.select(st_asGeoJSON(st_geomFromWKT("POINT(0 0)"))).first mustEqual expected
      }

      "lineString" >> {
        val line = "LINESTRING(0 0, 1 1, 2 2)"
        val r = sc.sql(
          s"""
            |select st_asGeoJSON(st_geomFromWKT('$line'))
          """.stripMargin
        )
        val expected = """{"type":"LineString","coordinates":[[0.0,0.0],[1,1],[2,2]]}"""
        r.collect().head.getAs[String](0) mustEqual expected
        dfBlank.select(st_asGeoJSON(st_geomFromWKT(line))).first mustEqual expected

      }

      "polygon" >> {
        val poly = "POLYGON((0.45 0.75, 1.15 0.75, 1.15 1.45, 0.45 1.45, 0.45 0.75))"
        val r = sc.sql(
          s"""
            |select st_asGeoJSON(st_geomFromWKT('$poly'))
          """.stripMargin
        )
        val expected = """{"type":"Polygon","coordinates":[[[0.45,0.75],[1.15,0.75],[1.15,1.45],[0.45,1.45],[0.45,0.75]]]}"""
        r.collect().head.getAs[String](0) mustEqual expected

        dfBlank.select(st_asGeoJSON(st_geomFromWKT(poly)))
          .first mustEqual expected
      }

      "multiPoint" >> {
        val point = "MULTIPOINT((0 0), (1 1))"
        val r = sc.sql(
          s"""
            |select st_asGeoJSON(st_geomFromWKT('$point'))
          """.stripMargin
        )
        val expected = """{"type":"MultiPoint","coordinates":[[0.0,0.0],[1,1]]}"""
        r.collect().head.getAs[String](0) mustEqual expected

        dfBlank.select(st_asGeoJSON(st_geomFromWKT(point))).first mustEqual expected
      }

      "multiLineString" >> {
        val line = "MULTILINESTRING((0 0, 1 1, 2 2), (-3 -3, -2 -2, -1 -1))"
        val r = sc.sql(
          s"""
            |select st_asGeoJSON(st_geomFromWKT('$line'))
          """.stripMargin
        )
        val expected = """{"type":"MultiLineString","coordinates":[[[0.0,0.0],[1,1],[2,2]],[[-3,-3],[-2,-2],[-1,-1]]]}"""
        r.collect().head.getAs[String](0) mustEqual expected

      }

      "multiPolygon" >> {
        val poly = "MULTIPOLYGON(((0.45 0.75, 1.15 0.75, 1.15 1.45, 0.45 1.45, 0.45 0.75)),((0 0, 1 0, 1 1, 0 1, 0 0)))"
        val r = sc.sql(
          s"""
            |select st_asGeoJSON(st_geomFromWKT('$poly'))
          """.stripMargin
        )
        val expected = """{"type":"MultiPolygon","coordinates":[[[[0.45,0.75],[1.15,0.75],[1.15,1.45],[0.45,1.45],""" +
          """[0.45,0.75]]],[[[0.0,0.0],[1,0.0],[1,1],[0.0,1],[0.0,0.0]]]]}"""

        r.collect().head.getAs[String](0) mustEqual expected
        dfBlank.select(st_asGeoJSON(st_geomFromWKT(poly))).first mustEqual expected
      }

      "geometryCollection" >> {
        val geom = "GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1, 2 2))"
        val r = sc.sql(
          s"""
            |select st_asGeoJSON(st_geomFromWKT('$geom'))
          """.stripMargin
        )
        val expected = """{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[0.0,0.0]},""" +
          """{"type":"LineString","coordinates":[[0.0,0.0],[1,1],[2,2]]}]}"""

        r.collect().head.getAs[String](0) mustEqual expected
        dfBlank.select(st_asGeoJSON(st_geomFromWKT(geom))).first mustEqual expected
      }
    }

    "st_asLatLonText" >> {
      sc.sql("select st_asLatLonText(null)").collect.head(0) must beNull
      import org.apache.spark.sql.functions.col
      val gf = new GeometryFactory()
      val df = sc.createDataset(Seq(gf.createPoint(new Coordinate(-76.5, 38.5)))).toDF()
      val r = df.select(st_asLatLonText(col("value")))

      r.collect().head mustEqual """38°30'0.000"N 77°30'0.000"W"""
    }

    "st_asText" >> {
      sc.sql("select st_asText(null)").collect.head(0) must beNull
      dfBlank.select(st_asText(lit(null))).first must beNull

      val point = "POINT (-76.5 38.5)"
      val r = sc.sql(
        s"""
          |select st_asText(st_geomFromWKT('$point'))
        """.stripMargin
      )
      val expected = "POINT (-76.5 38.5)"
      r.collect().head.getAs[String](0) mustEqual expected

      dfBlank.select(st_asText(st_geomFromWKT(point))).first mustEqual expected
    }

    "st_geoHash" >> {
      sc.sql("select st_geoHash(null, null)").collect.head(0) must beNull
      dfBlank.select(st_geoHash(lit(null), lit(null))).first must beNull


      val point = "POINT (-76.5 38.5)"
      val precision = 25
      val r = sc.sql(
        s"""
          |select st_geoHash(st_geomFromWKT('$point'), $precision)
        """.stripMargin
      )

      val expected = "dqce5"


      r.collect().head.getAs[String](0) mustEqual expected
      dfBlank.select(st_geoHash(st_geomFromWKT(lit(point)), lit(precision))).first mustEqual expected

    }

    //after
    step {
      spark.stop()
    }
  }
}
