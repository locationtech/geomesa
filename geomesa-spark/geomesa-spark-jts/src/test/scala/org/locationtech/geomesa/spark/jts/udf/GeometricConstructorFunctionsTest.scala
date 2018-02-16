/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark.jts.udf

import com.vividsolutions.jts.geom._
import org.apache.spark.sql.functions._
import org.geotools.geometry.jts.JTS
import org.junit.runner.RunWith
import org.locationtech.geomesa.spark.jts._
import org.locationtech.geomesa.spark.jts.util.WKTUtils
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

      dfBlank.select(st_geomFromWKB(st_byteArray(new String(geomArr)))).first mustEqual expected
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
      val expected = JTS.toGeometry(new Envelope(0, 2, 0, 2))
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
      val expected = WKTUtils.read("POLYGON((0.0 0.0, 2.0 0.0, 2.0 2.0, 0.0 2.0, 0.0 0.0))")
      r.collect().head.getAs[Geometry](0) mustEqual expected

      dfBlank.select(st_makeBox2D(
        st_castToPoint(st_geomFromWKT("POINT(0 0)")),
        st_castToPoint(st_geomFromWKT("POINT(2 2)"))
      )).first mustEqual expected
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
    }

    "st_makePoint" >> {
      sc.sql("select st_makePoint(null, null)").collect.head(0) must beNull
      dfBlank.select(st_makePoint(lit(null), lit(null))).first must beNull

      val r = sc.sql(
        """
          |select st_makePoint(0, 0)
        """.stripMargin
      )
      val expected = WKTUtils.read("POINT(0 0)")
      r.collect().head.getAs[Point](0) mustEqual expected
      dfBlank.select(st_makePoint(0, 0)).first mustEqual expected
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
      dfBlank.select(st_pointFromWKB(st_byteArray(new String(pointArr)))).first mustEqual expected
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

    // after
    step {
      spark.stop()
    }
  }
}
