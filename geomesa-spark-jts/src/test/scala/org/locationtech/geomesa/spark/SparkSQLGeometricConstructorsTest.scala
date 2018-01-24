/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark


import com.vividsolutions.jts.geom._
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.jts.JTSTypes
import org.geotools.geometry.jts.JTS
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkSQLGeometricConstructorsTest extends Specification {

  "sql geometry constructors" should {
    sequential

    var spark: SparkSession = null
    var sc: SQLContext = null

    // before
    step {
      spark = SparkSession.builder()
        .appName("testSpark")
        .master("local[*]")
        .getOrCreate()
      sc = spark.sqlContext
      JTSTypes.init(sc)
    }

    "st_geomFromWKT" >> {
      sc.sql("select st_geomFromWKT(null)").collect.head(0) must beNull

      val r = sc.sql(
        """
          |select st_geomFromWKT('POINT(0 0)')
        """.stripMargin
      )

      r.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POINT(0 0)")
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
      r.collect().head.getAs[Geometry](0) mustEqual WKTUtils.read("POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))")
    }

    "st_lineFromText" >> {
      sc.sql("select st_lineFromText(null)").collect.head(0) must beNull

      val r = sc.sql(
        """
          |select st_lineFromText('LINESTRING(0 0, 1 1, 2 2)')
        """.stripMargin
      )
      r.collect().head.getAs[LineString](0) mustEqual WKTUtils.read("LINESTRING(0 0, 1 1, 2 2)")
    }

    "st_makePolygon" >> {
      sc.sql("select st_makePolygon(null)").collect.head(0) must beNull

      val r = sc.sql(
        s"""
           |select st_makePolygon(st_castToLineString(
           |    st_geomFromWKT('LINESTRING(0 0, 2 2, 5 4, 7 2, 5 2, 3 0, 0 0)')))
        """.stripMargin
      )
      r.collect().head.getAs[Polygon](0) mustEqual WKTUtils.read("POLYGON((0 0, 2 2, 5 4, 7 2, 5 2, 3 0, 0 0))")
    }

    "st_makePoint" >> {
      sc.sql("select st_makePoint(null, null)").collect.head(0) must beNull

      val r = sc.sql(
        """
          |select st_makePoint(0, 0)
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_makePointM" >> {
      sc.sql("select st_makePointM(null, null, null)").collect.head(0) must beNull

      val r = sc.sql(
        """
          |select st_makePointM(0, 0, 1)
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0 1)")
    }

    "st_mLineFromText" >> {
      sc.sql("select st_mLineFromText(null)").collect.head(0) must beNull

      val r = sc.sql(
        """
          |select st_mLineFromText('MULTILINESTRING((0 0, 1 1, 2 2), (0 1, 1 2, 2 3))')
        """.stripMargin
      )

      r.collect().head.getAs[MultiLineString](0) mustEqual WKTUtils.read("MULTILINESTRING((0 0, 1 1, 2 2), " +
        "(0 1, 1 2, 2 3))")
    }

    "st_mPointFromText" >> {
      sc.sql("select st_mPointFromText(null)").collect.head(0) must beNull

      val r = sc.sql(
        """
          |select st_mPointFromText('MULTIPOINT((0 0), (1 1))')
        """.stripMargin
      )

      r.collect().head.getAs[MultiPoint](0) mustEqual WKTUtils.read("MULTIPOINT((0 0), (1 1))")
    }

    "st_mPolyFromText" >> {
      sc.sql("select st_mPolyFromText(null)").collect.head(0) must beNull

      val r = sc.sql(
        """
          |select st_mPolyFromText('MULTIPOLYGON((( -1 -1, 0 1, 1 -1, -1 -1 )),((-4 4, 4 4, 4 -4, -4 -4, -4 4),
          |                                    (2 2, -2 2, -2 -2, 2 -2, 2 2)))')
        """.stripMargin
      )

      r.collect().head.getAs[MultiPolygon](0) mustEqual
        WKTUtils.read("MULTIPOLYGON((( -1 -1, 0 1, 1 -1, -1 -1 ))," +
          "((-4 4, 4 4, 4 -4, -4 -4, -4 4),(2 2, -2 2, -2 -2, 2 -2, 2 2)))")
    }

    "st_point" >> {
      sc.sql("select st_point(null, null)").collect.head(0) must beNull

      val r = sc.sql(
        """
          |select st_point(0, 0)
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_pointFromText" >> {
      sc.sql("select st_pointFromText(null)").collect.head(0) must beNull
      val r = sc.sql(
        """
          |select st_pointFromText('Point(0 0)')
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_pointFromWKB" >> {
      sc.sql("select st_pointFromWKB(null)").collect.head(0) must beNull
      val pointArr = Array[Byte](0, 0, 0, 0, 1,
        0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0)
      val r = sc.sql(
        s"""
           |select st_pointFromWKB(st_byteArray('${new String(pointArr)}'))
        """.stripMargin
      )
      r.collect().head.getAs[Point](0) mustEqual WKTUtils.read("POINT(0 0)")
    }

    "st_polygon" >> {
      sc.sql("select st_polygon(null)").collect.head(0) must beNull
      val r = sc.sql(
        s"""
           |select st_polygon(st_castToLineString(
           |    st_geomFromWKT('LINESTRING(0 0, 2 2, 5 2, 3 0, 0 0)')))
        """.stripMargin
      )
      r.collect().head.getAs[Polygon](0) mustEqual WKTUtils.read("POLYGON((0 0, 2 2, 5 2, 3 0, 0 0))")
    }

    "st_polygonFromText" >> {
      sc.sql("select st_polygonFromText(null)").collect.head(0) must beNull
      val r = sc.sql(
        """
          |select st_polygonFromText('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))')
        """.stripMargin
      )
      r.collect().head.getAs[Polygon](0) mustEqual WKTUtils.read("POLYGON((0.0 0.0, 2.0 0.0, " +
        "2.0 2.0, 0.0 2.0, 0.0 0.0))")
    }

    // after
    step {
      spark.stop()
    }
  }
}
