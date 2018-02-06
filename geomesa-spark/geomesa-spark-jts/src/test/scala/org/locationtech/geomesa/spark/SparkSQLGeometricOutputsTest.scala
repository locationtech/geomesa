/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark


import com.vividsolutions.jts.geom.GeometryFactory
import com.vividsolutions.jts.geom.Coordinate
import org.apache.spark.sql._
import org.apache.spark.sql.jts.JTSTypes
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class SparkSQLGeometricOutputsTest extends Specification {

  "sql geometry constructors" should {
    sequential

    var spark: SparkSession = null
    var sc: SQLContext = null
    var df: DataFrame = null

    // before
    step {
      spark = SparkSession.builder()
        .appName("testSpark")
        .master("local[*]")
        .getOrCreate()
      sc = spark.sqlContext
      JTSTypes.init(sc)
    }

    "st_asBinary" >> {
      sc.sql("select st_asBinary(null)").collect.head(0) must beNull

      val r = sc.sql(
        """
          |select st_asBinary(st_geomFromWKT('POLYGON((0 0, 2 0, 2 2, 0 2, 0 0))'))
        """.stripMargin
      )
      r.collect().head.getAs[Array[Byte]](0) mustEqual Array[Byte](0, 0, 0, 0, 3, 0, 0, 0, 1, 0, 0, 0, 5, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0,
        64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0
      )
    }

    "st_asGeoJSON" >> {
      "null" >> {
        sc.sql("select st_asGeoJSON(null)").collect.head(0) must beNull
      }

      "point" >> {
        val r = sc.sql(
          """
            |select st_asGeoJSON(st_geomFromWKT('POINT(0 0)'))
          """.stripMargin
        )
        r.collect().head.getAs[String](0) mustEqual
          """{"type":"Point","coordinates":[0.0,0.0]}"""
      }

      "lineString" >> {
        val r = sc.sql(
          """
            |select st_asGeoJSON(st_geomFromWKT('LINESTRING(0 0, 1 1, 2 2)'))
          """.stripMargin
        )
        r.collect().head.getAs[String](0) mustEqual
          """{"type":"LineString","coordinates":[[0.0,0.0],[1,1],[2,2]]}"""
      }

      "polygon" >> {
        val r = sc.sql(
          """
            |select st_asGeoJSON(st_geomFromWKT('POLYGON((0.45 0.75, 1.15 0.75, 1.15 1.45, 0.45 1.45, 0.45 0.75))'))
          """.stripMargin
        )
        r.collect().head.getAs[String](0) mustEqual
          """{"type":"Polygon","coordinates":[[[0.45,0.75],[1.15,0.75],[1.15,1.45],[0.45,1.45],[0.45,0.75]]]}"""

      }

      "multiPoint" >> {
        val r = sc.sql(
          """
            |select st_asGeoJSON(st_geomFromWKT('MULTIPOINT((0 0), (1 1))'))
          """.stripMargin
        )
        r.collect().head.getAs[String](0) mustEqual
          """{"type":"MultiPoint","coordinates":[[0.0,0.0],[1,1]]}"""
      }

      "multiLineString" >> {
        val r = sc.sql(
          """
            |select st_asGeoJSON(st_geomFromWKT('MULTILINESTRING((0 0, 1 1, 2 2), (-3 -3, -2 -2, -1 -1))'))
          """.stripMargin
        )
        r.collect().head.getAs[String](0) mustEqual
          """{"type":"MultiLineString","coordinates":[[[0.0,0.0],[1,1],[2,2]],[[-3,-3],[-2,-2],[-1,-1]]]}"""
      }

      "multiPolygon" >> {
        val r = sc.sql(
          """
            |select st_asGeoJSON(st_geomFromWKT('MULTIPOLYGON(((0.45 0.75, 1.15 0.75, 1.15 1.45, 0.45 1.45, 0.45 0.75))
            |,((0 0, 1 0, 1 1, 0 1, 0 0)))'))
          """.stripMargin
        )
        r.collect().head.getAs[String](0) mustEqual
          """{"type":"MultiPolygon","coordinates":[[[[0.45,0.75],[1.15,0.75],[1.15,1.45],[0.45,1.45],""" +
            """[0.45,0.75]]],[[[0.0,0.0],[1,0.0],[1,1],[0.0,1],[0.0,0.0]]]]}"""
      }

      "geometryCollection" >> {
        val r =sc.sql(
          """
            |select st_asGeoJSON(st_geomFromWKT('GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1, 2 2))'))
          """.stripMargin
        )
        r.collect().head.getAs[String](0) mustEqual
          """{"type":"GeometryCollection","geometries":[{"type":"Point","coordinates":[0.0,0.0]},""" +
            """{"type":"LineString","coordinates":[[0.0,0.0],[1,1],[2,2]]}]}"""
      }
    }

    "st_asLatLonText" >> {
      sc.sql("select st_asLatLonText(null)").collect.head(0) must beNull
      import SQLGeometricOutputFunctions.st_asLatLonText
      import org.apache.spark.sql.functions.col
      import org.locationtech.geomesa.spark.SpatialEncoders.jtsPointEncoder
      val gf = new GeometryFactory()
      val df = sc.createDataset(Seq(gf.createPoint(new Coordinate(-76.5, 38.5)))).toDF()
      val r = df.select(st_asLatLonText(col("value")))

      r.collect().head mustEqual """38°30'0.000"N 77°30'0.000"W"""
    }

    "st_asText" >> {
      sc.sql("select st_asText(null)").collect.head(0) must beNull

      val r = sc.sql(
        """
          |select st_asText(st_geomFromWKT('POINT (-76.5 38.5)'))
        """.stripMargin
      )
      r.collect().head.getAs[String](0) mustEqual "POINT (-76.5 38.5)"
    }

    //after
    step {
      spark.stop()
    }
  }
}
