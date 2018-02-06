/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark


import com.vividsolutions.jts.geom._
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.jts.JTSTypes
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import SQLGeometricCastFunctions._
import SQLGeometricConstructorFunctions._

@RunWith(classOf[JUnitRunner])
class SparkSQLGeometricCastTest extends Specification with BlankDataFrame {

  "sql geometry accessors" should {
    sequential

    implicit var spark: SparkSession = null
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

    "st_castToPoint" >> {
      "null" >> {
        sc.sql("select st_castToPoint(null)").collect.head(0) must beNull
        dfBlank.select(st_castToPoint(lit(null))).first must beNull
      }

      "point" >> {
        val pointTxt = "POINT(1 1)"
        val point = s"st_geomFromWKT('$pointTxt')"
        val df = sc.sql(s"select st_castToPoint($point)")
        df.collect.head(0) must haveClass[Point]
        dfBlank.select(st_castToPoint(st_geomFromWKT(pointTxt))).first must haveClass[Point]
      }
    }

    "st_castToPolygon" >> {
      "null" >> {
        sc.sql("select st_castToPolygon(null)").collect.head(0) must beNull
        dfBlank.select(st_castToPolygon(null: Geometry)).first must beNull
      }

      "polygon" >> {
        val polygonTxt = "POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))"
        val polygon = s"st_geomFromWKT('$polygonTxt')"
        val df = sc.sql(s"select st_castToPolygon($polygon)")
        df.collect.head(0) must haveClass[Polygon]
        dfBlank.select(st_castToPolygon(st_geomFromWKT(polygonTxt))).first must haveClass[Polygon]
      }
    }

    "st_castToLineString" >> {
      "null" >> {
        sc.sql("select st_castToLineString(null)").collect.head(0) must beNull
        dfBlank.select(st_castToLineString(null: Geometry)).first must beNull
      }

      "linestring" >> {
        val lineTxt = "LINESTRING(1 1, 2 2)"
        val line = s"st_geomFromWKT('$lineTxt')"
        val df = sc.sql(s"select st_castToLineString($line)")
        df.collect.head(0) must haveClass[LineString]
        dfBlank.select(st_castToLineString(st_geomFromWKT(lineTxt))).first must haveClass[LineString]
      }
    }

    "st_bytearray" >> {
      "null" >> {
        sc.sql("select st_byteArray(null)").collect.head(0) must beNull
        dfBlank.select(st_byteArray(null: String)).first must beNull
      }

      "bytearray" >> {
        val df = sc.sql(s"select st_byteArray('foo')")
        val expected = "foo".toArray.map(_.toByte)
        df.collect.head(0) mustEqual expected
        dfBlank.select(st_byteArray("foo")).first mustEqual expected
      }
    }

    // after
    step {
      spark.stop()
    }
  }
}
