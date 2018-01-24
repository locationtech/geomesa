/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark


import com.vividsolutions.jts.geom.{LineString, Point, Polygon}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.jts.JTSTypes
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner


@RunWith(classOf[JUnitRunner])
class SparkSQLGeometricCastTest extends Specification {

  "sql geometry accessors" should {
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

    "st_castToPoint" >> {
      "null" >> {
        sc.sql("select st_castToPoint(null)").collect.head(0) must beNull
      }

      "point" >> {
        val point = "st_geomFromWKT('POINT(1 1)')"
        val df = sc.sql(s"select st_castToPoint($point)")
        df.collect.head(0) must haveClass[Point]
      }
    }

    "st_castToPolygon" >> {
      "null" >> {
        sc.sql("select st_castToPolygon(null)").collect.head(0) must beNull
      }

      "polygon" >> {
        val polygon = "st_geomFromWKT('POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))')"
        val df = sc.sql(s"select st_castToPolygon($polygon)")
        df.collect.head(0) must haveClass[Polygon]
      }
    }

    "st_castToLineString" >> {
      "null" >> {
        sc.sql("select st_castToLineString(null)").collect.head(0) must beNull
      }

      "linestring" >> {
        val line = "st_geomFromWKT('LINESTRING(1 1, 2 2)')"
        val df = sc.sql(s"select st_castToLineString($line)")
        df.collect.head(0) must haveClass[LineString]
      }
    }

    "st_bytearray" >> {
      "null" >> {
        sc.sql("select st_bytearray(null)").collect.head(0) must beNull
      }

      "bytearray" >> {
        val df = sc.sql(s"select st_bytearray('foo')")
        df.collect.head(0) mustEqual "foo".toArray.map(_.toByte)
      }
    }

    // after
    step {
      spark.stop()
    }
  }
}