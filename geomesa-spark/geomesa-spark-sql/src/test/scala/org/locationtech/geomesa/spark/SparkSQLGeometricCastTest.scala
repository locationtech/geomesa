/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.util.{Map => JMap}

import com.typesafe.scalalogging.LazyLogging
import com.vividsolutions.jts.geom.{LineString, Polygon, Point, Geometry}
import org.apache.spark.sql._
import org.geotools.data.{DataStoreFinder, DataStore}
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.interop.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class SparkSQLGeometricCastTest extends Specification with LazyLogging {

  "sql geometry accessors" should {
    sequential

    val dsParams: JMap[String, String] = Map("cqengine" -> "true", "geotools" -> "true")
    var ds: DataStore = null
    var spark: SparkSession = null
    var sc: SQLContext = null

    // before
    step {
      ds = DataStoreFinder.getDataStore(dsParams)
      spark = SparkSQLTestUtils.createSparkSession()
      sc = spark.sqlContext
      SQLTypes.init(sc)
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
      ds.dispose()
      spark.stop()
    }
  }
}