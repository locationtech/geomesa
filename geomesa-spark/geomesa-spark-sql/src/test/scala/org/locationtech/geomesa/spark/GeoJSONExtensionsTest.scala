/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.locationtech.geomesa.utils.text.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class GeoJSONExtensionsTest extends Specification {

  import org.locationtech.geomesa.spark.jts._

  var spark: SparkSession = _

  // before
  step {
    spark = SparkSQLTestUtils.createSparkSession().withJTS
  }

  "GeoJSONExtensions" should {
    "convert points" >> {
      val s = spark

      import org.locationtech.geomesa.spark.sql.GeoJSONExtensions.GeoJSONDataFrame
      import s.implicits._

      val rows = Seq(("1", 1, WKTUtils.read("POINT(1 2)")))
      val df = spark.sparkContext.parallelize(rows).toDF("__fid__", "name", "geom")

      df.toGeoJSON.head() mustEqual
          """{"type":"Feature","geometry":{"type":"Point","coordinates":[1,2]},"properties":{"name":1},"id":"1"}"""
    }

    "convert polygons" >> {
      val s = spark

      import org.locationtech.geomesa.spark.sql.GeoJSONExtensions.GeoJSONDataFrame
      import s.implicits._

      val rows = Seq(("1", 1, WKTUtils.read("POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))")))
      val df = spark.sparkContext.parallelize(rows).toDF("__fid__", "name", "geom")

      df.toGeoJSON.head() mustEqual
          """{"type":"Feature","geometry":{"type":"Polygon","coordinates":""" +
              """[[[1,1],[1,2],[2,2],[2,1],[1,1]]]},"properties":{"name":1},"id":"1"}"""
    }

    "handle multiple rows" >> {
      val s = spark

      import org.locationtech.geomesa.spark.sql.GeoJSONExtensions.GeoJSONDataFrame
      import s.implicits._

      val rows = Seq(
        ("1", 1, WKTUtils.read("POINT(1 1)")),
        ("2", 2, WKTUtils.read("POINT(2 2)")),
        ("3", 3, WKTUtils.read("POINT(3 3)"))
      )

      val df = spark.sparkContext.parallelize(rows).toDF("__fid__", "name", "geom")

      df.toGeoJSON.collect mustEqual Array(1, 2, 3).map { i =>
        s"""{"type":"Feature","geometry":{"type":"Point","coordinates":[$i,$i]},"properties":{"name":$i},"id":"$i"}"""
      }
    }

    "handle rows with nulls" >> {
      val s = spark

      import org.locationtech.geomesa.spark.sql.GeoJSONExtensions.GeoJSONDataFrame
      import s.implicits._

      val rows = Seq(
        ("1", Int.box(1), WKTUtils.read("POINT(1 1)")),
        ("2", null, null),
        ("3", Int.box(3), WKTUtils.read("POINT(3 3)"))
      )

      val df = spark.sparkContext.parallelize(rows).toDF("__fid__", "name", "geom")

      df.toGeoJSON.collect mustEqual Array(1, 2, 3).map {
        case 2 => """{"type":"Feature","properties":{},"id":"2"}"""
        case i => s"""{"type":"Feature","geometry":{"type":"Point","coordinates":[$i,$i]},"properties":{"name":$i},"id":"$i"}"""
      }
    }
  }

  // after
  step {
    spark.stop()
  }
}
