/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
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

      val rows = Seq(("1", 1, WKTUtils.read("POINT(1 2)")), ("2", 2, WKTUtils.read("POINT(1 3)")))
      val df = spark.sparkContext.parallelize(rows).toDF("__fid__", "name", "geom")

      val result = df.toGeoJSON.collect()
      result must haveLength(2)
      result.head mustEqual
          """{"type":"Feature","id":"1","geometry":{"type":"Point","coordinates":[1,2]},"properties":{"name":1}}"""
      result.last mustEqual
          """{"type":"Feature","id":"2","geometry":{"type":"Point","coordinates":[1,3]},"properties":{"name":2}}"""
    }

    "convert polygons" >> {
      val s = spark

      import org.locationtech.geomesa.spark.sql.GeoJSONExtensions.GeoJSONDataFrame
      import s.implicits._

      val rows = Seq(("1", 1, WKTUtils.read("POLYGON((1 1, 1 2, 2 2, 2 1, 1 1))")))
      val df = spark.sparkContext.parallelize(rows).toDF("__fid__", "name", "geom")

      df.toGeoJSON.head() mustEqual
          """{"type":"Feature","id":"1","geometry":{"type":"Polygon","coordinates":""" +
              """[[[1,1],[1,2],[2,2],[2,1],[1,1]]]},"properties":{"name":1}}"""
    }

    "handle multiple rows" >> {
      val s = spark

      import org.locationtech.geomesa.spark.sql.GeoJSONExtensions.GeoJSONDataFrame
      import s.implicits._

      val rows = (1 to 10).map { i => (s"$i", i, WKTUtils.read(s"POINT($i $i)")) }

      val df = spark.sparkContext.parallelize(rows).toDF("__fid__", "name", "geom")

      df.toGeoJSON.collect mustEqual Array.range(1, 11).map { i =>
        s"""{"type":"Feature","id":"$i","geometry":{"type":"Point","coordinates":[$i,$i]},"properties":{"name":$i}}"""
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
        case 2 => """{"type":"Feature","id":"2","geometry":null,"properties":{"name":null}}"""
        case i => s"""{"type":"Feature","id":"$i","geometry":{"type":"Point","coordinates":[$i,$i]},"properties":{"name":$i}}"""
      }
    }
  }

  // after
  step {
    spark.stop()
  }
}
