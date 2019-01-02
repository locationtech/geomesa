/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.util.{Map => JMap}

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.DataStoreFinder
import org.junit.runner.RunWith
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SparkSQLGeometricDistanceFunctionsTest extends Specification with LazyLogging {

  "sql geometric distance functions" should {
    import scala.collection.JavaConversions._

    sequential
    val dsParams: JMap[String, String] = Map("cqengine" -> "true", "geotools" -> "true")

    val ds = DataStoreFinder.getDataStore(dsParams)
    val spark = SparkSQLTestUtils.createSparkSession()
    val sc = spark.sqlContext

    SparkSQLTestUtils.ingestChicago(ds)

    val df = spark.read
      .format("geomesa")
      .options(dsParams)
      .option("geomesa.feature", "chicago")
      .load()
    logger.info(df.schema.treeString)
    df.createOrReplaceTempView("chicago")

    "st_aggregateDistanceSpheroid" >> {
      "should work with window functions" >> {
        val res = sc.sql(
          """
          |select
          |   case_number,dtg,st_aggregateDistanceSpheroid(l)
          |from (
          |  select
          |      case_number,
          |      dtg,
          |      collect_list(geom) OVER (PARTITION BY true ORDER BY dtg asc ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as l
          |  from chicago
          |)
          |where
          |   size(l) > 1
        """.stripMargin).
          collect().map(_.getDouble(2))
        Array(70681.00230533126, 141178.05958707482) must beEqualTo(res)
      }
    }

    "st_lengthSpheroid" >> {
      "should handle null" >> {
        sc.sql("select st_lengthSpheroid(null)").collect.head(0) must beNull
      }

      "should get great circle length of a linestring" >> {
        val res = sc.sql(
          """
          |select
          |  case_number,st_lengthSpheroid(st_makeLine(l))
          |from (
          |   select
          |      case_number,
          |      dtg,
          |      collect_list(geom) OVER (PARTITION BY true ORDER BY dtg asc ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) as l
          |   from chicago
          |)
          |where
          |   size(l) > 1
        """.stripMargin).
          collect().map(_.getDouble(1))
        Array(70681.00230533126, 141178.05958707482) must beEqualTo(res)
      }
    }

  }

}
