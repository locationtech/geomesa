/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.junit.runner.RunWith
import org.locationtech.geomesa.spark.jts._
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.locationtech.geomesa.spark.sql.DataFrameFunctions._
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner

@RunWith(classOf[JUnitRunner])
class DataFrameFunctionsTest extends Specification with LazyLogging with SpatialRelations {

  sequential

  var spark: SparkSession = _
  var dfBlank: DataFrame = _

  "DataFrame functions" should {

    // before
    step {
      spark = SparkSQLTestUtils.createSparkSession()
      val sc = spark.sqlContext
      sc.withJTS
      dfBlank = spark.createDataFrame(spark.sparkContext.makeRDD(Seq(Row())), StructType(Seq.empty))
    }

    "st_transform" >> {
      val pointWGS84 = "POINT(-0.871722 52.023636)"
      val expectedOSGB36 = WKTUtils.read("POINT(477514.0081191745 236736.03179981868)")
      val transf = dfBlank.select(st_transform(st_geomFromWKT(pointWGS84), lit("EPSG:4326"), lit("EPSG:27700"))).first
      transf must not(throwAn[Exception])
      transf mustEqual expectedOSGB36
    }

    // after
    step {
      spark.stop()
    }
  }
}
