/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.spark

import java.{lang => jl}

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, TypedColumn, _}
import org.junit.runner.RunWith
import org.locationtech.geomesa.spark.jts._
import org.locationtech.geomesa.spark.jts.util.WKTUtils
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.locationtech.geomesa.spark.DataFrameFunctions._

@RunWith(classOf[JUnitRunner])
class DataFrameFunctionsTest extends Specification with LazyLogging with SpatialRelations {
  type DFRelation = (Column, Column) => TypedColumn[Any, jl.Boolean]
  sequential

  "DataFrame functions" should {

    val spark = SparkSQLTestUtils.createSparkSession()
    val sc = spark.sqlContext
    sc.withJTS

    var dfBlank: DataFrame = null

    // before
    step {
      dfBlank = spark.createDataFrame(spark.sparkContext.makeRDD(Seq(Row())), StructType(Seq.empty))
    }

    "st_transform" >> {
      val pointWGS84 = "POINT(-0.871722 52.023636)"
      val expectedOSGB36 = WKTUtils.read("POINT(477514.0081191745 236736.03179982008)")
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
