/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark.sql

import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.locationtech.geomesa.spark.GeoMesaSparkKryoRegistrator
import org.specs2.mutable.SpecificationWithJUnit

trait TestWithSpark extends SpecificationWithJUnit with LazyLogging {

  import org.locationtech.geomesa.spark.jts._

  lazy val spark: SparkSession = TestWithSpark.spark
  lazy val sc: SQLContext = spark.sqlContext.withJTS // <-- withJTS should be a noop given the above, but is here to test that code path
  lazy val sparkContext: SparkContext = spark.sparkContext

  /**
   * Constructor for creating a DataFrame with a single row and no columns.
   * Useful for testing the invocation of data constructing UDFs.
   */
  def dfBlank(): DataFrame = {
    // This is to enable us to do a single row creation select operation in DataFrame
    // world. Probably a better/easier way of doing this.
    spark.createDataFrame(spark.sparkContext.makeRDD(Seq(Row())), StructType(Seq.empty))
  }
}

object TestWithSpark {

  import org.locationtech.geomesa.spark.jts._

  lazy val spark: SparkSession =
    SparkSession.builder()
      .appName("test")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.kryo.registrator", classOf[GeoMesaSparkKryoRegistrator].getName)
      .config("spark.sql.crossJoin.enabled", "true")
      .config("spark.ui.enabled", value = false)
      .master(s"local[1]")
      .getOrCreate()
      .withJTS
}
