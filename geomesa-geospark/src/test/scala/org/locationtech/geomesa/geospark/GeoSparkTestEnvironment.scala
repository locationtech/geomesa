/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.geospark

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql._
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.{SparkConf, SparkContext}
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

/**
 * Common JTS test setup and utilities.
 */
trait GeoSparkTestEnvironment {
  implicit lazy val geospark: SparkSession = {
    SparkSession.builder()
      .appName("testGeoSpark")
      .master("local[*]")
      .config("spark.serializer",classOf[KryoSerializer].getName)
      .config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
      .master("local[*]").appName("myGeoSparkSQLdemo").getOrCreate()
  }

  GeoSparkSQLRegistrator.registerAll(geospark)

  val conf = new SparkConf()
  conf.setAppName("GeoSparkRunnableExample") // Change this to a proper name
  conf.setMaster("local[*]") // Delete this if run in cluster mode
  // Enable GeoSpark custom Kryo serializer
  conf.set("spark.serializer", classOf[KryoSerializer].getName)
  conf.set("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName)
  lazy val gsc = new SparkContext(conf)

  /**
   * Constructor for creating a DataFrame with a single row and no columns.
   * Useful for testing the invocation of data constructing UDFs.
   */
//  JTS VERSION
//  def dfBlank(implicit spark: SparkSession): DataFrame = {
//    // This is to enable us to do a single row creation select operation in DataFrame
//    // world. Probably a better/easier way of doing this.
//    spark.createDataFrame(spark.sparkContext.makeRDD(Seq(Row())), StructType(Seq.empty))
//  }
}
