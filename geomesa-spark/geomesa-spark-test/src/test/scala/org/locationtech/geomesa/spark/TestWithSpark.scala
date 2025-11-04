/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.spark

import com.typesafe.scalalogging.StrictLogging
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.geomesa.testcontainers.spark.SparkCluster
import org.locationtech.geomesa.utils.io.CloseWithLogging
import org.specs2.mutable.SpecificationWithJUnit
import org.specs2.specification.BeforeAfterAll
import org.testcontainers.containers.Network

trait TestWithSpark extends SpecificationWithJUnit with BeforeAfterAll with StrictLogging {

  import org.locationtech.geomesa.spark.jts._

  val network = Network.newNetwork()
  val cluster = new SparkCluster().withNetwork(network)

  // TODO enforce only a single instance at once
  lazy val spark: SparkSession = cluster.getOrCreateSession().withJTS
  lazy val sc: SQLContext = spark.sqlContext.withJTS // <-- withJTS should be a noop given the above, but is here to test that code path
  lazy val sparkContext: SparkContext = spark.sparkContext

  override def beforeAll(): Unit = cluster.start()

  override def afterAll(): Unit = CloseWithLogging(cluster)

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
