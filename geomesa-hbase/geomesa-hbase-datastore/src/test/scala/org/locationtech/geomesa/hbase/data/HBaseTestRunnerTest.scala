/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.hbase.HBaseTestingUtility
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost
import org.junit.runner.RunWith
import org.locationtech.geomesa.hbase.coprocessor.GeoMesaCoprocessor
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.BeforeAfterAll
import org.specs2.specification.core.{Env, Fragments}

/**
  * Base class for running all hbase embedded tests
  */
@RunWith(classOf[JUnitRunner])
class HBaseTestRunnerTest extends Specification with BeforeAfterAll with LazyLogging {

  sequential

  var cluster: HBaseTestingUtility = new HBaseTestingUtility()
  var connection: Connection = _

  // add new tests here
  val specs = Seq(
    new HBaseAlterSchemaTest,
    new HBaseArrowTest,
    new HBaseBinAggregatorTest,
    new HBaseColumnGroupsTest,
    new HBaseDataStoreTest,
    new HBaseDensityFilterTest,
    new HBaseStatsAggregatorTest,
    new HBaseVisibilityTest,
    new HBasePartitioningTest,
    new HBaseS2IndexTest,
    new HBaseS3IndexTest,
    new HBaseBackCompatibilityTest,
    new HBaseSamplingFilterTest
  )

  override def beforeAll(): Unit = {
    logger.info("Starting embedded hbase")
    cluster.getConfiguration.set("hbase.superuser", "admin")
    cluster.getConfiguration.set(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY,
      classOf[GeoMesaCoprocessor].getCanonicalName)
    cluster.startMiniCluster(1)
    connection = cluster.getConnection
    logger.info("Started embedded hbase")
    specs.foreach { s => s.cluster = cluster; s.connection = connection }
  }

  override def map(fs: => Fragments, env: Env): Fragments =
     specs.foldLeft(super.map(fs, env))((fragments, spec) => fragments ^ spec.fragments(env))

  override def afterAll(): Unit = {
    logger.info("Stopping embedded hbase")
    // note: HBaseTestingUtility says don't close the connection
    // connection.close()
    cluster.shutdownMiniCluster()
    logger.info("Embedded HBase stopped")
  }
}

trait HBaseTest extends Specification {
  var cluster: HBaseTestingUtility = _
  var connection: Connection = _
  val catalogTableName = "hbasetest"
}
