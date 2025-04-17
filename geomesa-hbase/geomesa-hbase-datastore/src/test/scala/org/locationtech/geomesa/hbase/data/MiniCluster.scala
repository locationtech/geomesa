/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
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

import scala.util.Try

case object MiniCluster extends LazyLogging {

  private lazy val tryCluster: Try[HBaseTestingUtility] = Try {
    val cluster = new HBaseTestingUtility()
    logger.info("Starting embedded hbase")
    cluster.getConfiguration.set("hbase.superuser", "admin")
    cluster.getConfiguration.set(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY, HBaseIndexAdapter.CoprocessorClass)
    Option("zookeeper.jmx.log4j.disable").foreach(key => sys.props.get(key).foreach(cluster.getConfiguration.set(key, _)))
    cluster.startMiniCluster(sys.props.get("geomesa.hbase.test.region.servers").map(_.toInt).getOrElse(2))
    logger.info("Started embedded hbase")
    cluster
  }

  lazy val cluster: HBaseTestingUtility = tryCluster.get
  lazy val connection: Connection = cluster.getConnection

  sys.addShutdownHook({
    logger.info("Stopping embedded hbase")
    // note: HBaseTestingUtility says don't close the connection
    // connection.close()
    cluster.shutdownMiniCluster()
    logger.info("Embedded HBase stopped")
  })
}
