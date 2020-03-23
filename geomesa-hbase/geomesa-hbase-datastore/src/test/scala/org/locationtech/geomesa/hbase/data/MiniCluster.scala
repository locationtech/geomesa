/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
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

case object MiniCluster extends LazyLogging {

  lazy val cluster: HBaseTestingUtility = {
    val cluster = new HBaseTestingUtility()
    logger.info("Starting embedded hbase")
    cluster.getConfiguration.set("hbase.superuser", "admin")
    cluster.getConfiguration.set(CoprocessorHost.USER_REGION_COPROCESSOR_CONF_KEY, HBaseIndexAdapter.CoprocessorClass)
    cluster.startMiniCluster(sys.props.get("geomesa.hbase.test.region.servers").map(_.toInt).getOrElse(2))
    logger.info("Started embedded hbase")
    cluster
  }

  lazy val connection: Connection = cluster.getConnection

  sys.addShutdownHook({
    logger.info("Stopping embedded hbase")
    // note: HBaseTestingUtility says don't close the connection
    // connection.close()
    cluster.shutdownMiniCluster()
    logger.info("Embedded HBase stopped")
  })
}
