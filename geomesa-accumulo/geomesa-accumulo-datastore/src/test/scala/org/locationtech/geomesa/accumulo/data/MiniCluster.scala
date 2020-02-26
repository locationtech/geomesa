/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import java.io.File
import com.google.common.io.Files
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams

case object MiniCluster extends LazyLogging {
  val username = "root"
  val password = "admin"

  lazy val cluster: MiniAccumuloCluster = {

    logger.info("Starting accumulo minicluster")
    val miniClusterTempDir: File = Files.createTempDir();
    logger.info(miniClusterTempDir.getAbsolutePath())
    val cluster = new MiniAccumuloCluster(miniClusterTempDir, password)

    cluster.start
    logger.info("Started accmulo minicluster")
    cluster
  }

  lazy val connector: Connector =
    cluster.getConnector(username, password)

  lazy val getClusterParams: Map[String, String] = Map(
    AccumuloDataStoreParams.InstanceIdParam.key -> cluster.getInstanceName,
    AccumuloDataStoreParams.ZookeepersParam.key -> cluster.getZooKeepers,
    AccumuloDataStoreParams.UserParam.key -> username,
    AccumuloDataStoreParams.PasswordParam.key -> password
  )

  sys.addShutdownHook({
    logger.info("Stopping accumulo minicluster")
    cluster.stop
    logger.info("Accumulo minicluster stopped")
  })
}
