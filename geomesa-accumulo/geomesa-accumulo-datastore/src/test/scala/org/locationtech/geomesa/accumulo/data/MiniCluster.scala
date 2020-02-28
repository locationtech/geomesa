/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.nio.file.Files

import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.minicluster.MiniAccumuloCluster
import org.locationtech.geomesa.utils.io.PathUtils

case object MiniCluster extends LazyLogging {

  private val miniClusterTempDir = Files.createTempDirectory("gm-mini-acc")

  val _username = "root"
  val _password = "admin"

  lazy val cluster: MiniAccumuloCluster = {
    logger.info(s"Starting Accumulo minicluster at $miniClusterTempDir")
    val cluster = new MiniAccumuloCluster(miniClusterTempDir.toFile, _password)
    cluster.start()
    logger.info("Started Accmulo minicluster")
    cluster
  }

  def getConnector(
      username: String = _username,
      password: String = _password
  ): Connector = {
    cluster.getConnector(username, password)
  }

  lazy val getClusterParams: Map[String, String] = Map(
    AccumuloDataStoreParams.InstanceIdParam.key -> cluster.getInstanceName,
    AccumuloDataStoreParams.ZookeepersParam.key -> cluster.getZooKeepers,
    AccumuloDataStoreParams.UserParam.key -> _username,
    AccumuloDataStoreParams.PasswordParam.key -> _password
  )

  sys.addShutdownHook({
    logger.info("Stopping Accumulo minicluster")
    cluster.stop()
    PathUtils.deleteRecursively(miniClusterTempDir)
    logger.info("Stopped Accumulo minicluster")
  })
}
