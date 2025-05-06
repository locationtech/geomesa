/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.executors.CommandExecutor
import redis.clients.jedis.{Connection, HostAndPort, JedisClientConfig, UnifiedJedis}

import java.time.Duration
import scala.collection.JavaConverters._

/**
 * A subclass of JedisCluster that overrides the close method to prevent closing the cluster connection.
 */
class JedisClusterUncloseable(
    clusterNodes: java.util.Set[HostAndPort],
    clientConfig: JedisClientConfig,
    objectPoolConfig: GenericObjectPoolConfig[Connection],
    maxAttempts: Int,
    maxTotalRetriesDuration: Duration
  ) extends UnifiedJedis(clusterNodes, clientConfig, objectPoolConfig, maxAttempts, maxTotalRetriesDuration) {

  /**
   * Override the close method to prevent closing the JedisCluster instance.
   * This is intentional to avoid closing the cluster connection.
   */
  override def close(): Unit = {
    // Do not call the superclass close method
    // super.close()
  }

  private[data] def closePool(): Unit = {
    super.close()
  }
}
