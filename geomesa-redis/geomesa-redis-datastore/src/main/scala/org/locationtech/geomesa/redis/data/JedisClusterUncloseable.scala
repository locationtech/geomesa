/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data

import redis.clients.jedis.{HostAndPort, JedisClientConfig, UnifiedJedis}

import java.time.Duration
import scala.collection.JavaConverters._

/**
 * A subclass of JedisCluster that overrides the close method to prevent closing the cluster connection.
 */
class JedisClusterUncloseable(
    clusterNodes: java.util.Set[HostAndPort],
    clientConfig: JedisClientConfig,
    maxAttempts: Int,
    maxTotalRetriesDuration: Duration
  ) extends UnifiedJedis(clusterNodes, clientConfig, maxAttempts, maxTotalRetriesDuration) {

  /**
   * Override the close method to prevent closing the JedisCluster instance.
   * This is intentional to avoid closing the cluster connection.
   */
  override def close(): Unit = {
    // Do not call the superclass close method
    // super.close()
  }
}
