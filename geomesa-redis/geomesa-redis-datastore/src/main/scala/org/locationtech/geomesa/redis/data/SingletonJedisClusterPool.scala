/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data

import org.apache.commons.pool2.{PooledObject, PooledObjectFactory}
import redis.clients.jedis.executors.CommandExecutor
import redis.clients.jedis.util.Pool
import redis.clients.jedis.{HostAndPort, JedisClientConfig, JedisCluster, UnifiedJedis}

import java.time.Duration;

/**
 * Noop factory for JedisCluster. This is used to create a singleton pool for Geomesa compatibility.
 */
class NoopClusterFactory extends PooledObjectFactory[UnifiedJedis] {
  override def activateObject(p: PooledObject[UnifiedJedis]): Unit = {}

  override def destroyObject(p: PooledObject[UnifiedJedis]): Unit = {}

  override def passivateObject(p: PooledObject[UnifiedJedis]): Unit = {}

  override def validateObject(p: PooledObject[UnifiedJedis]): Boolean = true

  override def makeObject(): PooledObject[UnifiedJedis] = null
}

/**
 * Singleton pool for JedisCluster. This is used to create a singleton pool for Geomesa compatibility.
 * @param nodes
 * @param clientConfig
 */
class SingletonJedisClusterPool(nodes: java.util.Set[HostAndPort], clientConfig: JedisClientConfig)
    extends Pool[UnifiedJedis](new NoopClusterFactory) {

  private val cluster = new JedisClusterUncloseable(
    nodes,
    clientConfig,
    JedisCluster.DEFAULT_MAX_ATTEMPTS,
    Duration.ofMillis(clientConfig.getSocketTimeoutMillis * JedisCluster.DEFAULT_MAX_ATTEMPTS)
  )

  override def getResource: UnifiedJedis = cluster

  override def returnResource(resource: UnifiedJedis): Unit = {
    // Do nothing - we're always returning the same instance
  }

  override def returnBrokenResource(resource: UnifiedJedis): Unit = {
    // Since JedisCluster handles its own connection recovery, we don't need to do anything here
  }

  override def close(): Unit = {
    // `JedisClusterUncloseable` has disabled `close` but we can obtain a reference to the `executor` field
    // and call `close` on it, which is what UnifiedJedis' close method does
    // we need to do this when the pool itself is closed
    val executorField = classOf[UnifiedJedis].getDeclaredField("executor")
    executorField.setAccessible(true)
    executorField.get(cluster).asInstanceOf[CommandExecutor].close()
    // close the pool itself
    super.close()
  }
}