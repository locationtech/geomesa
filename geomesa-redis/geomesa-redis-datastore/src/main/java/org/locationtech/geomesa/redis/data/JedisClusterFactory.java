/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Connection;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisClientConfig;

import java.time.Duration;
import java.util.Set;

/**
 * A factory for creating and managing pooled JedisCluster instances.
 * This factory handles the lifecycle of JedisCluster objects within a pool.
 */
public class JedisClusterFactory extends BasePooledObjectFactory<JedisCluster> {

    private static final Logger logger = LoggerFactory.getLogger(JedisClusterFactory.class);
    private final Set<HostAndPort> clusterNodes;
    private final JedisClientConfig clientConfig;
    private final GenericObjectPoolConfig<Connection> connectionPoolConfig;
    private final int maxAttempts;
    private final Duration maxTotalRetriesDuration;

    /**
     * Creates a new factory with the specified configuration.
     *
     * @param clusterNodes The set of Redis cluster nodes to connect to
     * @param clientConfig The configuration for Jedis client
     * @param connectionPoolConfig The pool configuration for internal connection pools
     * @param maxAttempts The maximum number of attempts for command execution
     * @param maxTotalRetriesDuration The maximum duration for retry attempts
     */
    public JedisClusterFactory(
            Set<HostAndPort> clusterNodes,
            JedisClientConfig clientConfig,
            GenericObjectPoolConfig<Connection> connectionPoolConfig,
            int maxAttempts,
            Duration maxTotalRetriesDuration) {
        this.clusterNodes = clusterNodes;
        this.clientConfig = clientConfig;
        this.connectionPoolConfig = connectionPoolConfig;
        this.maxAttempts = maxAttempts;
        this.maxTotalRetriesDuration = maxTotalRetriesDuration;
    }

    /**
     * Simplified constructor with default values.
     *
     * @param clusterNodes The set of Redis cluster nodes to connect to
     * @param clientConfig The configuration for Jedis client
     * @param connectionPoolConfig The pool configuration for internal connection pools
     */
    public JedisClusterFactory(
            Set<HostAndPort> clusterNodes,
            JedisClientConfig clientConfig,
            GenericObjectPoolConfig<Connection> connectionPoolConfig) {
        this(clusterNodes, clientConfig, connectionPoolConfig, JedisCluster.DEFAULT_MAX_ATTEMPTS,
                Duration.ofMillis((long) clientConfig.getSocketTimeoutMillis() * JedisCluster.DEFAULT_MAX_ATTEMPTS));
    }

    /**
     * Creates a new JedisCluster instance.
     *
     * @return A new JedisCluster instance
     */
    @Override
    public JedisCluster create() {
        return new JedisCluster(
                clusterNodes,
                clientConfig,
                maxAttempts,
                maxTotalRetriesDuration,
                connectionPoolConfig);
    }

    /**
     * Wraps a JedisCluster instance in a PooledObject.
     *
     * @param jedisCluster The JedisCluster instance to wrap
     * @return A PooledObject wrapping the JedisCluster
     */
    @Override
    public PooledObject<JedisCluster> wrap(JedisCluster jedisCluster) {
        return new DefaultPooledObject<>(jedisCluster);
    }

    /**
     * Ensures that a pooled JedisCluster is ready for use.
     * This method is called when an object is borrowed from the pool.
     *
     * @param pooledJedisCluster The pooled JedisCluster to activate
     */
    @Override
    public void activateObject(PooledObject<JedisCluster> pooledJedisCluster) throws Exception {
        // JedisCluster is stateless, so no activation is needed
    }

    /**
     * Prepares a pooled JedisCluster to be returned to the pool.
     * This method is called when an object is returned to the pool.
     *
     * @param pooledJedisCluster The pooled JedisCluster to passivate
     */
    @Override
    public void passivateObject(PooledObject<JedisCluster> pooledJedisCluster) throws Exception {
        // JedisCluster is stateless, so no passivation is needed
    }

    /**
     * Destroys a pooled JedisCluster instance.
     * This method is called when an object is being removed from the pool.
     *
     * @param pooledJedisCluster The pooled JedisCluster to destroy
     */
    @Override
    public void destroyObject(PooledObject<JedisCluster> pooledJedisCluster) throws Exception {
        JedisCluster jedisCluster = pooledJedisCluster.getObject();
        if (jedisCluster != null) {
            jedisCluster.close();
        }
    }

    /**
     * Validates a pooled JedisCluster instance.
     * This method is called to check if an object is still valid in the pool.
     *
     * @param pooledJedisCluster The pooled JedisCluster to validate
     * @return true if the JedisCluster is valid, false otherwise
     */
    @Override
    public boolean validateObject(PooledObject<JedisCluster> pooledJedisCluster) {
        try {
            JedisCluster jedisCluster = pooledJedisCluster.getObject();
            // execute a query, we dont care about results, only if it throws an exception
            jedisCluster.exists("test-key");
            return true;
        } catch (Exception e) {
            logger.error("Failed to validate JedisCluster connection", e);
            return false;
        }
    }
}