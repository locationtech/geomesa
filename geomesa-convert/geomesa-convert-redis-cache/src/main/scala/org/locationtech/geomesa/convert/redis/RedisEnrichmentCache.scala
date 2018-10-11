/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.convert.redis

import java.io.Closeable
import java.util.concurrent.TimeUnit

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.typesafe.config.Config
import org.locationtech.geomesa.convert.{EnrichmentCache, EnrichmentCacheFactory}
import redis.clients.jedis.{Jedis, JedisPool}

import scala.util.Try

trait RedisConnectionBuilder extends Closeable {
  def getConn: Jedis
}

class RedisEnrichmentCache(jedisPool: RedisConnectionBuilder,
                           expiration: Long = -1,
                           localCache: Boolean) extends EnrichmentCache {
  type KV = java.util.Map[String, String]

  private val builder =
    if (expiration > 0) {
      CacheBuilder.newBuilder().expireAfterWrite(expiration, TimeUnit.MILLISECONDS)
    } else {
      if (!localCache) {
        CacheBuilder.newBuilder().expireAfterWrite(0, TimeUnit.MILLISECONDS).maximumSize(0)
      } else {
        CacheBuilder.newBuilder()
      }
    }

  private val cache: LoadingCache[String, KV] =
    builder
      .build(new CacheLoader[String, KV] {
        override def load(k: String): KV = {
          val conn = jedisPool.getConn
          try {
            conn.hgetAll(k)
          } finally {
            // Note: for a JedisPool this only returns it to the pool instead of actionally
            // closing the connection (so it's safe to call close() on the conn)
            conn.close()
          }
        }
      })

  override def get(args: Array[String]): Any = cache.get(args(0)).get(args(1))
  override def put(args: Array[String], value: Any): Unit = ???
  override def clear(): Unit = ???
  override def close(): Unit = jedisPool.close()
}

class RedisEnrichmentCacheFactory extends EnrichmentCacheFactory {
  override def canProcess(conf: Config): Boolean = conf.hasPath("type") && conf.getString("type").equals("redis")

  override def build(conf: Config): EnrichmentCache = {
    val Array(host, port) = parseRedisURL(conf.getString("redis-url"))
    val timeout = if (conf.hasPath("expiration")) conf.getLong("expiration") else -1
    val connBuilder: RedisConnectionBuilder = new RedisConnectionBuilder {
      private val pool = new JedisPool(host, port.toInt)
      override def getConn: Jedis = pool.getResource
      override def close(): Unit = pool.close()
    }

    val localCache = Try(conf.getBoolean("local-cache")).getOrElse(true)
    new RedisEnrichmentCache(connBuilder, timeout, localCache)
  }

  private def parseRedisURL(url: String) = {
    if (url.indexOf(":") != -1) url.split(":")
    else Array(url, "6379")
  }
}
