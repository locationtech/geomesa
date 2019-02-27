/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis

import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreParams
import org.locationtech.geomesa.security.SecurityParams
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import redis.clients.jedis.JedisPool

import scala.util.control.NonFatal

package object data extends LazyLogging {

  // keep lazy to allow for runtime setting of the underlying sys prop
  lazy val TransactionBackoffs: IndexedSeq[Int] = try { backoffs(RedisSystemProperties.TransactionBackoff.get) } catch {
    case NonFatal(_) =>
      logger.error(s"Invalid backoff for property '${RedisSystemProperties.TransactionBackoff.property}', using defaults")
      backoffs(RedisSystemProperties.TransactionBackoff.default)
  }

  object RedisDataStoreParams extends GeoMesaDataStoreParams with SecurityParams {
    // disable loose bbox by default, since we don't have any z-iterators
    override protected def looseBBoxDefault = false

    val RedisUrlParam       = new GeoMesaParam[String]("redis.url", "Redis connection URL. The URL can be used to specify the Redis database and credentials, if required - for example, 'redis://user:password@localhost:6379/1'", optional = false)
    val RedisCatalogParam   = new GeoMesaParam[String]("redis.catalog", "The name of the GeoMesa catalog table", optional = false)
    val PoolSizeParam       = new GeoMesaParam[Integer]("redis.connection.pool.size", "Max number of simultaneous connections to use", default = 16)
    val TestConnectionParam = new GeoMesaParam[java.lang.Boolean]("redis.connection.pool.validate", "Test connections when borrowed from the pool. Connections may be closed due to inactivity, which would cause a transient error if validation is disabled", default = Boolean.box(true))
    val PipelineParam       = new GeoMesaParam[java.lang.Boolean]("redis.pipeline.enabled", "Enable pipelining of query requests. This reduces network latency, but restricts queries to a single execution thread", default = Boolean.box(false))
    val ConnectionPoolParam = new GeoMesaParam[JedisPool]("redis.connection", "Connection pool") // generally used for testing
  }

  object RedisSystemProperties {
    val WriteBatchSize     = SystemProperty("geomesa.redis.write.batch", "1000")
    val TransactionRetries = SystemProperty("geomesa.redis.tx.retry", "10")
    val TransactionPause   = SystemProperty("geomesa.redis.tx.pause", "100ms")
    val TransactionBackoff = SystemProperty("geomesa.redis.tx.backoff", "1,1,2,2,5,10,20")
  }

  /**
    * Parse backoff property
    *
    * @param prop system property value
    * @return
    */
  private def backoffs(prop: String): IndexedSeq[Int] = {
    val seq = prop.split(",").map(_.toInt)
    require(seq.nonEmpty, "No backoff defined")
    seq
  }
}
