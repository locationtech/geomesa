/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data

import java.awt.RenderingHints
import java.io.Serializable
import java.net.URI

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreInfo
import org.locationtech.geomesa.redis.data.RedisDataStore.RedisDataStoreConfig
import org.locationtech.geomesa.security
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, NoOpAuditProvider}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import redis.clients.jedis.JedisPool
import redis.clients.jedis.util.JedisURIHelper

import scala.util.Try

class RedisDataStoreFactory extends DataStoreFactorySpi with LazyLogging {

  import org.locationtech.geomesa.redis.data.RedisDataStoreParams._

  override def createNewDataStore(params: java.util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
    val connection = RedisDataStoreFactory.buildConnection(params)
    val config = RedisDataStoreFactory.buildConfig(params)
    val ds = new RedisDataStore(connection, config)
    GeoMesaDataStore.initRemoteVersion(ds)
    ds
  }

  override def isAvailable = true

  override def getDisplayName: String = RedisDataStoreFactory.DisplayName

  override def getDescription: String = RedisDataStoreFactory.Description

  override def getParametersInfo: Array[Param] = RedisDataStoreFactory.ParameterInfo :+ NamespaceParam

  override def canProcess(params: java.util.Map[String,Serializable]): Boolean =
    RedisDataStoreFactory.canProcess(params)

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object RedisDataStoreFactory extends GeoMesaDataStoreInfo with LazyLogging {

  import org.locationtech.geomesa.redis.data.RedisDataStoreParams._

  override val DisplayName = "Redis (GeoMesa)"
  override val Description = "Redis\u2122 distributed memory store"

  override val ParameterInfo: Array[GeoMesaParam[_]] =
    Array(
      RedisUrlParam,
      RedisCatalogParam,
      PoolSizeParam,
      QueryThreadsParam,
      QueryTimeoutParam,
      PipelineParam,
      TestConnectionParam,
      GenerateStatsParam,
      AuditQueriesParam,
      LooseBBoxParam,
      CachingParam,
      AuthsParam,
      ForceEmptyAuthsParam
    )

  override def canProcess(params: java.util.Map[java.lang.String,Serializable]): Boolean =
    RedisCatalogParam.exists(params)

  /**
    * Builds a redis connection from the data store parameters
    *
    * @param params params
    * @return
    */
  def buildConnection(params: java.util.Map[String, Serializable]): JedisPool = {
    ConnectionPoolParam.lookupOpt(params).getOrElse {
      val url = RedisUrlParam.lookup(params)
      val config = new GenericObjectPoolConfig()
      PoolSizeParam.lookupOpt(params).foreach(s => config.setMaxTotal(s.intValue()))
      config.setTestOnBorrow(TestConnectionParam.lookup(params))
      // if there is no protocol/port, or the url is a valid redis url, use as is
      // else use the redis:// protocol to support databases, etc
      if (url.indexOf(":") == -1) {
        new JedisPool(config, url)
      } else {
        val uri = parse(url).orElse(parse(s"redis://$url")).getOrElse {
          throw new IllegalArgumentException(s"Could not create valid Redis connection URI from: $url")
        }
        new JedisPool(config, uri)
      }
    }
  }

  /**
    * Builds configuration from data store parameters
    *
    * @param params params
    * @return
    */
  def buildConfig(params: java.util.Map[String, Serializable]): RedisDataStoreConfig = {
    val catalog = RedisCatalogParam.lookup(params)
    val generateStats = GenerateStatsParam.lookup(params)
    val pipeline = PipelineParam.lookup(params)
    val queryThreads = QueryThreadsParam.lookup(params)
    val queryTimeout = QueryTimeoutParam.lookupOpt(params).map(_.toMillis)
    val looseBBox = LooseBBoxParam.lookup(params).booleanValue()
    val caching = CachingParam.lookup(params)

    val audit = if (!AuditQueriesParam.lookup(params)) { None } else {
      Some((AuditLogger, Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider), "redis"))
    }
    // get the auth params passed in as a comma-delimited string
    val authProvider = security.getAuthorizationsProvider(params,
      AuthsParam.lookupOpt(params).map(_.split(",").toSeq.filterNot(_.isEmpty)).getOrElse(Seq.empty))

    val ns = Option(NamespaceParam.lookUp(params).asInstanceOf[String])

    RedisDataStoreConfig(
      catalog, generateStats, audit, pipeline, queryThreads, queryTimeout, looseBBox, caching, authProvider, ns)
  }

  private def parse(url: String): Try[URI] = Try(new URI(url)).filter(JedisURIHelper.isValid)
}

