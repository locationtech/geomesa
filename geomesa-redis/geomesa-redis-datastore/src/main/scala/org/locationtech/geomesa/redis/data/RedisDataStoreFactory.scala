/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.geotools.api.data.DataAccessFactory.Param
import org.geotools.api.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.index.audit.AuditWriter
import org.locationtech.geomesa.index.audit.AuditWriter.AuditLogger
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{DataStoreQueryConfig, GeoMesaDataStoreConfig, GeoMesaDataStoreInfo}
import org.locationtech.geomesa.redis.data.index.RedisAgeOff
import org.locationtech.geomesa.security.{AuthUtils, AuthorizationsProvider}
import org.locationtech.geomesa.utils.audit.AuditProvider
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import redis.clients.jedis.util.{JedisURIHelper, Pool}
import redis.clients.jedis.{Connection, DefaultJedisClientConfig, HostAndPort, Jedis, JedisClientConfig, JedisCluster, JedisPool}

import java.awt.RenderingHints
import java.net.URI
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class RedisDataStoreFactory extends DataStoreFactorySpi with LazyLogging {

  import org.locationtech.geomesa.redis.data.RedisDataStoreParams._

  override def createNewDataStore(params: java.util.Map[String, _]): DataStore = createDataStore(params)

  override def createDataStore(params: java.util.Map[String, _]): DataStore = {
    val connection = RedisDataStoreFactory.buildConnection(params)
    val config = RedisDataStoreFactory.buildConfig(params)
    val ds = new RedisDataStore(connection, config)
    GeoMesaDataStore.initRemoteVersion(ds)
    RedisAgeOff.init(ds)
    ds
  }

  override def isAvailable = true

  override def getDisplayName: String = RedisDataStoreFactory.DisplayName

  override def getDescription: String = RedisDataStoreFactory.Description

  override def getParametersInfo: Array[Param] = Array(RedisDataStoreFactory.ParameterInfo :+ NamespaceParam: _*)

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    RedisDataStoreFactory.canProcess(params)

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object RedisDataStoreFactory extends GeoMesaDataStoreInfo with LazyLogging {

  import org.locationtech.geomesa.redis.data.RedisDataStoreParams._

  override val DisplayName = "Redis (GeoMesa)"
  override val Description = "Redis\u2122 distributed memory store"

  override val ParameterInfo: Array[GeoMesaParam[_ <: AnyRef]] =
    Array(
      RedisUrlParam,
      RedisCatalogParam,
      RedisClusterBoolParam,
      PoolSizeParam,
      SocketTimeoutParam,
      QueryThreadsParam,
      QueryTimeoutParam,
      PipelineParam,
      TestConnectionParam,
      GenerateStatsParam,
      AuditQueriesParam,
      LooseBBoxParam,
      PartitionParallelScansParam,
      AuthsParam,
      ForceEmptyAuthsParam
    )

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    RedisCatalogParam.exists(params)

  /**
    * Builds a redis connection from the data store parameters
    *
    * @param params params
    * @return
    */
  def buildConnection(params: java.util.Map[String, _]): Pool[_ <: CloseableJedisCommands] = {
    ConnectionPoolParam.lookupOpt(params).getOrElse {
      RedisClusterBoolParam.lookup(params).booleanValue() match {
        case false =>
          val uri = {
            val urls = RedisUrlParam.lookup(params).split(",")
            if (urls.length > 1) {
              throw new IllegalArgumentException(
                "Multiple Redis URLs can only be used in cluster mode. Set 'redis.clusterMode' to true.")
            }
            val url = urls.headOption.getOrElse {
              throw new IllegalArgumentException("No Redis URL provided. Please set 'redis.url'")
            }
            // if there is no protocol/port, or the url is a valid redis url, use as is
            // else use the redis:// protocol to support databases, etc
            val parsed =
              if (url.indexOf(":") == -1) {
                Try(new URI(url))
              } else {
                parse(url).orElse(parse(s"redis://$url"))
              }
            parsed match {
              case Success(uri) => uri
              case Failure(e) =>
                throw new IllegalArgumentException(s"Could not create valid Redis connection URI from: $url", e)
            }
          }
          val config = new GenericObjectPoolConfig[Jedis]()
          PoolSizeParam.lookupOpt(params).foreach(s => config.setMaxTotal(s.intValue()))
          config.setTestOnBorrow(TestConnectionParam.lookup(params))
          val timeout = SocketTimeoutParam.lookup(params).toMillis.toInt

          new JedisPool(config, uri, timeout)
        case true =>
          val urls = RedisUrlParam.lookup(params).split(",")
          if (urls.isEmpty) {
            throw new IllegalArgumentException("No Redis URLs provided. Please set 'redis.url'")
          }
          val uri = URI.create(urls.head)
          val objectPoolConfig = new GenericObjectPoolConfig[Connection]()
          PoolSizeParam.lookupOpt(params).foreach(s => objectPoolConfig.setMaxTotal(s.intValue()))
          objectPoolConfig.setTestOnBorrow(TestConnectionParam.lookup(params))
          val timeout = SocketTimeoutParam.lookup(params).toMillis.toInt

          val clusterNodes: java.util.Set[HostAndPort] =
            urls.map { url =>
              val parsed = parse(url).getOrElse {
                throw new IllegalArgumentException(s"Could not create valid Redis connection URI from: $url")
              }
              JedisURIHelper.getHostAndPort(parsed)
            }.toSet.asJava
          val jedisClientConfig: JedisClientConfig =
            DefaultJedisClientConfig.builder.connectionTimeoutMillis(timeout)
              .socketTimeoutMillis(timeout)
              .blockingSocketTimeoutMillis(0)
              .user(JedisURIHelper.getUser(uri))
              .password(JedisURIHelper.getPassword(uri))
              .database(JedisURIHelper.getDBIndex(uri))
              .clientName(null)
              .build
          new SingletonJedisClusterPool(clusterNodes, jedisClientConfig, objectPoolConfig)
      }
    }
  }

  /**
    * Builds configuration from data store parameters
    *
    * @param params params
    * @return
    */
  def buildConfig(params: java.util.Map[String, _]): RedisDataStoreConfig = {
    val catalog = RedisCatalogParam.lookup(params)
    val generateStats = GenerateStatsParam.lookup(params)
    val pipeline = PipelineParam.lookup(params)

    val audit = if (!AuditQueriesParam.lookup(params)) { None } else {
      Some(new AuditLogger("redis", AuditProvider.Loader.loadOrNone(params)))
    }
    // get the auth params passed in as a comma-delimited string
    val authProvider = AuthUtils.getProvider(params,
      AuthsParam.lookupOpt(params).map(_.split(",").toSeq.filterNot(_.isEmpty)).getOrElse(Seq.empty))

    val queries = RedisQueryConfig(
      threads = QueryThreadsParam.lookup(params),
      timeout = QueryTimeoutParam.lookupOpt(params).map(_.toMillis),
      looseBBox = LooseBBoxParam.lookup(params).booleanValue(),
      parallelPartitionScans = PartitionParallelScansParam.lookup(params)
    )

    val ns = Option(NamespaceParam.lookUp(params).asInstanceOf[String])

    RedisDataStoreConfig(catalog, generateStats, audit, authProvider, queries, pipeline, ns)
  }

  private def parse(url: String): Try[URI] = Try(new URI(url)).filter(JedisURIHelper.isValid)

  case class RedisDataStoreConfig(
      catalog: String,
      generateStats: Boolean,
      audit: Option[AuditWriter],
      authProvider: AuthorizationsProvider,
      queries: RedisQueryConfig,
      pipeline: Boolean,
      namespace: Option[String]
    ) extends GeoMesaDataStoreConfig

  case class RedisQueryConfig(
      threads: Int,
      timeout: Option[Long],
      looseBBox: Boolean,
      parallelPartitionScans: Boolean
    ) extends DataStoreQueryConfig
}

