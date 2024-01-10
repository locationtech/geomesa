/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.redis.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{DataStoreQueryConfig, GeoMesaDataStoreConfig, GeoMesaDataStoreInfo}
import org.locationtech.geomesa.redis.data.index.RedisAgeOff
import org.locationtech.geomesa.security.{AuthUtils, AuthorizationsProvider}
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import redis.clients.jedis.util.JedisURIHelper
import redis.clients.jedis.{Jedis, JedisPool}

import java.awt.RenderingHints
import java.net.URI
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
  def buildConnection(params: java.util.Map[String, _]): JedisPool = {
    ConnectionPoolParam.lookupOpt(params).getOrElse {
      val uri = {
        val url = RedisUrlParam.lookup(params)
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
      Some((AuditLogger, Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider), "redis"))
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
      audit: Option[(AuditWriter, AuditProvider, String)],
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

