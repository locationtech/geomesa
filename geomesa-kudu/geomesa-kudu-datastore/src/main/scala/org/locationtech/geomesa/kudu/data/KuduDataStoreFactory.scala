/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.data

import java.awt.RenderingHints
import java.io.Serializable
import java.nio.charset.StandardCharsets

import com.typesafe.scalalogging.LazyLogging
import org.apache.kudu.client.KuduClient
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{DataStoreQueryConfig, GeoMesaDataStoreConfig, GeoMesaDataStoreInfo, GeoMesaDataStoreParams}
import org.locationtech.geomesa.kudu.KuduSystemProperties.{AdminOperationTimeout, OperationTimeout, SocketReadTimeout}
import org.locationtech.geomesa.kudu.data.KuduDataStoreFactory.{KuduDataStoreConfig, KuduQueryConfig}
import org.locationtech.geomesa.security
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

class KuduDataStoreFactory extends DataStoreFactorySpi {

  import KuduDataStoreFactory.Params._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: java.util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {

    val client = KuduDataStoreFactory.buildClient(params)

    val generateStats = GenerateStatsParam.lookup(params)
    val audit = if (!AuditQueriesParam.lookup(params)) { None } else {
      Some(AuditLogger, Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider), "kudu")
    }
    val authProvider = {
      // get the auth params passed in as a comma-delimited string
      val auths = AuthsParam.lookupOpt(params).map(_.split(",").filterNot(_.isEmpty)).getOrElse(Array.empty)
      security.getAuthorizationsProvider(params, auths)
    }

    val catalog = CatalogParam.lookup(params)

    val queries = KuduQueryConfig(
      threads = QueryThreadsParam.lookup(params),
      timeout = QueryTimeoutParam.lookupOpt(params).map(_.toMillis),
      looseBBox = LooseBBoxParam.lookup(params),
      caching = CachingParam.lookup(params)
    )

    val ns = Option(NamespaceParam.lookUp(params).asInstanceOf[String])

    val cfg = KuduDataStoreConfig(catalog, generateStats, authProvider, audit, queries, ns)

    new KuduDataStore(client, cfg)
  }

  override def isAvailable = true

  override def getDisplayName: String = KuduDataStoreFactory.DisplayName

  override def getDescription: String = KuduDataStoreFactory.Description

  override def getParametersInfo: Array[Param] = KuduDataStoreFactory.ParameterInfo :+ NamespaceParam

  override def canProcess(params: java.util.Map[String,Serializable]): Boolean =
    KuduDataStoreFactory.canProcess(params)

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object KuduDataStoreFactory extends GeoMesaDataStoreInfo with LazyLogging {

  override val DisplayName = "Kudu (GeoMesa)"
  override val Description = "Apache Kudu\u2122 columnar store"

  override val ParameterInfo: Array[GeoMesaParam[_]] =
    Array(
      Params.KuduMasterParam,
      Params.CatalogParam,
      Params.CredentialsParam,
      Params.WorkerThreadsParam,
      Params.BossThreadsParam,
      Params.QueryThreadsParam,
      Params.QueryTimeoutParam,
      Params.AuthsParam,
      Params.LooseBBoxParam,
      Params.AuditQueriesParam,
      Params.GenerateStatsParam,
      Params.StatisticsParam,
      Params.CachingParam
    )

  override def canProcess(params: java.util.Map[String, _ <: java.io.Serializable]): Boolean =
    Params.KuduMasterParam.exists(params)

  // noinspection TypeAnnotation
  object Params extends GeoMesaDataStoreParams {

    override protected def looseBBoxDefault = false

    val KuduMasterParam =
      new GeoMesaParam[String](
        "kudu.master",
        "Kudu master host[:port][,host2[:port2]...]",
        optional = false,
        supportsNiFiExpressions = true)

    val CatalogParam =
      new GeoMesaParam[String](
        "kudu.catalog",
        "Name of GeoMesa catalog table",
        optional = false,
        supportsNiFiExpressions = true)

    val CredentialsParam =
      new GeoMesaParam[String](
        "kudu.credentials",
        "Kudu client authentication credentials",
        supportsNiFiExpressions = true)

    val WorkerThreadsParam =
      new GeoMesaParam[Integer](
        "kudu.worker.threads",
        "Number of worker threads",
        supportsNiFiExpressions = true)

    val BossThreadsParam =
      new GeoMesaParam[Integer](
        "kudu.boss.threads",
        "Number of boss threads",
        supportsNiFiExpressions = true)

    val StatisticsParam =
      new GeoMesaParam[java.lang.Boolean](
        "kudu.client.stats.disable",
        "Disable Kudu client statistics")

    val AuthsParam = org.locationtech.geomesa.security.AuthsParam
  }

  def buildClient(params: java.util.Map[String, java.io.Serializable]): KuduClient = {
    import Params._

    val master = KuduMasterParam.lookup(params)
    val builder = new KuduClient.KuduClientBuilder(master)

    WorkerThreadsParam.lookupOpt(params).foreach(i => builder.workerCount(i.intValue()))
    BossThreadsParam.lookupOpt(params).foreach(i => builder.bossCount(i.intValue()))

    if (StatisticsParam.lookupOpt(params).exists(_.booleanValue())) {
      builder.disableStatistics()
    }

    AdminOperationTimeout.toDuration.foreach(d => builder.defaultAdminOperationTimeoutMs(d.toMillis))
    OperationTimeout.toDuration.foreach(d => builder.defaultOperationTimeoutMs(d.toMillis))
    SocketReadTimeout.toDuration.foreach(d => builder.defaultSocketReadTimeoutMs(d.toMillis))

    logger.debug(s"Connecting to Kudu master at: $master")

    val client = builder.build()

    CredentialsParam.lookupOpt(params).foreach { creds =>
      client.importAuthenticationCredentials(creds.getBytes(StandardCharsets.UTF_8))
    }

    client
  }

  case class KuduDataStoreConfig(
      catalog: String,
      generateStats: Boolean,
      authProvider: AuthorizationsProvider,
      audit: Option[(AuditWriter, AuditProvider, String)],
      queries: KuduQueryConfig,
      namespace: Option[String]
    ) extends GeoMesaDataStoreConfig

  case class KuduQueryConfig(
      threads: Int,
      timeout: Option[Long],
      looseBBox: Boolean,
      caching: Boolean
    ) extends DataStoreQueryConfig
}
