/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.kudu.data

import java.awt.RenderingHints
import java.io.Serializable
import java.nio.charset.StandardCharsets
import java.util

import com.typesafe.scalalogging.LazyLogging
import org.apache.kudu.client.KuduClient
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{GeoMesaDataStoreConfig, GeoMesaDataStoreParams}
import org.locationtech.geomesa.kudu.data.KuduDataStoreFactory.KuduDataStoreConfig
import org.locationtech.geomesa.security
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

class KuduDataStoreFactory extends DataStoreFactorySpi with LazyLogging {

  import KuduDataStoreFactory.Params._
  import org.locationtech.geomesa.kudu.KuduSystemProperties.{AdminOperationTimeout, OperationTimeout, SocketReadTimeout}

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def createDataStore(params: util.Map[String, Serializable]): DataStore = {

    val client = {
      val master = KuduMasterParam.lookup(params)
      val builder = new KuduClient.KuduClientBuilder(master)

      WorkerThreadsParam.lookupOpt(params).foreach(i => builder.workerCount(i.intValue()))
      BossThreadsParam.lookupOpt(params).foreach(i => builder.bossCount(i.intValue()))
      StatisticsParam.lookupOpt(params).filterNot(_.booleanValue()).foreach(_ => builder.disableStatistics())

      AdminOperationTimeout.toDuration.foreach(d => builder.defaultAdminOperationTimeoutMs(d.toMillis))
      OperationTimeout.toDuration.foreach(d => builder.defaultOperationTimeoutMs(d.toMillis))
      SocketReadTimeout.toDuration.foreach(d => builder.defaultSocketReadTimeoutMs(d.toMillis))

      logger.debug(s"Connecting to Kudu master at: $master")
      builder.build()
    }

    CredentialsParam.lookupOpt(params).foreach { creds =>
      client.importAuthenticationCredentials(creds.getBytes(StandardCharsets.UTF_8))
    }

    val generateStats = GenerateStatsParam.lookup(params)
    val audit = if (!AuditQueriesParam.lookup(params)) { None } else {
      Some(AuditLogger, Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider), "kudu")
    }
    val authProvider = {
      // get the auth params passed in as a comma-delimited string
      val auths = AuthsParam.lookupOpt(params).map(_.split(",").filterNot(_.isEmpty)).getOrElse(Array.empty)
      security.getAuthorizationsProvider(params, auths)
    }

    val caching = CachingParam.lookup(params)

    val catalog = CatalogParam.lookup(params)

    val looseBBox = LooseBBoxParam.lookup(params)

    // not used but required for config inheritance
    val queryThreads = QueryThreadsParam.lookup(params)
    val queryTimeout = QueryTimeoutParam.lookupOpt(params).map(_.toMillis)

    val ns = Option(NamespaceParam.lookUp(params).asInstanceOf[String])

    val cfg = KuduDataStoreConfig(catalog, generateStats, authProvider, audit, caching,
      queryThreads, queryTimeout, looseBBox, ns)

    new KuduDataStore(client, cfg)
  }

  override def getDisplayName: String = KuduDataStoreFactory.DisplayName

  override def getDescription: String = KuduDataStoreFactory.Description

  override def getParametersInfo: Array[Param] =
    Array(
      KuduMasterParam,
      CatalogParam,
      WorkerThreadsParam,
      BossThreadsParam,
      GenerateStatsParam,
      AuditQueriesParam,
      AuthsParam,
      LooseBBoxParam,
      CachingParam,
      QueryThreadsParam,
      QueryTimeoutParam,
      NamespaceParam
    )

  override def canProcess(params: java.util.Map[String,Serializable]): Boolean = KuduMasterParam.exists(params)

  override def isAvailable = true
  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object KuduDataStoreFactory {

  val DisplayName = "Kudu (GeoMesa)"
  val Description = "Apache Kudu\u2122 columnar store"

  // noinspection TypeAnnotation
  object Params extends GeoMesaDataStoreParams {

    override protected def looseBBoxDefault = false

    val KuduMasterParam    = new GeoMesaParam[String]("kudu.master", "Kudu master host[:port][,host2[:port2]...]", optional = false)
    val CatalogParam       = new GeoMesaParam[String]("kudu.catalog", "Name of GeoMesa catalog table", optional = false)
    val CredentialsParam   = new GeoMesaParam[String]("kudu.credentials", "Kudu client authentication credentials")
    val WorkerThreadsParam = new GeoMesaParam[Integer]("kudu.worker.threads", "Number of worker threads")
    val BossThreadsParam   = new GeoMesaParam[Integer]("kudu.boss.threads", "Number of boss threads")
    val StatisticsParam    = new GeoMesaParam[java.lang.Boolean]("kudu.client.stats", "Enable Kudu client statistics")
    val AuthsParam         = org.locationtech.geomesa.security.AuthsParam
  }

  case class KuduDataStoreConfig(catalog: String,
                                 generateStats: Boolean,
                                 authProvider: AuthorizationsProvider,
                                 audit: Option[(AuditWriter, AuditProvider, String)],
                                 caching: Boolean,
                                 queryThreads: Int,
                                 queryTimeout: Option[Long],
                                 looseBBox: Boolean,
                                 namespace: Option[String]) extends GeoMesaDataStoreConfig
}
