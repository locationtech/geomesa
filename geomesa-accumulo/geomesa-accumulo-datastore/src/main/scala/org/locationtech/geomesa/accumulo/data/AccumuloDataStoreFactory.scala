/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016-2024 Dstl
 * Portions Copyright (c) 2021 The MITRE Corporation
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * This software was produced for the U. S. Government under Basic
 * Contract No. W56KGU-18-D-0004, and is subject to the Rights in
 * Noncommercial Computer Software and Noncommercial Computer Software
 * Documentation Clause 252.227-7014 (FEB 2012)
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data


import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, KerberosToken, PasswordToken}
import org.apache.accumulo.core.client.{Accumulo, AccumuloClient}
import org.apache.accumulo.core.conf.ClientProperty
import org.apache.hadoop.security.UserGroupInformation
<<<<<<< HEAD
import org.geotools.api.data.DataAccessFactory.Param
import org.geotools.api.data.{DataStoreFactorySpi, Parameter}
=======
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStoreFactorySpi, Parameter}
<<<<<<< HEAD
<<<<<<< HEAD
>>>>>>> 4a4bbd8ec03 (GEOMESA-3254 Add Bloop build support)
=======
<<<<<<< HEAD
>>>>>>> 76c1a24bd97 (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
import org.locationtech.geomesa.accumulo.AccumuloProperties.{BatchWriterProperties, RemoteProcessingProperties}
=======
import org.locationtech.geomesa.accumulo.AccumuloProperties.BatchWriterProperties
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)
=======
import org.locationtech.geomesa.accumulo.AccumuloProperties.{BatchWriterProperties, RemoteProcessingProperties}
>>>>>>> 585c5638c0 (GEOMESA-3267 HBase, Accumulo - Fix potential deadlocks in data store factory)
import org.locationtech.geomesa.accumulo.audit.{AccumuloAuditService, ParamsAuditProvider}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory._
import org.locationtech.geomesa.security.{AuthUtils, AuthorizationsProvider}
import org.locationtech.geomesa.utils.audit.{AuditProvider, AuditReader, AuditWriter}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

import java.awt.RenderingHints
import java.io.IOException
import java.nio.charset.StandardCharsets
import java.util.Locale

class AccumuloDataStoreFactory extends DataStoreFactorySpi {

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: java.util.Map[String, _]): AccumuloDataStore =
    createDataStore(params)

  override def createDataStore(params: java.util.Map[String, _]): AccumuloDataStore = {
    val connector = AccumuloDataStoreFactory.buildAccumuloConnector(params)
    val config = AccumuloDataStoreFactory.buildConfig(connector, params)
    val ds = new AccumuloDataStore(connector, config)
    GeoMesaDataStore.initRemoteVersion(ds)
    ds
  }

  override def isAvailable = true

  override def getDisplayName: String = AccumuloDataStoreFactory.DisplayName

  override def getDescription: String = AccumuloDataStoreFactory.Description

  override def getParametersInfo: Array[Param] =
    AccumuloDataStoreFactory.ParameterInfo ++
        Array(AccumuloDataStoreParams.NamespaceParam, AccumuloDataStoreFactory.DeprecatedGeoServerPasswordParam)

  override def canProcess(params: java.util.Map[String,_]): Boolean =
    AccumuloDataStoreFactory.canProcess(params)

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object AccumuloDataStoreFactory extends GeoMesaDataStoreInfo {

  import AccumuloDataStoreParams._

  import scala.collection.JavaConverters._

  @deprecated("Moved to org.locationtech.geomesa.accumulo.AccumuloProperties.RemoteProcessingProperties")
  val RemoteArrowProperty   : SystemProperty = RemoteProcessingProperties.RemoteArrowProperty
  @deprecated("Moved to org.locationtech.geomesa.accumulo.AccumuloProperties.RemoteProcessingProperties")
  val RemoteBinProperty     : SystemProperty = RemoteProcessingProperties.RemoteBinProperty
  @deprecated("Moved to org.locationtech.geomesa.accumulo.AccumuloProperties.RemoteProcessingProperties")
  val RemoteDensityProperty : SystemProperty = RemoteProcessingProperties.RemoteDensityProperty
  @deprecated("Moved to org.locationtech.geomesa.accumulo.AccumuloProperties.RemoteProcessingProperties")
  val RemoteStatsProperty   : SystemProperty = RemoteProcessingProperties.RemoteStatsProperty

  override val DisplayName = "Accumulo (GeoMesa)"
  override val Description = "Apache Accumulo\u2122 distributed key/value store"

  override val ParameterInfo: Array[GeoMesaParam[_ <: AnyRef]] =
    Array(
      InstanceNameParam,
      ZookeepersParam,
      CatalogParam,
      UserParam,
      PasswordParam,
      KeytabPathParam,
      QueryThreadsParam,
      RecordThreadsParam,
      WriteThreadsParam,
      QueryTimeoutParam,
      ZookeeperTimeoutParam,
      RemoteArrowParam,
      RemoteBinParam,
      RemoteDensityParam,
      RemoteStatsParam,
      GenerateStatsParam,
      AuditQueriesParam,
      LooseBBoxParam,
      PartitionParallelScansParam,
      AuthsParam,
      ForceEmptyAuthsParam
    )

  // used to handle geoserver password encryption in persisted ds params
  private val DeprecatedGeoServerPasswordParam =
    new Param(
      "password",
      classOf[String],
      "",
      false,
      null,
      Map(Parameter.DEPRECATED -> true, Parameter.IS_PASSWORD -> true).asJava)

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    CatalogParam.exists(params)

  def buildAccumuloConnector(params: java.util.Map[String, _]): AccumuloClient = {
    val config = AccumuloClientConfig.load()

    def setRequired(param: GeoMesaParam[String], key: ClientProperty): Unit = {
      param.lookupOpt(params) match {
        case Some(v) => config.put(key.getKey, v)
        case None =>
          if (config.get(key.getKey) == null) {
            throw new IOException(s"Parameter ${param.key} is required: ${param.description}")
          }
      }
    }

    def setOptional(param: GeoMesaParam[String], key: ClientProperty): Unit =
      param.lookupOpt(params).foreach(config.put(key.getKey, _))

    def getRequired(param: GeoMesaParam[String], key: ClientProperty): String = {
      param.lookupOpt(params).orElse(Option(config.getProperty(key.getKey))).getOrElse {
        throw new IOException(s"Parameter ${param.key} is required: ${param.description}")
      }
    }

    setRequired(InstanceNameParam, ClientProperty.INSTANCE_NAME)
    setRequired(ZookeepersParam, ClientProperty.INSTANCE_ZOOKEEPERS)
    setOptional(ZookeeperTimeoutParam, ClientProperty.INSTANCE_ZOOKEEPERS_TIMEOUT)

    val user = getRequired(UserParam, ClientProperty.AUTH_PRINCIPAL)

    if (PasswordParam.exists(params) && KeytabPathParam.exists(params)) {
      throw new IllegalArgumentException(
        s"'${PasswordParam.key}' and '${KeytabPathParam.key}' are mutually exclusive, but are both set")
    }

    val authType =
      if (PasswordParam.exists(params)) {
        AccumuloClientConfig.PasswordAuthType
      } else if (KeytabPathParam.exists(params)) {
        AccumuloClientConfig.KerberosAuthType
      } else {
        ClientProperty.AUTH_TYPE.getValue(config).toLowerCase(Locale.US)
      }

    // build authentication token according to how we are authenticating
    val auth: AuthenticationToken = if (authType == AccumuloClientConfig.PasswordAuthType) {
      new PasswordToken(getRequired(PasswordParam, ClientProperty.AUTH_TOKEN).getBytes(StandardCharsets.UTF_8))
    } else if (authType == AccumuloClientConfig.KerberosAuthType) {
      val file = new java.io.File(getRequired(KeytabPathParam, ClientProperty.AUTH_TOKEN))
      // mimic behavior from accumulo 1.9 and earlier:
      // `public KerberosToken(String principal, File keytab, boolean replaceCurrentUser)`
      UserGroupInformation.loginUserFromKeytab(user, file.getAbsolutePath)
      new KerberosToken(user, file)
    } else {
      throw new IllegalArgumentException(s"Unsupported auth type: $authType")
    }

    if (WriteThreadsParam.exists(params)) {
      config.put(ClientProperty.BATCH_WRITER_THREADS_MAX.getKey, WriteThreadsParam.lookup(params).toString)
    } else if (!config.containsKey(ClientProperty.BATCH_WRITER_THREADS_MAX.getKey)) {
      BatchWriterProperties.WRITER_THREADS.option.foreach { threads =>
        config.put(ClientProperty.BATCH_WRITER_THREADS_MAX.getKey, String.valueOf(threads))
      }
    }
    if (!config.containsKey(ClientProperty.BATCH_WRITER_MEMORY_MAX.getKey)) {
      BatchWriterProperties.WRITER_MEMORY_BYTES.toBytes.foreach { memory =>
        config.put(ClientProperty.BATCH_WRITER_MEMORY_MAX.getKey, String.valueOf(memory))
      }
    }
    if (!config.containsKey(ClientProperty.BATCH_WRITER_LATENCY_MAX.getKey)) {
      BatchWriterProperties.WRITER_LATENCY.toDuration.foreach { duration =>
        config.put(ClientProperty.BATCH_WRITER_LATENCY_MAX.getKey, s"${duration.toMillis}ms")
      }
    }
    if (!config.containsKey(ClientProperty.BATCH_WRITER_TIMEOUT_MAX.getKey)) {
      BatchWriterProperties.WRITE_TIMEOUT.toDuration.foreach { duration =>
        config.put(ClientProperty.BATCH_WRITER_TIMEOUT_MAX.getKey, s"${duration.toMillis}ms")
      }
    }

    Accumulo.newClient().from(config).as(user, auth).build()
  }

  def buildConfig(connector: AccumuloClient, params: java.util.Map[String, _]): AccumuloDataStoreConfig = {
    val catalog = CatalogParam.lookup(params)

    val authProvider = buildAuthsProvider(connector, params)
    val auditProvider = buildAuditProvider(params)
    val auditQueries = AuditQueriesParam.lookup(params).booleanValue()
    
    val auditService = new AccumuloAuditService(connector, authProvider, s"${catalog}_queries", auditQueries)

    val queries = AccumuloQueryConfig(
      threads = QueryThreadsParam.lookup(params),
      recordThreads = RecordThreadsParam.lookup(params),
      timeout = QueryTimeoutParam.lookupOpt(params).map(_.toMillis),
      looseBBox = LooseBBoxParam.lookup(params),
      parallelPartitionScans = PartitionParallelScansParam.lookup(params)
    )

    val remote = RemoteScansEnabled(
      arrow = RemoteArrowParam.lookup(params),
      bin = RemoteBinParam.lookup(params),
      density = RemoteDensityParam.lookup(params),
      stats = RemoteStatsParam.lookup(params)
    )

    AccumuloDataStoreConfig(
      catalog = catalog,
      generateStats = GenerateStatsParam.lookup(params),
      authProvider = authProvider,
      audit = Some(auditService, auditProvider, AccumuloAuditService.StoreType),
      queries = queries,
      remote = remote,
      writeThreads = WriteThreadsParam.lookup(params),
      namespace = NamespaceParam.lookupOpt(params)
    )
  }

  def buildAuditProvider(params: java.util.Map[String, _]): AuditProvider = {
    Option(AuditProvider.Loader.load(params)).getOrElse {
      val provider = new ParamsAuditProvider
      provider.configure(params)
      provider
    }
  }

  def buildAuthsProvider(connector: AccumuloClient, params: java.util.Map[String, _]): AuthorizationsProvider = {
    // convert the connector authorizations into a string array - this is the maximum auths this connector can support
    val securityOps = connector.securityOperations
    val masterAuths = securityOps.getUserAuthorizations(connector.whoami).asScala.toSeq.map(b => new String(b))

    // get the auth params passed in as a comma-delimited string
<<<<<<< HEAD
    val configuredAuths = AuthsParam.lookupOpt(params).getOrElse("").split(",").filterNot(_.isEmpty).toSeq
=======
    val configuredAuths = AuthsParam.lookupOpt(params).getOrElse("").split(",").filterNot(_.isEmpty)
>>>>>>> 58d14a257e (GEOMESA-3254 Add Bloop build support)

    // verify that the configured auths are valid for the connector we are using (fail-fast)
    val invalidAuths = configuredAuths.filterNot(masterAuths.contains)
    if (invalidAuths.nonEmpty) {
      throw new IllegalArgumentException(s"The authorizations '${invalidAuths.mkString(",")}' " +
        "are not valid for the Accumulo connection being used")
    }

    val forceEmptyAuths = ForceEmptyAuthsParam.lookup(params)

    // if the caller provided any non-null string for authorizations, use it;
    // otherwise, grab all authorizations to which the Accumulo user is entitled
    if (configuredAuths.nonEmpty && forceEmptyAuths) {
      throw new IllegalArgumentException("Forcing empty auths is checked, but explicit auths are provided")
    }
    val auths = if (forceEmptyAuths || configuredAuths.nonEmpty) { configuredAuths } else { masterAuths }

    AuthUtils.getProvider(params, auths)
  }

  /**
   * Configuration options for AccumuloDataStore
   *
   * @param catalog table in Accumulo used to store feature type metadata
   * @param generateStats write stats on data during ingest
   * @param authProvider provides the authorizations used to access data
   * @param audit optional implementations to audit queries
   * @param queries query config
   * @param remote remote query configs
   * @param writeThreads number of threads used for writing
   */
  case class AccumuloDataStoreConfig(
      catalog: String,
      generateStats: Boolean,
      authProvider: AuthorizationsProvider,
      audit: Option[(AuditWriter with AuditReader, AuditProvider, String)],
      queries: AccumuloQueryConfig,
      remote: RemoteScansEnabled,
      writeThreads: Int,
      namespace: Option[String]
    ) extends GeoMesaDataStoreConfig

  case class AccumuloQueryConfig(
      threads: Int,
      recordThreads: Int,
      timeout: Option[Long],
      looseBBox: Boolean,
      parallelPartitionScans: Boolean
    ) extends DataStoreQueryConfig

  case class RemoteScansEnabled(arrow: Boolean, bin: Boolean, density: Boolean, stats: Boolean)
}
