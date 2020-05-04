/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016-2020 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.awt.RenderingHints
import java.io.{IOException, Serializable}
import java.nio.charset.StandardCharsets
import java.util.Locale

import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty
import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, KerberosToken, PasswordToken}
import org.apache.accumulo.core.client.{ClientConfiguration, Connector, ZooKeeperInstance}
import org.apache.hadoop.security.UserGroupInformation
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.accumulo.audit.{AccumuloAuditService, ParamsAuditProvider}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory._
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditProvider, AuditReader, AuditWriter}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

class AccumuloDataStoreFactory extends DataStoreFactorySpi {

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: java.util.Map[String, Serializable]): AccumuloDataStore =
    createDataStore(params)

  override def createDataStore(params: java.util.Map[String, Serializable]): AccumuloDataStore = {
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

  override def canProcess(params: java.util.Map[String,Serializable]): Boolean =
    AccumuloDataStoreFactory.canProcess(params)

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object AccumuloDataStoreFactory extends GeoMesaDataStoreInfo {

  import AccumuloDataStoreParams._

  import scala.collection.JavaConverters._

  val RemoteArrowProperty   : SystemProperty = SystemProperty("geomesa.accumulo.remote.arrow.enable")
  val RemoteBinProperty     : SystemProperty = SystemProperty("geomesa.accumulo.remote.bin.enable")
  val RemoteDensityProperty : SystemProperty = SystemProperty("geomesa.accumulo.remote.density.enable")
  val RemoteStatsProperty   : SystemProperty = SystemProperty("geomesa.accumulo.remote.stats.enable")

  override val DisplayName = "Accumulo (GeoMesa)"
  override val Description = "Apache Accumulo\u2122 distributed key/value store"

  override val ParameterInfo: Array[GeoMesaParam[_]] =
    Array(
      InstanceIdParam,
      ZookeepersParam,
      ZookeeperTimeoutParam,
      CatalogParam,
      UserParam,
      PasswordParam,
      KeytabPathParam,
      QueryThreadsParam,
      RecordThreadsParam,
      WriteThreadsParam,
      QueryTimeoutParam,
      RemoteArrowParam,
      RemoteBinParam,
      RemoteDensityParam,
      RemoteStatsParam,
      GenerateStatsParam,
      AuditQueriesParam,
      LooseBBoxParam,
      CachingParam,
      AuthsParam,
      ForceEmptyAuthsParam
    )

  private val MockParam =
    new GeoMesaParam[java.lang.Boolean](
      "accumulo.mock",
      default = false,
      deprecatedKeys = Seq("useMock", "accumulo.useMock"))

  // used to handle geoserver password encryption in persisted ds params
  private val DeprecatedGeoServerPasswordParam =
    new Param(
      "password",
      classOf[String],
      "",
      false,
      null,
      Map(Parameter.DEPRECATED -> true, Parameter.IS_PASSWORD -> true).asJava)

  override def canProcess(params: java.util.Map[String, _ <: Serializable]): Boolean =
    CatalogParam.exists(params)

  def buildAccumuloConnector(params: java.util.Map[String, _ <: Serializable]): Connector = {
    if (MockParam.lookup(params)) {
      throw new IllegalArgumentException("Mock Accumulo connections are not supported")
    }

    def lookup[T <: AnyRef](param: GeoMesaParam[T], fallback: => Option[T]): T =
      param.lookupOpt(params).orElse(fallback).getOrElse {
        throw new IOException(s"Parameter ${param.key} is required: ${param.description}")
      }

    lazy val config = AccumuloClientConfig.load()
    val instance = lookup(InstanceIdParam, config.instance)
    val zookeepers = lookup(ZookeepersParam, config.zookeepers)

    val conf = ClientConfiguration.create().withInstance(instance).withZkHosts(zookeepers)
    ZookeeperTimeoutParam.lookupOpt(params).orElse(config.zkTimeout).foreach { timeout =>
      conf.`with`(ClientProperty.INSTANCE_ZK_TIMEOUT, timeout)
    }

    val user = lookup(UserParam, config.principal)

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
        config.authType.map(_.trim.toLowerCase(Locale.US)).getOrElse {
          throw new IOException(s"Parameter ${PasswordParam.key} is required: ${PasswordParam.description}")
        }
      }

    // build authentication token according to how we are authenticating
    val auth: AuthenticationToken = if (authType == AccumuloClientConfig.PasswordAuthType) {
      new PasswordToken(lookup(PasswordParam, config.token).getBytes(StandardCharsets.UTF_8))
    } else if (authType == AccumuloClientConfig.KerberosAuthType) {
      // explicitly enable SASL for kerberos connections
      // this shouldn't be required if Accumulo client.conf is set appropriately, but it doesn't seem to work
      conf.withSasl(true)
      val file = new java.io.File(lookup(KeytabPathParam, config.token))
      // mimic behavior from accumulo 1.9 and earlier:
      // `public KerberosToken(String principal, File keytab, boolean replaceCurrentUser)`
      UserGroupInformation.loginUserFromKeytab(user, file.getAbsolutePath)
      new KerberosToken(user, file)
    } else {
      throw new IllegalArgumentException(s"Unsupported auth type: $authType")
    }

    new ZooKeeperInstance(conf).getConnector(user, auth)
  }

  def buildConfig(connector: Connector, params: java.util.Map[String, _ <: Serializable]): AccumuloDataStoreConfig = {
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
      caching = CachingParam.lookup(params)
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

  def buildAuditProvider(params: java.util.Map[String, _ <: Serializable]): AuditProvider = {
    Option(AuditProvider.Loader.load(params)).getOrElse {
      val provider = new ParamsAuditProvider
      provider.configure(params)
      provider
    }
  }

  def buildAuthsProvider(
      connector: Connector,
      params: java.util.Map[String, _ <: Serializable]): AuthorizationsProvider = {
    // convert the connector authorizations into a string array - this is the maximum auths this connector can support
    val securityOps = connector.securityOperations
    val masterAuths = securityOps.getUserAuthorizations(connector.whoami).asScala.toArray.map(b => new String(b))

    // get the auth params passed in as a comma-delimited string
    val configuredAuths = AuthsParam.lookupOpt(params).getOrElse("").split(",").filter(s => !s.isEmpty)

    // verify that the configured auths are valid for the connector we are using (fail-fast)
    val invalidAuths = configuredAuths.filterNot(masterAuths.contains)
    if (invalidAuths.nonEmpty) {
      throw new IllegalArgumentException(s"The authorizations '${invalidAuths.mkString(",")}' " +
        "are not valid for the Accumulo connection being used")
    }

    val forceEmptyAuths = ForceEmptyAuthsParam.lookup(params)

    // if the caller provided any non-null string for authorizations, use it;
    // otherwise, grab all authorizations to which the Accumulo user is entitled
    if (configuredAuths.length != 0 && forceEmptyAuths) {
      throw new IllegalArgumentException("Forcing empty auths is checked, but explicit auths are provided")
    }
    val auths = if (forceEmptyAuths || configuredAuths.length > 0) { configuredAuths } else { masterAuths }

    AuthorizationsProvider.apply(params, java.util.Arrays.asList(auths: _*))
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
      caching: Boolean
    ) extends DataStoreQueryConfig

  case class RemoteScansEnabled(arrow: Boolean, bin: Boolean, density: Boolean, stats: Boolean)
}
