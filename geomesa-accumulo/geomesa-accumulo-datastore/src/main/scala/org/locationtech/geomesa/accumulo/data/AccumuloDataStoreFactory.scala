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
import java.io.Serializable
import java.util.{Map => JMap}

import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty
import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, KerberosToken, PasswordToken}
import org.apache.accumulo.core.client.{ClientConfiguration, Connector, ZooKeeperInstance}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.DataStoreFactorySpi
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.audit.{AccumuloAuditService, ParamsAuditProvider}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore.AccumuloDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory._
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.AuditProvider
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

class AccumuloDataStoreFactory extends DataStoreFactorySpi {

  import AccumuloDataStoreParams.{DeprecatedGeoServerPasswordParam, NamespaceParam}

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: JMap[String, Serializable]): AccumuloDataStore = createDataStore(params)

  override def createDataStore(params: JMap[String, Serializable]): AccumuloDataStore = {
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
    AccumuloDataStoreFactory.ParameterInfo ++ Array(NamespaceParam, DeprecatedGeoServerPasswordParam)

  override def canProcess(params: java.util.Map[String,Serializable]): Boolean =
    AccumuloDataStoreFactory.canProcess(params)

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object AccumuloDataStoreFactory extends GeoMesaDataStoreInfo {

  import AccumuloDataStoreParams._

  import scala.collection.JavaConverters._

  override val DisplayName = "Accumulo (GeoMesa)"
  override val Description = "Apache Accumulo\u2122 distributed key/value store"

  override val ParameterInfo: Array[GeoMesaParam[_]] =
    Array(
      InstanceIdParam,
      ZookeepersParam,
      CatalogParam,
      UserParam,
      PasswordParam,
      KeytabPathParam,
      AuthsParam,
      VisibilitiesParam,
      QueryTimeoutParam,
      QueryThreadsParam,
      RecordThreadsParam,
      WriteThreadsParam,
      LooseBBoxParam,
      GenerateStatsParam,
      AuditQueriesParam,
      CachingParam,
      ForceEmptyAuthsParam
    )

  private val MockParam =
    new GeoMesaParam[java.lang.Boolean](
      "accumulo.mock",
      default = false,
      deprecatedKeys = Seq("useMock", "accumulo.useMock"))

  override def canProcess(params: java.util.Map[String, _ <: Serializable]): Boolean = {
    val hasConnector = ConnectorParam.lookupOpt(params).isDefined
    def hasConnection = InstanceIdParam.exists(params) && ZookeepersParam.exists(params) && UserParam.exists(params)
    def hasPassword = PasswordParam.exists(params) && !KeytabPathParam.exists(params)
    def hasKeytab = !PasswordParam.exists(params) && KeytabPathParam.exists(params) && isKerberosAvailable
    hasConnector || (hasConnection && (hasPassword || hasKeytab))
  }

  def buildAccumuloConnector(params: java.util.Map[String, _ <: Serializable]): Connector = {
    if (MockParam.lookup(params)) {
      throw new IllegalArgumentException("Mock Accumulo connections are no longer supported")
    }

    ConnectorParam.lookupOpt(params).getOrElse {
      val instance = InstanceIdParam.lookup(params)
      val zookeepers = ZookeepersParam.lookup(params)
      // NB: For those wanting to set this via JAVA_OPTS, this key is "instance.zookeeper.timeout" in Accumulo 1.6.x.
      val timeout = GeoMesaSystemProperties.getProperty(ClientProperty.INSTANCE_ZK_TIMEOUT.getKey)
      val conf = new ClientConfiguration().withInstance(instance).withZkHosts(zookeepers)
      if (timeout != null) {
        conf.`with`(ClientProperty.INSTANCE_ZK_TIMEOUT, timeout)
      }

      val user = UserParam.lookup(params)
      val password = PasswordParam.lookup(params)
      val keytabPath = KeytabPathParam.lookup(params)

      // build authentication token according to how we are authenticating
      val auth: AuthenticationToken = if (password != null && keytabPath == null) {
        new PasswordToken(password.getBytes("UTF-8"))
      } else if (password == null && keytabPath != null) {
        // explicitly enable SASL for kerberos connections
        // this shouldn't be required if Accumulo client.conf is set appropriately, but it doesn't seem to work
        conf.withSasl(true)
        // this API is only in Accumulo >=1.7, but canProcess should ensure this isn't actually invoked on earlier
        new KerberosToken(user, new java.io.File(keytabPath), true)
      } else {
        // should never reach here thanks to canProcess
        throw new IllegalArgumentException("Neither or both of password & keytabPath are set")
      }

      new ZooKeeperInstance(conf).getConnector(user, auth)
    }
  }

  def buildConfig(connector: Connector, params: JMap[String, Serializable]): AccumuloDataStoreConfig = {
    val catalog = CatalogParam.lookup(params)

    val authProvider = buildAuthsProvider(connector, params)
    val auditProvider = buildAuditProvider(params)
    val auditQueries = AuditQueriesParam.lookup(params).booleanValue()
    
    val auditService = new AccumuloAuditService(connector, authProvider, s"${catalog}_queries", auditQueries)

    val generateStats = GenerateStatsParam.lookup(params)
    val queryTimeout = QueryTimeoutParam.lookupOpt(params).map(_.toMillis)
    val visibility = VisibilitiesParam.lookupOpt(params).getOrElse("")

    val ns = NamespaceParam.lookupOpt(params)

    AccumuloDataStoreConfig(
      catalog,
      visibility,
      generateStats,
      authProvider,
      Some(auditService, auditProvider, AccumuloAuditService.StoreType),
      queryTimeout,
      LooseBBoxParam.lookup(params),
      CachingParam.lookup(params),
      WriteThreadsParam.lookup(params),
      QueryThreadsParam.lookup(params),
      RecordThreadsParam.lookup(params),
      ns
    )
  }

  def buildAuditProvider(params: JMap[String, Serializable]): AuditProvider = {
    Option(AuditProvider.Loader.load(params)).getOrElse {
      val provider = new ParamsAuditProvider
      provider.configure(params)
      provider
    }
  }

  def buildAuthsProvider(connector: Connector, params: JMap[String, Serializable]): AuthorizationsProvider = {
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

  // Kerberos is only available in Accumulo >= 1.7.
  // Note: doesn't confirm whether correctly configured for Kerberos e.g. core-site.xml on CLASSPATH
  def isKerberosAvailable: Boolean =
    AccumuloVersion.accumuloVersion != AccumuloVersion.V15 && AccumuloVersion.accumuloVersion != AccumuloVersion.V16
}
