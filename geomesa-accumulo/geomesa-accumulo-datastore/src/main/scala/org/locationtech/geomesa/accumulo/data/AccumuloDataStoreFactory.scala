/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/


package org.locationtech.geomesa.accumulo.data

import java.io.Serializable
import java.util.{Collections, Map => JMap}

import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty
import org.apache.accumulo.core.client.mock.{MockConnector, MockInstance}
import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, KerberosToken, PasswordToken}
import org.apache.accumulo.core.client.{ClientConfiguration, Connector, ZooKeeperInstance}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.accumulo.AccumuloVersion
import org.locationtech.geomesa.accumulo.audit.{AccumuloAuditService, ParamsAuditProvider}
import org.locationtech.geomesa.accumulo.security.AccumuloAuthsProvider
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.security
import org.locationtech.geomesa.utils.audit.AuditProvider
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties

import scala.collection.JavaConversions._

class AccumuloDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory._
  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: JMap[String, Serializable]): AccumuloDataStore = createDataStore(params)

  override def createDataStore(params: JMap[String, Serializable]): AccumuloDataStore = {
    import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.RichParam

    val connector = connParam.lookupOpt[Connector](params).getOrElse {
      buildAccumuloConnector(params, java.lang.Boolean.valueOf(mockParam.lookUp(params).asInstanceOf[String]))
    }
    val config = buildConfig(connector, params)
    new AccumuloDataStore(connector, config)
  }

  override def getDisplayName = AccumuloDataStoreFactory.DISPLAY_NAME

  override def getDescription = AccumuloDataStoreFactory.DESCRIPTION

  override def getParametersInfo =
    Array(
      instanceIdParam,
      zookeepersParam,
      userParam,
      passwordParam,
      keytabPathParam,
      tableNameParam,
      authsParam,
      visibilityParam,
      queryTimeoutParam,
      queryThreadsParam,
      recordThreadsParam,
      writeThreadsParam,
      looseBBoxParam,
      generateStatsParam,
      auditQueriesParam,
      cachingParam,
      forceEmptyAuthsParam
    )

  def canProcess(params: JMap[String,Serializable]): Boolean = AccumuloDataStoreFactory.canProcess(params)

  override def isAvailable = true

  override def getImplementationHints = null
}

object AccumuloDataStoreFactory {

  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams._
  import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.RichParam

  val DISPLAY_NAME = "Accumulo (GeoMesa)"
  val DESCRIPTION = "Apache Accumulo\u2122 distributed key/value store"

  def buildAccumuloConnector(params: JMap[String,Serializable], useMock: Boolean): Connector = {
    val instance = instanceIdParam.lookup[String](params)
    val user = userParam.lookup[String](params)
    val password = passwordParam.lookup[String](params)
    val keytabPath = keytabPathParam.lookup[String](params)

    // Build authentication token according to how we are authenticating
    val authToken : AuthenticationToken = if (password != null && keytabPath == null) {
      new PasswordToken(password.getBytes("UTF-8"))
    } else if (password == null && keytabPath != null) {
      if(!useMock) {
        // This API is only in Accumulo >=1.7, but canProcess should ensure this isn't actually invoked on earlier
        new KerberosToken(user, new java.io.File(keytabPath), true)
      } else {
        // Mock doesn't support Kerberos, so give it a pretend PasswordTOken
        new PasswordToken("".getBytes("UTF-8"))
      }
    } else {
      // Should never reach here thanks to canProcess
      throw new IllegalArgumentException("Neither or both of password & keytabPath are set")
    }

    if (useMock) {
      new MockInstance(instance).getConnector(user, authToken)
    } else {
      val zookeepers = zookeepersParam.lookup[String](params)
      // NB: For those wanting to set this via JAVA_OPTS, this key is "instance.zookeeper.timeout" in Accumulo 1.6.x.
      val timeout = GeoMesaSystemProperties.getProperty(ClientProperty.INSTANCE_ZK_TIMEOUT.getKey)
      val clientConfiguration = if (timeout !=  null) {
        new ClientConfiguration()
          .withInstance(instance)
          .withZkHosts(zookeepers)
          .`with`(ClientProperty.INSTANCE_ZK_TIMEOUT, timeout)
      } else {
        new ClientConfiguration().withInstance(instance).withZkHosts(zookeepers)
      }

      if (authToken.isInstanceOf[PasswordToken]) {
        // Using password authentication
        new ZooKeeperInstance(clientConfiguration).getConnector(user, authToken)
      } else {
        // Otherwise must be using Kerberos authentication, in which case we explicitly enable SASL.
        // This shouldn't be required if Accumulo client.conf is set appropriately, but it doesn't seem to work.
        new ZooKeeperInstance(clientConfiguration.withSasl((true))).getConnector(user, authToken)
      }
    }
  }

  def buildConfig(connector: Connector, params: JMap[String, Serializable]): AccumuloDataStoreConfig = {
    val catalog = tableNameParam.lookUp(params).asInstanceOf[String]

    val authProvider = buildAuthsProvider(connector, params)
    val auditProvider = buildAuditProvider(params)

    val auditQueries = !connector.isInstanceOf[MockConnector] &&
        (auditQueriesParam.lookupWithDefault[Boolean](params) || collectQueryStatsParam.lookupWithDefault[Boolean](params))
    val auditService = {
      val auditTable = GeoMesaFeatureIndex.formatSharedTableName(catalog, "queries")
      new AccumuloAuditService(connector, authProvider, auditTable, auditQueries)
    }

    val generateStats = GeoMesaDataStoreFactory.generateStats(params)
    val queryTimeout = GeoMesaDataStoreFactory.queryTimeout(params)
    val visibility = visibilityParam.lookupOpt[String](params).getOrElse("")

    AccumuloDataStoreConfig(
      catalog,
      visibility,
      generateStats,
      authProvider,
      Some(auditService, auditProvider, AccumuloAuditService.StoreType),
      queryTimeout,
      looseBBoxParam.lookupWithDefault(params),
      cachingParam.lookupWithDefault(params),
      writeThreadsParam.lookupWithDefault(params),
      queryThreadsParam.lookupWithDefault(params),
      recordThreadsParam.lookupWithDefault(params)
    )
  }

  def buildAuditProvider(params: JMap[String, Serializable]): AuditProvider = {
    Option(AuditProvider.Loader.load(params)).getOrElse {
      val provider = new ParamsAuditProvider
      provider.configure(params)
      provider
    }
  }

  def buildAuthsProvider(connector: Connector, params: JMap[String, Serializable]): AccumuloAuthsProvider = {
    val forceEmptyOpt: Option[java.lang.Boolean] = forceEmptyAuthsParam.lookupOpt[java.lang.Boolean](params)
    val forceEmptyAuths = forceEmptyOpt.getOrElse(java.lang.Boolean.FALSE).asInstanceOf[Boolean]

    // convert the connector authorizations into a string array - this is the maximum auths this connector can support
    val securityOps = connector.securityOperations
    val masterAuths = securityOps.getUserAuthorizations(connector.whoami)
    val masterAuthsStrings = masterAuths.map(b => new String(b))

    // get the auth params passed in as a comma-delimited string
    val configuredAuths = authsParam.lookupOpt[String](params).getOrElse("").split(",").filter(s => !s.isEmpty)

    // verify that the configured auths are valid for the connector we are using (fail-fast)
    if (!connector.isInstanceOf[MockConnector]) {
      val invalidAuths = configuredAuths.filterNot(masterAuthsStrings.contains)
      if (invalidAuths.nonEmpty) {
        throw new IllegalArgumentException(s"The authorizations '${invalidAuths.mkString(",")}' " +
          "are not valid for the Accumulo connection being used")
      }
    }

    // if the caller provided any non-null string for authorizations, use it;
    // otherwise, grab all authorizations to which the Accumulo user is entitled
    if (configuredAuths.length != 0 && forceEmptyAuths) {
      throw new IllegalArgumentException("Forcing empty auths is checked, but explicit auths are provided")
    }
    val auths: List[String] =
      if (forceEmptyAuths || configuredAuths.length > 0) configuredAuths.toList
      else masterAuthsStrings.toList

    new AccumuloAuthsProvider(security.getAuthorizationsProvider(params, auths))
  }

  // Kerberos is only available in Accumulo >= 1.7.
  // Note: doesn't confirm whether correctly configured for Kerberos e.g. core-site.xml on CLASSPATH
  def isKerberosAvailable: Boolean = {
    AccumuloVersion.accumuloVersion != AccumuloVersion.V15 && AccumuloVersion.accumuloVersion != AccumuloVersion.V16
  }

  def canProcess(params: JMap[String,Serializable]): Boolean = {

    // Determine which parameters are set. Those which are required according to the parameter definition will throw an
    // exception on lookup, so need the extra containsKey check.
    val hasConnection = connParam.lookup[String](params) != null
    val hasInstanceId = params.containsKey(instanceIdParam.key) && instanceIdParam.lookup[String](params) != null
    val hasZookeepers = params.containsKey(zookeepersParam.key) && zookeepersParam.lookup[String](params) != null
    val hasUser = params.containsKey(userParam.key) && userParam.lookup[String](params) != null
    val hasPassword = params.containsKey(passwordParam.key) && passwordParam.lookup[String](params) != null
    val hasKeytabPath = params.containsKey(keytabPathParam.key) && keytabPathParam.lookup[String](params) != null

    val passwordAuth = hasInstanceId && hasZookeepers && hasUser && hasPassword && !hasKeytabPath
    val kerberosAuth = hasInstanceId && hasZookeepers && hasUser && !hasPassword && hasKeytabPath && isKerberosAvailable

    hasConnection || passwordAuth || kerberosAuth
  }
}

// keep params in a separate object so we don't require accumulo classes on the build path to access it
object AccumuloDataStoreParams {
  val connParam              = new Param("connector", classOf[Connector], "Accumulo connector", false)
  val instanceIdParam        = new Param("instanceId", classOf[String], "Accumulo Instance ID", true)
  val zookeepersParam        = new Param("zookeepers", classOf[String], "Zookeepers", true)
  val userParam              = new Param("user", classOf[String], "Accumulo user", true)
  val passwordParam          = new Param("password", classOf[String], "Accumulo password", false, null, Collections.singletonMap(Parameter.IS_PASSWORD, java.lang.Boolean.TRUE))
  val keytabPathParam        = new Param("keytabPath", classOf[String], "Path to keytab file", false)
  val authsParam             = org.locationtech.geomesa.security.AuthsParam
  val visibilityParam        = new Param("visibilities", classOf[String], "Default Accumulo visibilities to apply to all written data", false)
  val tableNameParam         = new Param("tableName", classOf[String], "Accumulo catalog table name", true)
  val queryTimeoutParam      = GeoMesaDataStoreFactory.QueryTimeoutParam
  val queryThreadsParam      = GeoMesaDataStoreFactory.QueryThreadsParam
  val recordThreadsParam     = new Param("recordThreads", classOf[Integer], "The number of threads to use for record retrieval", false, 10)
  val writeThreadsParam      = new Param("writeThreads", classOf[Integer], "The number of threads to use for writing records", false, 10)
  val looseBBoxParam         = GeoMesaDataStoreFactory.LooseBBoxParam
  val generateStatsParam     = GeoMesaDataStoreFactory.GenerateStatsParam
  val collectQueryStatsParam = new Param("collectQueryStats", classOf[java.lang.Boolean], "Collect statistics on queries being run", false, true)
  val auditQueriesParam      = GeoMesaDataStoreFactory.AuditQueriesParam
  val cachingParam           = GeoMesaDataStoreFactory.CachingParam
  val mockParam              = new Param("useMock", classOf[String], "Use a mock connection (for testing)", false)
  val forceEmptyAuthsParam   = org.locationtech.geomesa.security.ForceEmptyAuthsParam
}