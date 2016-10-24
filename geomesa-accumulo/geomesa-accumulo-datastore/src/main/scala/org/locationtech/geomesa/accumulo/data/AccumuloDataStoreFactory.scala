/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.accumulo.data

import java.io.Serializable
import java.util.{Collections, Map => JMap}

import org.apache.accumulo.core.client.ClientConfiguration.ClientProperty
import org.apache.accumulo.core.client.mock.{MockConnector, MockInstance}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{ClientConfiguration, Connector, ZooKeeperInstance}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties
import org.locationtech.geomesa.accumulo.data.stats.usage.ParamsAuditProvider
import org.locationtech.geomesa.security
import org.locationtech.geomesa.security.AuthorizationsProvider

import scala.collection.JavaConversions._
import scala.collection.immutable.HashMap

class AccumuloDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory._
  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams._

  // this is a pass-through required of the ancestor interface
  def createNewDataStore(params: JMap[String, Serializable]) = createDataStore(params)

  def createDataStore(params: JMap[String, Serializable]) = {
    val visibility = visibilityParam.lookupOpt[String](params).getOrElse("")

    val tableName = tableNameParam.lookUp(params).asInstanceOf[String]
    val connector = connParam.lookupOpt[Connector](params).getOrElse {
      buildAccumuloConnector(params, java.lang.Boolean.valueOf(mockParam.lookUp(params).asInstanceOf[String]))
    }

    val authProvider = buildAuthsProvider(connector, params)
    val auditProvider = buildAuditProvider(params)

    val config = buildConfig(connector, params)

    new AccumuloDataStore(connector, tableName, authProvider, auditProvider, visibility, config)
  }

  override def getDisplayName = AccumuloDataStoreFactory.DISPLAY_NAME

  override def getDescription = AccumuloDataStoreFactory.DESCRIPTION

  override def getParametersInfo =
    Array(
      instanceIdParam,
      zookeepersParam,
      userParam,
      passwordParam,
      tableNameParam,
      authsParam,
      visibilityParam,
      queryTimeoutParam,
      queryThreadsParam,
      recordThreadsParam,
      writeThreadsParam,
      looseBBoxParam,
      generateStatsParam,
      collectQueryStatsParam,
      cachingParam,
      forceEmptyAuthsParam
    )

  def canProcess(params: JMap[String,Serializable]) = AccumuloDataStoreFactory.canProcess(params)

  override def isAvailable = true

  override def getImplementationHints = null
}

object AccumuloDataStoreFactory {

  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams._

  val DISPLAY_NAME = "Accumulo (GeoMesa)"
  val DESCRIPTION = "Apache Accumulo\u2122 distributed key/value store"

  val EmptyParams: JMap[String, Serializable] = new HashMap[String, Serializable]()

  implicit class RichParam(val p: Param) extends AnyVal {
    def lookup[T](params: JMap[String, Serializable]): T = p.lookUp(params).asInstanceOf[T]
    def lookupOpt[T](params: JMap[String, Serializable]): Option[T] = Option(p.lookup[T](params))
    def lookupWithDefault[T](params: JMap[String, Serializable]): T =
      p.lookupOpt[T](params).getOrElse(p.getDefaultValue.asInstanceOf[T])
  }

  def buildAccumuloConnector(params: JMap[String,Serializable], useMock: Boolean): Connector = {
    val instance = instanceIdParam.lookup[String](params)
    val user = userParam.lookup[String](params)
    val password = passwordParam.lookup[String](params)

    val authToken = new PasswordToken(password.getBytes("UTF-8"))
    if (useMock) {
      new MockInstance(instance).getConnector(user, authToken)
    } else {
      val zookeepers = zookeepersParam.lookup[String](params)
      // NB: For those wanting to set this via JAVA_OPTS, this key is "instance.zookeeper.timeout" in Accumulo 1.6.x.
      val clientConfiguration = if (System.getProperty(ClientProperty.INSTANCE_ZK_TIMEOUT.getKey) != null) {
        new ClientConfiguration()
          .withInstance(instance)
          .withZkHosts(zookeepers)
          .`with`(ClientProperty.INSTANCE_ZK_TIMEOUT, System.getProperty(ClientProperty.INSTANCE_ZK_TIMEOUT.getKey))
      } else {
        new ClientConfiguration().withInstance(instance).withZkHosts(zookeepers)
      }

      new ZooKeeperInstance(clientConfiguration).getConnector(user, authToken)
    }
  }

  def buildConfig(connector: Connector, params: JMap[String, Serializable] = EmptyParams): AccumuloDataStoreConfig = {
    val queryTimeout = queryTimeoutParam.lookupOpt[Int](params).map(i => i * 1000L).orElse {
      GeomesaSystemProperties.QueryProperties.QUERY_TIMEOUT_MILLIS.option.map(_.toLong)
    }
    val collectQueryStats =
      !connector.isInstanceOf[MockConnector] && collectQueryStatsParam.lookupWithDefault[Boolean](params)

    AccumuloDataStoreConfig(
      queryTimeout,
      queryThreadsParam.lookupWithDefault(params),
      recordThreadsParam.lookupWithDefault(params),
      writeThreadsParam.lookupWithDefault(params),
      generateStatsParam.lookupWithDefault[Boolean](params),
      collectQueryStats,
      cachingParam.lookupWithDefault(params),
      looseBBoxParam.lookupWithDefault(params)
    )
  }

  def buildAuditProvider(params: JMap[String, Serializable] = EmptyParams) = {
    security.getAuditProvider(params).getOrElse {
      val provider = new ParamsAuditProvider
      provider.configure(params)
      provider
    }
  }

  def buildAuthsProvider(connector: Connector, params: JMap[String, Serializable] = EmptyParams): AuthorizationsProvider = {
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

    security.getAuthorizationsProvider(params, auths)
  }

  def canProcess(params: JMap[String,Serializable]): Boolean =
    params.containsKey(instanceIdParam.key) || params.containsKey(connParam.key)
}

// keep params in a separate object so we don't require accumulo classes on the build path to access it
object AccumuloDataStoreParams {
  val connParam              = new Param("connector", classOf[Connector], "Accumulo connector", false)
  val instanceIdParam        = new Param("instanceId", classOf[String], "Accumulo Instance ID", true)
  val zookeepersParam        = new Param("zookeepers", classOf[String], "Zookeepers", true)
  val userParam              = new Param("user", classOf[String], "Accumulo user", true)
  val passwordParam          = new Param("password", classOf[String], "Accumulo password", true, null, Collections.singletonMap(Parameter.IS_PASSWORD, java.lang.Boolean.TRUE))
  val authsParam             = org.locationtech.geomesa.security.authsParam
  val visibilityParam        = new Param("visibilities", classOf[String], "Default Accumulo visibilities to apply to all written data", false)
  val tableNameParam         = new Param("tableName", classOf[String], "Accumulo catalog table name", true)
  val queryTimeoutParam      = new Param("queryTimeout", classOf[Integer], "The max time a query will be allowed to run before being killed, in seconds", false)
  val queryThreadsParam      = new Param("queryThreads", classOf[Integer], "The number of threads to use per query", false, 8)
  val recordThreadsParam     = new Param("recordThreads", classOf[Integer], "The number of threads to use for record retrieval", false, 10)
  val writeThreadsParam      = new Param("writeThreads", classOf[Integer], "The number of threads to use for writing records", false, 10)
  val looseBBoxParam         = new Param("looseBoundingBox", classOf[java.lang.Boolean], "Use loose bounding boxes - queries will be faster but may return extraneous results", false, true)
  val generateStatsParam     = new Param("generateStats", classOf[java.lang.Boolean], "Generate data statistics for improved query planning", false, true)
  val collectQueryStatsParam = new Param("collectQueryStats", classOf[java.lang.Boolean], "Collect statistics on queries being run", false, true)
  val cachingParam           = new Param("caching", classOf[java.lang.Boolean], "Cache the results of queries for faster repeated searches. Warning: large result sets can swamp memory", false, false)
  val mockParam              = new Param("useMock", classOf[String], "Use a mock connection (for testing)", false)
  val forceEmptyAuthsParam   = new Param("forceEmptyAuths", classOf[java.lang.Boolean], "Default to using no authorizations during queries, instead of using the connection user's authorizations", false, false)
}