/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.accumulo.data

import java.io.Serializable
import java.util.{Map => JMap, Collections}

import org.apache.accumulo.core.client.mock.{MockConnector, MockInstance}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{Parameter, DataStoreFactorySpi}
import org.locationtech.geomesa.accumulo.GeomesaSystemProperties
import org.locationtech.geomesa.accumulo.stats.{ParamsAuditProvider, StatWriter}
import org.locationtech.geomesa.security

import scala.collection.JavaConversions._

class AccumuloDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory._
  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams._

  // this is a pass-through required of the ancestor interface
  def createNewDataStore(params: JMap[String, Serializable]) = createDataStore(params)

  def createDataStore(params: JMap[String, Serializable]) = {

    val visibility = visibilityParam.lookupOpt[String](params).getOrElse("")

    val tableName = tableNameParam.lookUp(params).asInstanceOf[String]
    val useMock = java.lang.Boolean.valueOf(mockParam.lookUp(params).asInstanceOf[String])
    val connector = connParam.lookupOpt[Connector](params).getOrElse(buildAccumuloConnector(params, useMock))

    val forceOpt: Option[java.lang.Boolean] = forceAuthsParam.lookupOpt[java.lang.Boolean](params)
    val forceAuths = (forceOpt.getOrElse(java.lang.Boolean.FALSE)).asInstanceOf[Boolean]

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
    if (configuredAuths.size != 0 && !forceAuths) {
      throw new IllegalArgumentException("Forcing provided auths is un-checked, but expclicit auths are provided")
    }
    val auths: List[String] = if (forceAuths) configuredAuths.toList else masterAuthsStrings.toList

    val authProvider = security.getAuthorizationsProvider(params, auths)
    val auditProvider = security.getAuditProvider(params).getOrElse {
      val provider = new ParamsAuditProvider
      provider.configure(params)
      provider
    }

    val queryTimeout = queryTimeoutParam.lookupOpt[Int](params).map(i => i * 1000L).orElse {
      GeomesaSystemProperties.QueryProperties.QUERY_TIMEOUT_MILLIS.option.map(_.toLong)
    }
    val config = AccumuloDataStoreConfig(
      queryTimeout,
      queryThreadsParam.lookupWithDefault(params),
      recordThreadsParam.lookupWithDefault(params),
      writeThreadsParam.lookupWithDefault(params),
      cachingParam.lookupWithDefault(params),
      looseBBoxParam.lookupWithDefault(params)
    )

    // stats defaults to true if not specified
    val collectStats = !useMock && statsParam.lookupWithDefault[Boolean](params)

    if (collectStats) {
      new AccumuloDataStore(connector, tableName, authProvider, auditProvider, visibility, config) with StatWriter
    } else {
      new AccumuloDataStore(connector, tableName, authProvider, auditProvider, visibility, config)
    }
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
      statsParam,
      cachingParam,
      forceAuthsParam
    )

  def canProcess(params: JMap[String,Serializable]) = AccumuloDataStoreFactory.canProcess(params)

  override def isAvailable = true

  override def getImplementationHints = null
}

object AccumuloDataStoreFactory {

  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams._

  val DISPLAY_NAME = "Accumulo (GeoMesa)"
  val DESCRIPTION = "Apache Accumulo\u2122 distributed key/value store"

  implicit class RichParam(val p: Param) extends AnyVal {
    def lookup[T](params: JMap[String, Serializable]): T = p.lookUp(params).asInstanceOf[T]
    def lookupOpt[T](params: JMap[String, Serializable]): Option[T] = Option(p.lookup[T](params))
    def lookupWithDefault[T](params: JMap[String, Serializable]): T =
      p.lookupOpt[T](params).getOrElse(p.getDefaultValue.asInstanceOf[T])
  }

  def buildAccumuloConnector(params: JMap[String,Serializable], useMock: Boolean): Connector = {
    val zookeepers = zookeepersParam.lookup[String](params)
    val instance = instanceIdParam.lookup[String](params)
    val user = userParam.lookup[String](params)
    val password = passwordParam.lookup[String](params)

    val authToken = new PasswordToken(password.getBytes("UTF-8"))
    if (useMock) {
      new MockInstance(instance).getConnector(user, authToken)
    } else {
      new ZooKeeperInstance(instance, zookeepers).getConnector(user, authToken)
    }
  }

  def canProcess(params: JMap[String,Serializable]): Boolean =
    params.containsKey(instanceIdParam.key) || params.containsKey(connParam.key)
}

// keep params in a separate object so we don't require accumulo classes on the build path to access it
object AccumuloDataStoreParams {
  val connParam           = new Param("connector", classOf[Connector], "Accumulo connector", false)
  val instanceIdParam     = new Param("instanceId", classOf[String], "Accumulo Instance ID", true)
  val zookeepersParam     = new Param("zookeepers", classOf[String], "Zookeepers", true)
  val userParam           = new Param("user", classOf[String], "Accumulo user", true)
  val passwordParam       = new Param("password", classOf[String], "Accumulo password", true, null, Collections.singletonMap(Parameter.IS_PASSWORD, java.lang.Boolean.TRUE))
  val authsParam          = org.locationtech.geomesa.security.authsParam
  val visibilityParam     = new Param("visibilities", classOf[String], "Accumulo visibilities to apply to all written data", false)
  val tableNameParam      = new Param("tableName", classOf[String], "Accumulo catalog table name", true)
  val queryTimeoutParam   = new Param("queryTimeout", classOf[Integer], "The max time a query will be allowed to run before being killed, in seconds", false)
  val queryThreadsParam   = new Param("queryThreads", classOf[Integer], "The number of threads to use per query", false, 8)
  val recordThreadsParam  = new Param("recordThreads", classOf[Integer], "The number of threads to use for record retrieval", false, 10)
  val writeThreadsParam   = new Param("writeThreads", classOf[Integer], "The number of threads to use for writing records", false, 10)
  val looseBBoxParam      = new Param("looseBoundingBox", classOf[java.lang.Boolean], "Use loose bounding boxes - queries will be faster but may return extraneous results", false, true)
  val statsParam          = new Param("collectStats", classOf[java.lang.Boolean], "Toggle collection of statistics", false, true)
  val cachingParam        = new Param("caching", classOf[java.lang.Boolean], "Toggle caching of results", false, false)
  val mockParam           = new Param("useMock", classOf[String], "Use a mock connection (for testing)", false)
  val forceAuthsParam     = new Param("forceAuths", classOf[java.lang.Boolean], "When checked, force the data store to use exactly the Accumulo authorizations provided", false, false)
}