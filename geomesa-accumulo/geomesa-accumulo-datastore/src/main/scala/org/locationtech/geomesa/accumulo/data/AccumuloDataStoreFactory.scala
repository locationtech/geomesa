/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/


package org.locationtech.geomesa.accumulo.data

import java.io.Serializable
import java.util.{Map => JMap, Collections}

import org.apache.accumulo.core.client.mock.{MockConnector, MockInstance}
import org.apache.accumulo.core.client.security.tokens.{AuthenticationToken, PasswordToken}
import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{Parameter, DataStoreFactorySpi}
import org.locationtech.geomesa.accumulo.stats.StatWriter
import org.locationtech.geomesa.security

import scala.collection.JavaConversions._

class AccumuloDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory._
  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.params._

  // this is a pass-through required of the ancestor interface
  def createNewDataStore(params: JMap[String, Serializable]) = createDataStore(params)

  def createDataStore(params: JMap[String, Serializable]) = {

    val visStr = visibilityParam.lookUp(params).asInstanceOf[String]

    val visibility =
      if (visStr == null)
        ""
      else
        visStr

    val tableName = tableNameParam.lookUp(params).asInstanceOf[String]
    val useMock = java.lang.Boolean.valueOf(mockParam.lookUp(params).asInstanceOf[String])
    val (connector, token) =
      if (params.containsKey(connParam.key)) (connParam.lookUp(params).asInstanceOf[Connector], null)
      else buildAccumuloConnector(params, useMock)

    // convert the connector authorizations into a string array - this is the maximum auths this connector can support
    val securityOps = connector.securityOperations
    val masterAuths = securityOps.getUserAuthorizations(connector.whoami)
    val masterAuthsStrings = masterAuths.map(b => new String(b))

    // get the auth params passed in as a comma-delimited string
    val configuredAuths = authsParam.lookupOpt[String](params).getOrElse("").split(",").filter(s => !s.isEmpty)

    // verify that the configured auths are valid for the connector we are using (fail-fast)
    configuredAuths.foreach(a => if(!masterAuthsStrings.contains(a) && !connector.isInstanceOf[MockConnector])
             throw new IllegalArgumentException(s"The authorization '$a' is not valid for the Accumulo connector being used"))

    // if no auths are specified we default to the connector auths
    // TODO would it be safer to default to no auths?
    val auths: List[String] =
      if (!configuredAuths.isEmpty)
        configuredAuths.toList
      else
        masterAuthsStrings.toList

    val authorizationsProvider = security.getAuthorizationsProvider(params, auths)

    // stats defaults to true if not specified
    val collectStats = !useMock &&
        Option(statsParam.lookUp(params)).map(_.toString.toBoolean).forall(_ == true)
    // caching defaults to false if not specified
    val caching = Option(cachingParam.lookUp(params)).exists(_.toString.toBoolean)

    if (collectStats) {
      new AccumuloDataStore(connector,
        token,
        tableName,
        authorizationsProvider,
        visibility,
        queryTimeoutParam.lookupOpt[Int](params).map(i => i * 1000L),
        queryThreadsParam.lookupOpt(params),
        recordThreadsParam.lookupOpt(params),
        writeThreadsParam.lookupOpt(params),
        caching) with StatWriter
    } else {
      new AccumuloDataStore(connector,
        token,
        tableName,
        authorizationsProvider,
        visibility,
        queryTimeoutParam.lookupOpt[Int](params).map(i => i * 1000L),
        queryThreadsParam.lookupOpt(params),
        recordThreadsParam.lookupOpt(params),
        writeThreadsParam.lookupOpt(params),
        caching)
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
      authsParam,
      visibilityParam,
      tableNameParam,
      statsParam,
      cachingParam,
      queryTimeoutParam,
      queryThreadsParam,
      recordThreadsParam,
      writeThreadsParam
    )

  def canProcess(params: JMap[String,Serializable]) = AccumuloDataStoreFactory.canProcess(params)

  override def isAvailable = true

  override def getImplementationHints = null
}

object AccumuloDataStoreFactory {

  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreFactory.params._

  val DISPLAY_NAME = "Accumulo (GeoMesa)"
  val DESCRIPTION = "Apache Accumulo\u2122 distributed key/value store"

  implicit class RichParam(val p: Param) {
    def lookupOpt[A](params: JMap[String, Serializable]) =
      Option(p.lookUp(params)).asInstanceOf[Option[A]]
  }

  object params {
    val connParam           = new Param("connector", classOf[Connector], "Accumulo connector", false)
    val instanceIdParam     = new Param("instanceId", classOf[String], "Accumulo Instance ID", true)
    val zookeepersParam     = new Param("zookeepers", classOf[String], "Zookeepers", true)
    val userParam           = new Param("user", classOf[String], "Accumulo user", true)
    val passwordParam       = new Param("password", classOf[String], "Accumulo password", true, null, Collections.singletonMap(Parameter.IS_PASSWORD, java.lang.Boolean.TRUE))
    val authsParam          = org.locationtech.geomesa.security.authsParam
    val visibilityParam     = new Param("visibilities", classOf[String], "Accumulo visibilities to apply to all written data", false)
    val tableNameParam      = new Param("tableName", classOf[String], "Accumulo catalog table name", true)
    val queryTimeoutParam   = new Param("queryTimeout", classOf[Integer], "The max time a query will be allowed to run before being killed, in seconds", false)
    val queryThreadsParam   = new Param("queryThreads", classOf[Integer], "The number of threads to use per query", false)
    val recordThreadsParam  = new Param("recordThreads", classOf[Integer], "The number of threads to use for record retrieval", false)
    val writeThreadsParam   = new Param("writeThreads", classOf[Integer], "The number of threads to use for writing records", false)
    val statsParam          = new Param("collectStats", classOf[java.lang.Boolean], "Toggle collection of statistics", false)
    val cachingParam        = new Param("caching", classOf[java.lang.Boolean], "Toggle caching of results", false)
    val mockParam           = new Param("useMock", classOf[String], "Use a mock connection (for testing)", false)
  }

  def buildAccumuloConnector(params: JMap[String,Serializable], useMock: Boolean): (Connector, AuthenticationToken) = {
    val zookeepers = zookeepersParam.lookUp(params).asInstanceOf[String]
    val instance = instanceIdParam.lookUp(params).asInstanceOf[String]
    val user = userParam.lookUp(params).asInstanceOf[String]
    val password = passwordParam.lookUp(params).asInstanceOf[String]

    val authToken = new PasswordToken(password.getBytes)
    if(useMock) {
      (new MockInstance(instance).getConnector(user, authToken), authToken)
    } else {
      (new ZooKeeperInstance(instance, zookeepers).getConnector(user, authToken), authToken)
    }
  }

  /**
   * Return true/false whether or not the catalog referenced by these params exists
   * already (aka the accumulo table has been created)
   */
  def catalogExists(params: JMap[String,Serializable], useMock: Boolean): Boolean = {
    val (conn, _) = buildAccumuloConnector(params, useMock)
    conn.tableOperations().exists(tableNameParam.lookUp(params).asInstanceOf[String])
  }

  def canProcess(params: JMap[String,Serializable]): Boolean =
    params.containsKey(instanceIdParam.key) || params.containsKey(connParam.key)
}
