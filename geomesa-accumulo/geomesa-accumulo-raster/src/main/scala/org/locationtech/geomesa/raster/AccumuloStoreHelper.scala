/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster

import java.io.Serializable
import java.util.{Collections, Map => JMap}

import org.apache.accumulo.core.client.mock.{MockConnector, MockInstance}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.locationtech.geomesa.security.AuthorizationsProvider

import scala.collection.JavaConversions._

//TODO: WCS: refactor this and AccumuloDataStoreFactory to remove duplication, etc...
// GEOMESA-570
object AccumuloStoreHelper {
  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams._

  def getVisibility(params: JMap[String,Serializable]): String = VisibilitiesParam.lookupOpt(params).getOrElse("")
  
  def getAuthorization(params: JMap[String,Serializable]): String = AuthsParam.lookupOpt(params).getOrElse("")

  def buildAccumuloConnector(user: String,
                             password: String,
                             instance: String,
                             zookeepers: String,
                             useMock: Boolean = false): Connector = {
    val authToken = new PasswordToken(password.getBytes)
    if(useMock) new MockInstance(instance).getConnector(user, authToken)
    else new ZooKeeperInstance(instance, zookeepers).getConnector(user, authToken)
  }

  def buildAccumuloConnector(params: JMap[String,Serializable], useMock: Boolean): Connector = {
    val zookeepers = ZookeepersParam.lookup(params)
    val instance = InstanceIdParam.lookup(params)
    val user = UserParam.lookup(params)
    val password = PasswordParam.lookup(params)

    val authToken = new PasswordToken(password.getBytes)
    if(useMock) new MockInstance(instance).getConnector(user, authToken)
    else new ZooKeeperInstance(instance, zookeepers).getConnector(user, authToken)
  }

  def getAuthorizations(params: JMap[String,Serializable], connector: Connector): List[String] = {
    // convert the connector authorizations into a string array - this is the maximum auths this connector can support
    val securityOps = connector.securityOperations
    val masterAuths = securityOps.getUserAuthorizations(connector.whoami)
    val masterAuthsStrings = masterAuths.map(b => new String(b))

    // get the auth params passed in as a comma-delimited string
    val configuredAuths = AuthsParam.lookupOpt(params).getOrElse("").split(",").filter(s => !s.isEmpty)

    // verify that the configured auths are valid for the connector we are using (fail-fast)
    if (!connector.isInstanceOf[MockConnector])
      configuredAuths.foreach(a => if(!masterAuthsStrings.contains(a))
        throw new IllegalArgumentException(s"The authorization '$a' is not valid for the Accumulo connector being used"))

    // if no auths are specified we default to the connector auths
    if (!configuredAuths.isEmpty)
      configuredAuths.toList
    else
      masterAuthsStrings.toList
  }

  def getAuthorizationsProvider(params: JMap[String,Serializable], connector: Connector): AuthorizationsProvider =
    AuthorizationsProvider.apply(Collections.emptyMap(), getAuthorizations(params, connector))

  def getAuthorizationsProvider(auths: Seq[String], connector: Connector): AuthorizationsProvider =
    AuthorizationsProvider.apply(Collections.emptyMap(), auths)
}
