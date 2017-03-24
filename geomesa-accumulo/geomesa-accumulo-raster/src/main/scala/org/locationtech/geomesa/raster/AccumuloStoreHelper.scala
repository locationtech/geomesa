/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster

import java.io.Serializable
import java.util.{ServiceLoader, Map => JMap}

import org.apache.accumulo.core.client.mock.{MockConnector, MockInstance}
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.client.{Connector, ZooKeeperInstance}
import org.locationtech.geomesa.security._

import scala.collection.JavaConversions._

//TODO: WCS: refactor this and AccumuloDataStoreFactory to remove duplication, etc...
// GEOMESA-570
object AccumuloStoreHelper {
  import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams._

  def getVisibility(params: JMap[String,Serializable]): String = {
    val visStr = visibilityParam.lookUp(params).asInstanceOf[String]
    if (visStr == null) "" else visStr
  }
  
  def getAuthorization(params: JMap[String,Serializable]): String = {
    val auths = authsParam.lookUp(params).asInstanceOf[String]
    if (auths == null) "" else auths
  }

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
    val zookeepers = zookeepersParam.lookUp(params).asInstanceOf[String]
    val instance = instanceIdParam.lookUp(params).asInstanceOf[String]
    val user = userParam.lookUp(params).asInstanceOf[String]
    val password = passwordParam.lookUp(params).asInstanceOf[String]

    val authToken = new PasswordToken(password.getBytes)
    if(useMock) new MockInstance(instance).getConnector(user, authToken)
    else new ZooKeeperInstance(instance, zookeepers).getConnector(user, authToken)
  }

  def getAuthorizations(params: JMap[String,Serializable], connector: Connector): List[String] = {
    import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.RichParam

    // convert the connector authorizations into a string array - this is the maximum auths this connector can support
    val securityOps = connector.securityOperations
    val masterAuths = securityOps.getUserAuthorizations(connector.whoami)
    val masterAuthsStrings = masterAuths.map(b => new String(b))

    // get the auth params passed in as a comma-delimited string
    val configuredAuths = authsParam.lookupOpt[String](params).getOrElse("").split(",").filter(s => !s.isEmpty)

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

  def getAuthorizationsProvider(params: JMap[String,Serializable], connector: Connector): AuthorizationsProvider = {
    val auths = getAuthorizations(params, connector)
    getAuthorizationsProvider(auths, connector)
  }

  def getAuthorizationsProvider(auths: Seq[String], connector: Connector): AuthorizationsProvider = {
    // we wrap the authorizations provider in one that will filter based on the max auths configured for this store
    val authorizationsProvider = new FilteringAuthorizationsProvider ({
      val providers = ServiceLoader.load(classOf[AuthorizationsProvider]).toBuffer
      GEOMESA_AUTH_PROVIDER_IMPL.option match {
        case Some(prop) =>
          if (classOf[DefaultAuthorizationsProvider].getName == prop)
            new DefaultAuthorizationsProvider
          else
            providers.find(_.getClass.getName == prop)
              .getOrElse {
                val message =
                  s"The service provider class '$prop' specified by " +
                    s"${AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY} could not be loaded"
                throw new IllegalArgumentException(message)
              }
        case None =>
          providers.length match {
            case 0 => new DefaultAuthorizationsProvider
            case 1 => providers.head
            case _ =>
              val message =
                "Found multiple AuthorizationsProvider implementations. Please specify the one " +
                  "to use with the system property " +
                  s"'${AuthorizationsProvider.AUTH_PROVIDER_SYS_PROPERTY}' :: " +
                  s"${providers.map(_.getClass.getName).mkString(", ")}"
              throw new IllegalStateException(message)
          }
      }
    })

    // update the authorizations in the parameters and then configure the auth provider
    // we copy the map so as not to modify the original
    val modifiedParams = Map(authsParam.key -> auths.mkString(","))
    authorizationsProvider.configure(modifiedParams)
    authorizationsProvider
  }
}
