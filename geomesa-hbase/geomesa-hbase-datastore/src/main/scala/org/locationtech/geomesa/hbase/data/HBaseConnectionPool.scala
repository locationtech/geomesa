/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.io.{ByteArrayInputStream, Closeable}
import java.nio.charset.StandardCharsets
import java.security.PrivilegedExceptionAction

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.{HBaseGeoMesaKeyTab, HBaseGeoMesaPrincipal}
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams.{ConfigPathsParam, ConfigsParam, ConnectionParam, ZookeeperParam}
import org.locationtech.geomesa.hbase.utils.HBaseVersions
import org.locationtech.geomesa.utils.io.{CloseWithLogging, HadoopUtils}

object HBaseConnectionPool extends LazyLogging {

  import scala.collection.JavaConverters._

  private var userCheck: Option[User] = _
  private var kerberosTicket: Option[Closeable] = None

  private val configs = Caffeine.newBuilder().build(
    new CacheLoader[(Option[String], Option[String], Option[String]), Configuration] {

      // add common resources from system property - lazy to allow object initialization if there's an error
      private lazy val configuration = {
        val base = HBaseConfiguration.create()
        HBaseDataStoreFactory.ConfigPathProperty.option.foreach(addResources(base, _))
        base
      }

      override def load(key: (Option[String], Option[String], Option[String])): Configuration = {
        val (zookeepers, paths, props) = key
        val conf = new Configuration(configuration)
        // add the explicit props first, they may be needed for loading the path resources
        props.foreach(xml => conf.addResource(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))))
        paths.foreach(addResources(conf, _))
        zookeepers.foreach(zk => conf.set(HConstants.ZOOKEEPER_QUORUM, zk))
        if (zookeepers.isEmpty && conf.get(HConstants.ZOOKEEPER_QUORUM) == "localhost") {
          logger.warn("HBase connection is set to localhost - " +
              "this may indicate that 'hbase-site.xml' is not on the classpath")
        }
        configureSecurity(conf)
        conf
      }

      private def addResources(conf: Configuration, paths: String): Unit =
        paths.split(',').map(_.trim).filterNot(_.isEmpty).foreach(HadoopUtils.addResource(conf, _))
    }
  )

  private val connections = Caffeine.newBuilder().build(
    new CacheLoader[(Configuration, Boolean), Connection] {
      override def load(key: (Configuration, Boolean)): Connection = createConnection(key._1, key._2)
    }
  )

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = {
      CloseWithLogging(kerberosTicket)
      CloseWithLogging(connections.asMap().values().asScala)
    }
  })

  /**
    * Get (or create) a cached configuration
    *
    * @param params data store params
    * @return
    */
  def getConfiguration(params: java.util.Map[String, _]): Configuration = {
    val zk = ZookeeperParam.lookupOpt(params)
    val paths = ConfigPathsParam.lookupOpt(params)
    val props = ConfigsParam.lookupOpt(params)
    configs.get((zk, paths, props))
  }

  /**
    * Get (or create) a cached connection
    *
    * @param params data store params
    * @param validate validate the connection after creation, or not
    * @return
    */
  def getConnection(params: java.util.Map[String, _], validate: Boolean): Connection = {
    if (ConnectionParam.exists(params)) {
      ConnectionParam.lookup(params)
    } else {
      connections.get((getConfiguration(params), validate))
    }
  }

  /**
    * Create a new connection (not pooled)
    *
    * @param conf hbase configuration
    * @param validate validate the connection after creation, or not
    * @return
    */
  def createConnection(conf: Configuration, validate: Boolean): Connection = {
    val action = new PrivilegedExceptionAction[Connection]() {
      override def run(): Connection = {
        if (validate) {
          logger.debug("Checking configuration availability")
          HBaseVersions.checkAvailable(conf)
        }
        ConnectionFactory.createConnection(conf)
      }
    }
    val user = if (User.isHBaseSecurityEnabled(conf)) { Option(User.getCurrent) } else { None }
    user match {
      case None => action.run()
      case Some(u) => u.runAs(action)
    }
  }

  /**
    * Configures hadoop security, based on the configuration.
    *
    * Note: hadoop security is configured globally - having different security settings in a single JVM
    * will likely result in errors
    *
    * @param conf conf
    */
  def configureSecurity(conf: Configuration): Unit = synchronized {

    val last = userCheck // will be null first time through

    if (User.isHBaseSecurityEnabled(conf)) {
      val authMethod: AuthenticationMethod = org.apache.hadoop.security.SecurityUtil.getAuthenticationMethod(conf)

      logger.debug(s"Auth method: $authMethod")

      if (authMethod != AuthenticationMethod.KERBEROS || authMethod != AuthenticationMethod.KERBEROS_SSL) {
        logger.warn(s"HBase is configured to used Kerberos, however Hadoop authentication is: $authMethod")
      }

      UserGroupInformation.setConfiguration(conf)

      val principal = conf.get(HBaseGeoMesaPrincipal)
      val keytab = conf.get(HBaseGeoMesaKeyTab)

      logger.debug(s"Is Hadoop security enabled: ${UserGroupInformation.isSecurityEnabled}")
      logger.debug(s"Using Kerberos with principal $principal and file $keytab")

      UserGroupInformation.loginUserFromKeytab(principal, keytab)

      logger.debug(s"Logged into Hadoop with user '${User.getCurrent}'")

      if (kerberosTicket.isEmpty) {
        kerberosTicket = Some(HadoopUtils.kerberosTicketRenewer())
      }
    }

    userCheck = Option(User.getCurrent)

    if (last != null && last != userCheck) {
      logger.warn(s"Detected change in authenticated user from ${last.getOrElse("unauthenticated")} to " +
          s"${userCheck.getOrElse("unauthenticated")} - this may not work properly with Hadoop security")
    }
  }
}
