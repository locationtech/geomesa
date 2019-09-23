/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.io.Serializable
import java.security.PrivilegedExceptionAction

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin}
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.{HBaseGeoMesaKeyTab, HBaseGeoMesaPrincipal}
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams.{ConfigPathsParam, ConnectionParam, ZookeeperParam}
import org.locationtech.geomesa.utils.io.{PathUtils, WithClose}

object HBaseConnectionPool extends LazyLogging {

  private var userCheck: Option[User] = _

  // add common resources from system property
  private val configuration = withPaths(HBaseConfiguration.create(), HBaseDataStoreFactory.ConfigPathProperty.option)

  private val configCache = Caffeine.newBuilder().build(
    new CacheLoader[(Option[String], Option[String]), Configuration] {
      override def load(key: (Option[String], Option[String])): Configuration = {
        val (zookeepers, paths) = key
        val conf = withPaths(configuration, paths)
        zookeepers.foreach(zk => conf.set(HConstants.ZOOKEEPER_QUORUM, zk))
        if (zookeepers.isEmpty && conf.get(HConstants.ZOOKEEPER_QUORUM) == "localhost") {
          logger.warn("HBase connection is set to localhost - " +
              "this may indicate that 'hbase-site.xml' is not on the classpath")
        }
        configureSecurity(conf)
        conf
      }
    }
  )

  private val connectionCache = Caffeine.newBuilder().build(
    new CacheLoader[(Configuration, Boolean), Connection] {
      override def load(key: (Configuration, Boolean)): Connection = {
        val (conf, validate) = key
        val action = new PrivilegedExceptionAction[Connection]() {
          override def run(): Connection = {
            if (validate) {
              logger.debug("Checking configuration availability")
              HBaseAdmin.checkHBaseAvailable(conf)
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
    }
  )

  Runtime.getRuntime.addShutdownHook(new Thread() {
    import scala.collection.JavaConversions._
    override def run(): Unit = connectionCache.asMap().foreach(_._2.close())
  })

  def getConfiguration(params: java.util.Map[String, Serializable]): Configuration = {
    val zk = ZookeeperParam.lookupOpt(params)
    val paths = ConfigPathsParam.lookupOpt(params)
    configCache.get((zk, paths))
  }

  def getConnection(params: java.util.Map[String, Serializable], validate: Boolean): Connection = {
    if (ConnectionParam.exists(params)) {
      ConnectionParam.lookup(params)
    } else {
      connectionCache.get((getConfiguration(params), validate))
    }
  }

  // hadoop/hbase security is configured in a global manner...
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
    }

    userCheck = Option(User.getCurrent)

    if (last != null && last != userCheck) {
      logger.warn(s"Detected change in authenticated user from ${last.getOrElse("unauthenticated")} to " +
          s"${userCheck.getOrElse("unauthenticated")} - this may not work properly with Hadoop security")
    }
  }

  /**
    * Creates a new configuration with the paths added. If no paths, will share the existing configuration.
    *
    * @param base original configuration
    * @param paths resources to add, comma-delimited
    * @return a new configuration, or the existing one
    */
  private def withPaths(base: Configuration, paths: Option[String]): Configuration = {
    val sanitized = paths.filterNot(_.trim.isEmpty).toSeq.flatMap(_.split(',')).map(_.trim).filterNot(_.isEmpty)
    if (sanitized.isEmpty) { base } else {
      val conf = new Configuration(base)
      sanitized.foreach { path =>
        // use our path handling logic, which is more robust than just passing paths to the config
        val handles = PathUtils.interpretPath(path)
        if (handles.isEmpty) {
          logger.warn(s"Could not load configuration file at: $path")
        } else {
          handles.foreach { handle =>
            WithClose(handle.open) { files =>
              files.foreach {
                case (None, is) => conf.addResource(is)
                case (Some(name), is) => conf.addResource(is, name)
              }
              conf.size() // this forces a loading of the resource files, before we close our file handle
            }
          }
        }
      }
      conf
    }
  }
}
