/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin}
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.{HBaseGeoMesaKeyTab, HBaseGeoMesaPrincipal}
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams.{ConfigPathsParam, ConnectionParam, ZookeeperParam}

object HBaseConnectionPool extends LazyLogging {

  private var userCheck: Option[User] = _

  private val zkConfigCache = Caffeine.newBuilder().build(
    new CacheLoader[(Option[String], Option[String]), Configuration] {
      override def load(key: (Option[String], Option[String])): Configuration = {
        val (zookeepers, paths) = key
        val configuration = withPaths(HBaseConfiguration.create(), HBaseDataStoreFactory.ConfigPathProperty.option)
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
  private val confConfigCache = Caffeine.newBuilder().build(
    new CacheLoader[(Configuration, Option[String]), Configuration] {
      override def load(key: (Configuration, Option[String])): Configuration = {
        val (configuration, paths) = key
        val config = withPaths(HBaseConfiguration.create(configuration), HBaseDataStoreFactory.ConfigPathProperty.option)
        withPaths(config, paths)
        configureSecurity(config)
        config
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

  def getConnection(params: java.util.Map[String, Serializable], validate: Boolean): Connection = {
    if (ConnectionParam.exists(params)) {
      ConnectionParam.lookup(params)
    } else if (params.containsKey("hadoop.configuration")) {
      val conf = params.get("hadoop.configuration").asInstanceOf[Configuration]
      val paths = ConfigPathsParam.lookupOpt(params)
      connectionCache.get((confConfigCache.get((conf, paths)), validate))
    } else {
      val zk = ZookeeperParam.lookupOpt(params)
      val paths = ConfigPathsParam.lookupOpt(params)
      connectionCache.get((zkConfigCache.get((zk, paths)), validate))
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
      sanitized.foreach(p => conf.addResource(new Path(p)))
      conf
    }
  }
}
