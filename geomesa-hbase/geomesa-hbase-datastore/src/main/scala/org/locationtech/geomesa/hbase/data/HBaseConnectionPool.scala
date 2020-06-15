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

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine, LoadingCache}
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants}
import org.apache.hadoop.security.authentication.util.KerberosUtil
import org.apache.hadoop.security.{SecurityUtil, UserGroupInformation}
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.{HBaseGeoMesaKeyTab, HBaseGeoMesaPrincipal}
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams.{ConfigPathsParam, ConfigsParam, ConnectionParam, ZookeeperParam}
import org.locationtech.geomesa.hbase.utils.HBaseVersions
import org.locationtech.geomesa.utils.io.{CloseWithLogging, HadoopUtils}

import scala.util.{Failure, Success, Try}

object HBaseConnectionPool extends LazyLogging {

  import scala.collection.JavaConverters._

  private val configs: LoadingCache[ConfigKey, Configuration] = Caffeine.newBuilder().build(
    new CacheLoader[ConfigKey, Configuration] {

      // add common resources from system property - lazy to allow object initialization if there's an error
      private lazy val configuration = {
        val base = HBaseConfiguration.create()
        HBaseDataStoreFactory.ConfigPathProperty.option.foreach(addResources(base, _))
        base
      }

      override def load(key: ConfigKey): Configuration = {
        val conf = new Configuration(configuration)
        // add the explicit props first, they may be needed for loading the path resources
        key.xml.foreach(xml => conf.addResource(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))))
        key.paths.foreach(addResources(conf, _))
        key.zookeepers.foreach(zk => conf.set(HConstants.ZOOKEEPER_QUORUM, zk))
        if (key.zookeepers.isEmpty && conf.get(HConstants.ZOOKEEPER_QUORUM) == "localhost") {
          logger.warn("HBase connection is set to localhost - " +
              "this may indicate that 'hbase-site.xml' is not on the classpath")
        }
        conf
      }

      private def addResources(conf: Configuration, paths: String): Unit =
        paths.split(',').map(_.trim).filterNot(_.isEmpty).foreach(HadoopUtils.addResource(conf, _))
    }
  )

  private val connections: LoadingCache[(Configuration, Boolean), CachedConnection] =  Caffeine.newBuilder().build(
    new CacheLoader[(Configuration, Boolean), CachedConnection] {
      override def load(key: (Configuration, Boolean)): CachedConnection = {
        createConnection(key._1, key._2) match {
          case SingletonConnection(connection, kerberos) => CachedConnection(connection, kerberos)
          case c => throw new NotImplementedError(s"Expected SingletonConnection but got $c")
        }
      }
    }
  )

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit =
      CloseWithLogging(connections.asMap().values().asScala.flatMap { case CachedConnection(c, k) => Seq(c) ++ k })
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
    val xml = ConfigsParam.lookupOpt(params)
    configs.get(ConfigKey(zk, paths, xml))
  }

  /**
   * Get (or create) a cached connection
   *
   * @param params data store params
   * @param validate validate the connection after creation, or not
   * @return
   */
  def getConnection(params: java.util.Map[String, _], validate: Boolean): ConnectionWrapper = {
    if (ConnectionParam.exists(params)) {
      ProvidedConnection(ConnectionParam.lookup(params))
    } else {
      val conf = getConfiguration(params)
      if (HBaseDataStoreParams.CacheConnectionsParam.lookup(params)) {
        connections.get((conf, validate))
      } else {
        createConnection(conf, validate)
      }
    }
  }

  /**
   * Create a new connection (not pooled)
   *
   * @param conf hbase configuration
   * @param validate validate the connection after creation, or not
   * @return
   */
  def createConnection(conf: Configuration, validate: Boolean): ConnectionWrapper = {
    if (User.isHBaseSecurityEnabled(conf)) {
      configureSecurity(conf)
      val action = new PrivilegedExceptionAction[ConnectionWrapper]() {
        override def run(): ConnectionWrapper = doCreateConnection(conf, validate)
      }
      User.getCurrent.runAs(action)
    } else {
      doCreateConnection(conf, validate)
    }
  }

  private def doCreateConnection(conf: Configuration, validate: Boolean): ConnectionWrapper = {
    if (validate) {
      logger.debug("Checking configuration availability")
      HBaseVersions.checkAvailable(conf)
    }
    val connection = ConnectionFactory.createConnection(conf)
    val kerberos = if (User.isHBaseSecurityEnabled(conf)) { Some(HadoopUtils.kerberosTicketRenewer()) } else { None }
    SingletonConnection(connection, kerberos)
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
    if (User.isHBaseSecurityEnabled(conf)) {
      val principal = conf.get(HBaseGeoMesaPrincipal)
      val keytab = conf.get(HBaseGeoMesaKeyTab)

      logger.debug(
        s"Using Kerberos with principal '$principal', keytab '$keytab', " +
            s"and Hadoop authentication method '${SecurityUtil.getAuthenticationMethod(conf)}'")

      val currentUser = UserGroupInformation.getCurrentUser

      if (currentUser.hasKerberosCredentials && sameName(currentUser, principal)) {
        logger.debug(s"User '$principal' is already authenticated")
      } else {
        if (currentUser.hasKerberosCredentials) {
          logger.warn(
            s"Changing global authenticated Hadoop user from '${currentUser.getUserName}' to '$principal' -" +
                "this will affect any connections still using the old user")
        }
        UserGroupInformation.setConfiguration(conf)
        UserGroupInformation.loginUserFromKeytab(principal, keytab)

        logger.debug(s"Logged into Hadoop with user '${UserGroupInformation.getCurrentUser.getUserName}'")
      }
    }
  }

  /**
   * Compare two kerberos principals.
   *
   * The existing principal is expected to have a realm and hostname filled out already.
   *
   * To compare, we: add the default realm, if there is no realm specified; replace _HOST with the current host.
   *
   * `SecurityUtil.getServerPrincipal` will replace the _HOST but only if there is already a realm
   *
   * @param current existing user
   * @param principal new user name
   * @return
   */
  private def sameName(current: UserGroupInformation, principal: String): Boolean = {
    val fullName = if (principal.indexOf('@') != -1) {
      // we have a realm so this should be work to replace _HOST if present
      SecurityUtil.getServerPrincipal(principal, null: String)
    } else {
      // try to add the default realm and replace _HOST if present
      Try(KerberosUtil.getDefaultRealm) match {
        case Success(realm) => SecurityUtil.getServerPrincipal(s"$principal@$realm", null: String)
        case Failure(e) =>
          logger.debug(s"Unable to get default Kerberos realm: $e")
          if (!principal.contains(SecurityUtil.HOSTNAME_PATTERN)) { principal } else {
            // append a fake realm so that the _HOST replacement works and then remove it afterwards
            SecurityUtil.getServerPrincipal(s"$principal@foo", null: String).dropRight(4)
          }
      }
    }

    current.getUserName == fullName
  }

  /**
   * Managed connection. The connection itself should not be closed - instead close the wrapper to handle
   * lifecycle events appropriately.
   */
  sealed trait ConnectionWrapper extends Closeable {
    val connection: Connection
  }

  /**
   * An unshared connection
   *
   * @param connection connection
   * @param kerberos kerberos ticket renewal thread
   */
  case class SingletonConnection(connection: Connection, kerberos: Option[Closeable]) extends ConnectionWrapper {
    override def close(): Unit = CloseWithLogging(kerberos.toSeq ++ Seq(connection))
  }

  /**
   * A shared, cached connection
   *
   * @param connection connection
   * @param kerberos kerberos ticket renewal thread
   */
  case class CachedConnection(connection: Connection, kerberos: Option[Closeable]) extends ConnectionWrapper {
    override def close(): Unit = {}
  }

  /**
   * Provided connection - no lifecycle management is performed
   *
   * @param connection connection
   */
  case class ProvidedConnection(connection: Connection) extends ConnectionWrapper {
    override def close(): Unit = {}
  }

  private case class ConfigKey(zookeepers: Option[String], paths: Option[String], xml: Option[String])
}
