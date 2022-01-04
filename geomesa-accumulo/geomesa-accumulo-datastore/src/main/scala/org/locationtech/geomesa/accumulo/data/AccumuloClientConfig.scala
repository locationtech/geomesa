/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * Portions Copyright (c) 2021 The MITRE Corporation
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 * This software was produced for the U. S. Government under Basic
 * Contract No. W56KGU-18-D-0004, and is subject to the Rights in
 * Noncommercial Computer Software and Noncommercial Computer Software
 * Documentation Clause 252.227-7014 (FEB 2012)
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data

import java.io.{File, FileInputStream}
import java.util.Properties

import com.typesafe.scalalogging.Logger
import org.apache.accumulo.core.client.ClientConfiguration
import org.locationtech.geomesa.utils.io.WithClose
import org.slf4j.LoggerFactory

import scala.util.Try

/**
 * Client configuration options, loaded from a config file
 *
 * @param instance instance name (not actually used during configuration)
 * @param zookeepers zookeeper connect string (not actually used during configuration)
 * @param zkTimeout zookeeper timeout (not actually used during configuration)
 * @param authType  authentication type ("password", "kerberos" or authentication token class)
 * @param principal authentication principal (username, keytab file, etc)
 * @param token authentication token (password, etc)
 */
case class AccumuloClientConfig(
    instance: Option[String],
    zookeepers: Option[String],
    zkTimeout: Option[String],
    authType: Option[String],
    principal: Option[String],
    token: Option[String]
  ) {

  @SuppressWarnings(Array("deprecated"))
  private[this] var config: ClientConfiguration = ClientConfiguration.create();
  private[this] var hasInstance: Boolean = false;

  @SuppressWarnings(Array("deprecated"))
  private[data] def getConfig(): ClientConfiguration = {
    config;
  }

  private[data] def getHasInstance(): Boolean = {
    hasInstance;
  }

  /**
   * Client configuration options, loaded from a config file
   *
   * @param config Accumulo client configuration
   * @param hasInstance instance id or name set in config file
   * @param instance instance name (not actually used during configuration)
   * @param zookeepers zookeeper connect string (not actually used during configuration)
   * @param zkTimeout zookeeper timeout (not actually used during configuration)
   * @param authType  authentication type ("password", "kerberos" or authentication token class)
   * @param principal authentication principal (username, keytab file, etc)
   * @param token authentication token (password, etc)
   */
  private def this(config: ClientConfiguration,
    hasInstance: Boolean,
    instance: Option[String],
    zookeepers: Option[String],
    zkTimeout: Option[String],
    authType: Option[String],
    principal: Option[String],
    token: Option[String]
  ) {
    this(instance, zookeepers, zkTimeout, authType, principal, token);
    this.config = config;
    this.hasInstance = hasInstance;
  }

  override def toString: String = {
    val values =
      Seq(s"hasInstance=$hasInstance") ++
      instance.map(s => s"instance=$s").toSeq ++ zookeepers.map(s => s"zookeepers=$s") ++
        zkTimeout.map(s => s"zookeepers.timeout=$s") ++ authType.map(s => s"auth.type=$s") ++
        principal.map(s => s"principal=$s") ++ token.map(_ => "token=***")
    values.mkString("{", ",", "}")
  }
}

object AccumuloClientConfig {

  val PasswordAuthType = "password"
  val KerberosAuthType = "kerberos"

  // get the logger directly so that the logger name (and config) doesn't have a $ on the end
  private lazy val logger: Logger = Logger(LoggerFactory.getLogger(classOf[AccumuloClientConfig]))

  /**
   * Search the classpath for Accumulo configuration files
   *
   * @return
   */
  def load(): AccumuloClientConfig = {
    val loader = Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
    AccumuloClientProperties(loader).orElse(ClientConf(loader))
      .getOrElse(AccumuloClientConfig(None, None, None, None, None, None))
  }

  /**
   * This class mimics the Accumulo 2.x logic for reading 'accumulo-client.properties'
   */
  private object AccumuloClientProperties extends ConfigLoader {
    override protected val fileName: String = "accumulo-client.properties"
    override protected val zookeeperKey: String = "instance.zookeepers"
    override protected val zookeeperTimeoutKey: String = "instance.zookeepers.timeout"
    override protected val authTypeKey: Option[String] = Some("auth.type")
    override protected val principalKey: Option[String] = Some("auth.principal")
    override protected val tokenKey: Option[String] = Some("auth.token")
  }

  /**
   * This class mimics the Accumulo 1.x logic for reading 'client.conf'
   */
  private object ClientConf extends ConfigLoader {
    override protected val fileName: String = "client.conf"
    override protected val zookeeperKey: String = "instance.zookeeper.host"
    override protected val zookeeperTimeoutKey: String = "instance.zookeeper.timeout"
    override protected val authTypeKey: Option[String] = None
    override protected val principalKey: Option[String] = None
    override protected val tokenKey: Option[String] = None
  }

  private trait ConfigLoader {

    protected def fileName: String
    protected def zookeeperKey: String
    protected def zookeeperTimeoutKey: String
    protected def authTypeKey: Option[String]
    protected def principalKey: Option[String]
    protected def tokenKey: Option[String]

    def apply(cl: ClassLoader): Option[AccumuloClientConfig] = {
      loadFromClasspath(cl, fileName).map { f =>
        val props = new Properties()
        WithClose(new FileInputStream(f))(props.load)

        val instance = Option(props.getProperty("instance.name"))
        val hasInstance = (instance != None) || props.getProperty("instance.id") != null
        val zk = Option(props.getProperty(zookeeperKey))
        val zkTimeout = Option(props.getProperty(zookeeperTimeoutKey))//=30s
        val auth = authTypeKey.map(props.getProperty).filter(_ != null)
        val principal = principalKey.map(props.getProperty).filter(_ != null)
        val token = tokenKey.map(props.getProperty).filter(_ != null)
        @SuppressWarnings(Array("deprecated"))
        val conf = ClientConfiguration.fromFile(f)
        val config = new AccumuloClientConfig(conf, hasInstance, instance, zk, zkTimeout, auth, principal, token)
        logger.info(s"Loaded Accumulo config from '${f.getAbsolutePath}': $config")
        config
      }
    }

    /**
     * Attempt to load a file from the classpath
     *
     * @param cl classloader
     * @param fileName file name
     * @return
     */
    private def loadFromClasspath(cl: ClassLoader, fileName: String): Option[File] =
      Option(cl.getResource(fileName)).flatMap(url => Try(new File(url.toURI)).filter(_.exists).toOption)
  }
}
