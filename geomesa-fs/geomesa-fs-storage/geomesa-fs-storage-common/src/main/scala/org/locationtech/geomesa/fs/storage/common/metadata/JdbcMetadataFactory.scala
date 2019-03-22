/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.dbcp2.{PoolingDataSource, _}
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.utils.io.{CloseQuietly, WithClose}
import org.locationtech.geomesa.utils.stats.MethodProfiling

import scala.util.control.NonFatal

class JdbcMetadataFactory extends StorageMetadataFactory {

  override def name: String = JdbcMetadata.MetadataType

  /**
    * Creates a metadata instance from an existing root. The metadata connection info is persisted in a
    * `metadata.json` file under the root path.
    *
    * If a previous check was made to load a file from this root, and the file did not exist, will not
    * re-attempt to load it until after a configurable timeout.
    *
    * @see `org.locationtech.geomesa.fs.storage.common.utils.PathCache#CacheDurationProperty()`
    * @param context file context
    * @return
    **/
  override def load(context: FileSystemContext): Option[JdbcMetadata] = {
    MetadataJson.readMetadata(context).collect {
      case NamedOptions(name, opts) if name.equalsIgnoreCase(this.name) =>
        val root = context.root.toUri.toString
        val source = JdbcMetadataFactory.createDataSource(opts)
        try {
          val metadata = JdbcMetadata.load(source, root).getOrElse {
            throw new IllegalArgumentException(s"Could not load metadata at root '$root'")
          }
          val sft = namespaced(metadata.sft, context.namespace)
          val scheme = PartitionSchemeFactory.load(sft, metadata.scheme)
          new JdbcMetadata(source, root, sft, metadata.encoding, scheme, metadata.leafStorage)
        } catch {
          case NonFatal(e) => CloseQuietly(source).foreach(e.addSuppressed); throw e
        }
    }
  }

  override def create(context: FileSystemContext, config: Map[String, String], meta: Metadata): JdbcMetadata = {
    // load the partition scheme first in case it fails
    val scheme = PartitionSchemeFactory.load(meta.sft, meta.scheme)
    MetadataJson.writeMetadata(context, NamedOptions(name, config))
    val root = context.root.toUri.toString
    val sft = namespaced(meta.sft, context.namespace)
    val source = JdbcMetadataFactory.createDataSource(config)
    try {
      JdbcMetadata.create(source, root, meta)
      new JdbcMetadata(source, root, sft, meta.encoding, scheme, meta.leafStorage)
    } catch {
      case NonFatal(e) => CloseQuietly(source).foreach(e.addSuppressed); throw e
    }
  }
}

object JdbcMetadataFactory extends MethodProfiling with LazyLogging {

  private def createDataSource(config: Map[String, String]): PoolingDataSource[PoolableConnection] = {
    import JdbcMetadata.Config._

    val url = config.getOrElse(UrlKey, throw new IllegalArgumentException(s"JdbcMetadata requires '$UrlKey'"))
    config.get(DriverKey).foreach(Class.forName) // required for older drivers
    val props = new Properties()
    config.get(UserKey).foreach(props.put("user", _))
    config.get(PasswordKey).foreach(props.put("password", _))

    val driver = new DriverManagerConnectionFactory(url, props)

    // validate the connection parameters
    WithClose(driver.createConnection()) { connection =>
      if (!connection.isValid(10)) {
        throw new IllegalArgumentException(
          s"Could not create valid connection using configuration ${config.mkString(", ")}")
      }
    }

    def setPoolConfig[T](key: String, conversion: String => T, method: T => Unit): Unit = {
      config.get(key).foreach { v =>
        try { method.apply(conversion(v)) } catch {
          case NonFatal(e) => logger.warn(s"Invalid configuration value '$v' for key $key: $e")
        }
      }
    }

    val poolConfig = new GenericObjectPoolConfig[PoolableConnection]()
    setPoolConfig[Int](MaxIdleKey, _.toInt, poolConfig.setMaxIdle)
    setPoolConfig[Int](MinIdleKey, _.toInt, poolConfig.setMinIdle)
    setPoolConfig[Int](MaxSizeKey, _.toInt, poolConfig.setMaxTotal)
    setPoolConfig[Boolean](FairnessKey, _.toBoolean, poolConfig.setFairness)
    setPoolConfig[Boolean](TestOnBorrowKey, _.toBoolean, poolConfig.setTestOnBorrow)
    setPoolConfig[Boolean](TestOnCreateKey, _.toBoolean, poolConfig.setTestOnCreate)
    setPoolConfig[Boolean](TestWhileIdlKey, _.toBoolean, poolConfig.setTestWhileIdle)

    val factory = new PoolableConnectionFactory(driver, null)
    val pool = new GenericObjectPool(factory, poolConfig)
    factory.setPool(pool)

    new PoolingDataSource[PoolableConnection](pool)
  }
}
