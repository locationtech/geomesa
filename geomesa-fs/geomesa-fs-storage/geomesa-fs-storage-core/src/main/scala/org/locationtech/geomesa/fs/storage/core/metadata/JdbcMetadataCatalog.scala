/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core
package metadata

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.dbcp2._
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.metadata.JdbcMetadata.{FilesTable, JdbcMetadataConfig, MetadataTable}
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, PartitionSchemeFactory}
import org.locationtech.geomesa.utils.io.{CloseQuietly, WithClose}

import java.util.Properties
import scala.util.control.NonFatal

/**
 * Catalog for jdbc metadata
 *
 * @param context file system context
 */
class JdbcMetadataCatalog(context: FileSystemContext) extends StorageMetadataCatalog {

  // fail fast if url is not defined or invalid
  private val conf = JdbcMetadataConfig(context.conf)
  private val driver = JdbcMetadataCatalog.createConnectionFactory(conf)
  private val root = context.root.toString

  override def getTypeNames: Seq[String] = {
    val meta = new MetadataTable(conf.schema, conf.tablePrefix, root, null)
    WithClose(driver.createConnection()) { cx =>
      WithClose(cx.getMetaData.getTables(null, meta.schema, meta.tableName, null)) { rs =>
        if (!rs.next()) {
          return Seq.empty
        }
      }
      meta.selectTypeNames(cx, root)
    }
  }

  override def load(typeName: String): StorageMetadata = {
    val pool = JdbcMetadataCatalog.createDataSource(driver, conf)
    try {
      val meta = new MetadataTable(conf.schema, conf.tablePrefix, root, typeName)
      val files = new FilesTable(conf.schema, conf.tablePrefix, root, typeName)
      new JdbcMetadata(pool, meta, files, context.namespace)
    } catch {
      case NonFatal(e) => CloseQuietly(pool).foreach(e.addSuppressed); throw e
    }
  }

  override def create(sft: SimpleFeatureType, partitions: Seq[String], targetFileSize: Option[Long]): StorageMetadata = {
    // load the partition scheme first in case it fails
    partitions.foreach(PartitionSchemeFactory.load(sft, _))
    val pool = JdbcMetadataCatalog.createDataSource(driver, conf)
    try {
      val meta = new MetadataTable(conf.schema, conf.tablePrefix, root, sft.getTypeName)
      val files = new FilesTable(conf.schema, conf.tablePrefix, root, sft.getTypeName)
      WithClose(pool.getConnection()) { cx =>
        meta.create(cx)
        meta.insert(cx, sft)
        meta.insert(cx, partitions)
        targetFileSize.foreach(size => meta.insert(cx, Metadata.TargetFileSize, size.toString))
        files.create(cx)
      }
      new JdbcMetadata(pool, meta, files, context.namespace)
    } catch {
      case NonFatal(e) => CloseQuietly(pool).foreach(e.addSuppressed); throw e
    }
  }
}

private object JdbcMetadataCatalog extends LazyLogging {

  /**
   * Create (and validate) a connection factory
   *
   * @param config config
   * @return
   */
  private def createConnectionFactory(config: JdbcMetadataConfig): DriverManagerConnectionFactory = {
    config.driver.foreach(Class.forName) // required for older drivers
    val props = new Properties()
    config.user.foreach(props.put("user", _))
    config.password.foreach(props.put("password", _))

    val driver = new DriverManagerConnectionFactory(config.url, props)

    // validate the connection parameters
    WithClose(driver.createConnection()) { connection =>
      if (!connection.isValid(10)) {
        throw new IllegalArgumentException(
          s"Could not create valid connection using configuration ${config.url}")
      }
    }

    driver
  }

  /**
    * Create a jdbc data source based on a configuration
    *
    * @param config config
    * @return
    */
  private def createDataSource(driver: DriverManagerConnectionFactory, config: JdbcMetadataConfig): PoolingDataSource[PoolableConnection] = {
    val poolConfig = new GenericObjectPoolConfig[PoolableConnection]()

    config.maxIdle.foreach(poolConfig.setMaxIdle)
    config.minIdle.foreach(poolConfig.setMinIdle)
    config.maxSize.foreach(poolConfig.setMaxTotal)
    config.fairness.foreach(poolConfig.setFairness)
    config.testOnBorrow.foreach(poolConfig.setTestOnBorrow)
    config.testOnCreate.foreach(poolConfig.setTestOnCreate)
    config.testWhileIdle.foreach(poolConfig.setTestWhileIdle)

    val factory = new PoolableConnectionFactory(driver, null)
    val pool = new GenericObjectPool(factory, poolConfig)
    factory.setPool(pool)

    new PoolingDataSource[PoolableConnection](pool)
  }
}
