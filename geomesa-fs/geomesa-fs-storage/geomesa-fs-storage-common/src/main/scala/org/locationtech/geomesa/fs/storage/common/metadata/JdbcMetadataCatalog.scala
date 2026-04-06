/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.common.metadata

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.dbcp2._
import org.apache.commons.pool2.impl.{GenericObjectPool, GenericObjectPoolConfig}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.metadata.JdbcMetadata.{FilesTable, JdbcMetadataConfig, MetadataTable}
import org.locationtech.geomesa.utils.io.{CloseQuietly, WithClose}

import java.util.Properties
import scala.util.control.NonFatal

class JdbcMetadataCatalog(context: FileSystemContext, config: Map[String, String]) extends StorageMetadataCatalog {

  // fail fast if url is not defined or invalid
  private val conf = JdbcMetadataConfig(config)
  private val driver = JdbcMetadataCatalog.createConnectionFactory(conf)
  private val metaTable = new MetadataTable(conf.schema, conf.tablePrefix)
  private val root = context.root.toString

  override def getTypeNames: Seq[String] = {
    WithClose(driver.createConnection()) { cx =>
      WithClose(cx.getMetaData.getTables(null, metaTable.schema, metaTable.tableName, null)) { rs =>
        if (!rs.next()) {
          return Seq.empty
        }
      }
      metaTable.selectTypeNames(cx, root)
    }
  }

  override def load(typeName: String): StorageMetadata = {
    val pool = JdbcMetadataCatalog.createDataSource(driver, conf)
    try {
      val meta = WithClose(pool.getConnection())(metaTable.selectMetadata(_, root, typeName))
      newJdbcMeta(pool, meta)
    } catch {
      case NonFatal(e) => CloseQuietly(pool).foreach(e.addSuppressed); throw e
    }
  }

  override def create(sft: SimpleFeatureType, partitions: Seq[String], targetFileSize: Option[Long]): StorageMetadata = {
    // load the partition scheme first in case it fails
    partitions.foreach(PartitionSchemeFactory.load(sft, _))
    val meta = Metadata(sft, partitions, targetFileSize)
    val pool = JdbcMetadataCatalog.createDataSource(driver, conf)
    try {
      WithClose(pool.getConnection()) { cx =>
        metaTable.create(cx)
        metaTable.insert(cx, root, meta)
        new FilesTable(conf.schema, conf.tablePrefix).create(cx)
      }
      newJdbcMeta(pool, meta)
    } catch {
      case NonFatal(e) => CloseQuietly(pool).foreach(e.addSuppressed); throw e
    }
  }

  private def newJdbcMeta(pool: PoolingDataSource[PoolableConnection], meta: Metadata): JdbcMetadata =
    new JdbcMetadata(pool, root, conf.schema, conf.tablePrefix, meta.copy(sft = namespaced(meta.sft, context.namespace)))
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
