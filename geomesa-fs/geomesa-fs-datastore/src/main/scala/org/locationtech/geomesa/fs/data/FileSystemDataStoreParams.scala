/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import org.apache.iceberg.CatalogUtil
import org.locationtech.geomesa.fs.storage.converter.ConverterCatalog
import org.locationtech.geomesa.fs.storage.core.FileSystemStorage
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.NamespaceParams
import org.locationtech.geomesa.security.SecurityParams
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.geotools.GeoMesaParam.{ReadWriteFlag, SystemPropertyIntegerParam}

import java.util.Properties
import scala.concurrent.duration.Duration

object FileSystemDataStoreParams extends FileSystemDataStoreParams

trait FileSystemDataStoreParams extends SecurityParams with NamespaceParams {

  val WriterFileTimeout: SystemProperty = SystemProperty("geomesa.fs.writer.partition.timeout")
  val WritersMaxOpenPartitions: SystemProperty = SystemProperty("geomesa.fs.writer.partitions.max.open")

  val PathParam =
    new GeoMesaParam[String](
      "fs.path",
      "Root of the filesystem hierarchy",
      optional = false,
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadWrite,
    )

  val CatalogTypeParam =
    new GeoMesaParam[String](
      "fs.catalog.type",
      s"Type of data catalog to use, either an Iceberg catalog class or '${ConverterCatalog.CatalogType}'. Alternatively, " +
        "an Iceberg catalog can be specified through config with 'catalog-impl'",
      default = "",
      enumerations = Seq(
        CatalogUtil.ICEBERG_CATALOG_TYPE_HADOOP,
        CatalogUtil.ICEBERG_CATALOG_TYPE_HIVE,
        CatalogUtil.ICEBERG_CATALOG_TYPE_REST,
        CatalogUtil.ICEBERG_CATALOG_TYPE_GLUE,
        CatalogUtil.ICEBERG_CATALOG_TYPE_NESSIE,
        CatalogUtil.ICEBERG_CATALOG_TYPE_JDBC,
        CatalogUtil.ICEBERG_CATALOG_TYPE_BIGQUERY,
        ConverterCatalog.CatalogType,
      ),
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadWrite,
    )

  val ConfigParam =
    new GeoMesaParam[Properties](
      "fs.config.properties",
      "Configuration options, in Java properties format",
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadWrite,
    )

  val ConfigFileParam =
    new GeoMesaParam[String](
      "fs.config.file",
      "A file containing configuration options, in Java properties format",
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadWrite,
    )

  val QueryThreadsParam =
    new GeoMesaParam[Integer](
      GeoMesaDataStoreFactory.QueryThreadsParam.key,
      GeoMesaDataStoreFactory.QueryThreadsParam.description.toString,
      default = GeoMesaDataStoreFactory.QueryThreadsParam.default,
      deprecatedKeys = Seq("fs.read-threads"),
      systemProperty = GeoMesaDataStoreFactory.QueryThreadsParam.systemProperty,
      supportsNiFiExpressions = GeoMesaDataStoreFactory.QueryThreadsParam.supportsNiFiExpressions,
      readWrite = GeoMesaDataStoreFactory.QueryThreadsParam.readWrite
    )

  val WritersMaxOpenPartitionsParam =
    new GeoMesaParam[Integer](
      FileSystemStorage.WriterMaxOpenPartitions,
      "How many partition files to hold open concurrently during a write operation",
      default = FileSystemStorage.WriterMaxOpenPartitionsDefault,
      systemProperty = Some(SystemPropertyIntegerParam(WritersMaxOpenPartitions)),
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.WriteOnly
    )

  val QueryTimeoutParam: GeoMesaParam[Duration] = GeoMesaDataStoreFactory.QueryTimeoutParam

  @deprecated("Use fs.config.properties and/or fs.config.file")
  private[data] val ConfigPathsParam =
    new GeoMesaParam[String](
      "fs.config.paths",
      "Additional Hadoop configuration resource files (comma-delimited)",
      supportsNiFiExpressions = true
    )

  @deprecated("Use fs.config.properties and/or fs.config.file")
  private[data] val ConfigXmlParam =
    new GeoMesaParam[String](
      "fs.config.xml",
      "Additional Hadoop configuration properties, as a standard XML `<configuration>` element",
      largeText = true,
      supportsNiFiExpressions = true
    )
}
