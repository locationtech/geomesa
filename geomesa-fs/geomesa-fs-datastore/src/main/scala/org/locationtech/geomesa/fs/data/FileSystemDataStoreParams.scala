/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import org.locationtech.geomesa.fs.storage.core.metadata.{ConverterMetadata, FileBasedMetadata, JdbcMetadata}
import org.locationtech.geomesa.fs.storage.core.{FileSystemStorageFactory, StorageMetadataCatalog}
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorage
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.NamespaceParams
import org.locationtech.geomesa.security.SecurityParams
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.geotools.GeoMesaParam.{ReadWriteFlag, SystemPropertyDurationParam}

import java.util.Properties
import scala.concurrent.duration.Duration

object FileSystemDataStoreParams extends FileSystemDataStoreParams

trait FileSystemDataStoreParams extends SecurityParams with NamespaceParams {

  val WriterFileTimeout: SystemProperty = SystemProperty("geomesa.fs.writer.partition.timeout", "60s")

  val PathParam =
    new GeoMesaParam[String](
      "fs.path",
      "Root of the filesystem hierarchy",
      optional = false,
      supportsNiFiExpressions = true
    )

  val EncodingParam =
    new GeoMesaParam[String](
      "fs.encoding",
      "Encoding to use for data files",
      default = ParquetFileSystemStorage.Encoding,
      enumerations = FileSystemStorageFactory.factories.map(_.encoding),
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadWrite
    )

  val MetadataTypeParam =
    new GeoMesaParam[String](
      StorageMetadataCatalog.MetadataTypeConfig,
      "Type of metadata to use",
      default = "", // needed to prevent geoserver from selecting something
      enumerations = Seq(FileBasedMetadata.MetadataType, JdbcMetadata.MetadataType, ConverterMetadata.MetadataType),
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

  val ReadThreadsParam =
    new GeoMesaParam[Integer](
      "fs.read-threads",
      "Read Threads",
      default = 4,
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadOnly
    )

  val WriteTimeoutParam =
    new GeoMesaParam[Duration](
      "fs.writer.partition.timeout",
      "Timeout for closing a partition file after write, e.g. '60 seconds'",
      default = Duration("60s"),
      systemProperty = Some(SystemPropertyDurationParam(WriterFileTimeout)),
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
