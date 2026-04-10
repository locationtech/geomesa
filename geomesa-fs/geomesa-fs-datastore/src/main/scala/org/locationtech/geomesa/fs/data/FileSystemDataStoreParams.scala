/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import org.apache.hadoop.conf.Configuration
import org.locationtech.geomesa.fs.storage.api.FileSystemStorageFactory
import org.locationtech.geomesa.fs.storage.common.metadata.{ConverterMetadata, FileBasedMetadata, JdbcMetadata}
import org.locationtech.geomesa.fs.storage.parquet.ParquetFileSystemStorage
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.NamespaceParams
import org.locationtech.geomesa.security.SecurityParams
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.geotools.GeoMesaParam.{ConvertedParam, ReadWriteFlag, SystemPropertyDurationParam}

import java.io.{StringReader, StringWriter}
import java.util.Properties
import scala.concurrent.duration.Duration

object FileSystemDataStoreParams extends FileSystemDataStoreParams

trait FileSystemDataStoreParams extends SecurityParams with NamespaceParams {

  import scala.collection.JavaConverters._

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
      "fs.metadata.type",
      "Type of metadata to use",
      optional = false,
      default = "", // needed to prevent geoserver from selecting something
      enumerations = Seq(FileBasedMetadata.MetadataType, JdbcMetadata.MetadataType, ConverterMetadata.MetadataType),
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadWrite,
    )

  val MetadataConfigParam =
    new GeoMesaParam[Properties](
      "fs.metadata.config",
      "Configuration options for the metadata implementation, in Java properties format",
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.ReadWrite,
    )

  val ConfigPathsParam =
    new GeoMesaParam[String](
      "fs.config.paths",
      "Additional Hadoop configuration resource files (comma-delimited)",
      supportsNiFiExpressions = true
    )

  val ConfigsParam =
    new GeoMesaParam[String](
      "fs.config.xml",
      "Additional Hadoop configuration properties, as a standard XML `<configuration>` element",
      largeText = true,
      deprecatedParams = Seq(new ConvertedParam[String, String]("fs.config", convertPropsToXml)),
      supportsNiFiExpressions = true
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

  /**
   * Convert java properties format to *-site.xml
   *
   * @param properties props
   * @return
   */
  private def convertPropsToXml(properties: String): String = {
    val conf = new Configuration(false)

    val props = new Properties()
    props.load(new StringReader(properties))
    props.asScala.foreach { case (k, v) => conf.set(k, v) }

    val out = new StringWriter()
    conf.writeXml(out)
    out.toString
  }
}
