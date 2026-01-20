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
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.NamespaceParams
import org.locationtech.geomesa.security.SecurityParams
import org.locationtech.geomesa.utils.classpath.ServiceLoader
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
      "Encoding of data",
      default = "", // needed to prevent geoserver from selecting something
      enumerations = ServiceLoader.load[FileSystemStorageFactory]().map(_.encoding),
      supportsNiFiExpressions = true,
      readWrite = ReadWriteFlag.WriteOnly
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

  @deprecated("ConfigsParam")
  val ConfParam =
    new GeoMesaParam[Properties](
      "fs.config",
      "Values to set in the root Configuration, in Java properties format",
      largeText = true
    )

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
