/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import java.awt.RenderingHints
import java.io.{ByteArrayInputStream, StringReader, StringWriter}
import java.nio.charset.StandardCharsets
import java.util.{Collections, Properties}

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.fs.storage.api.FileSystemStorageFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{GeoMesaDataStoreInfo, NamespaceParams}
import org.locationtech.geomesa.utils.classpath.ServiceLoader
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.geotools.GeoMesaParam.{ConvertedParam, ReadWriteFlag, SystemPropertyDurationParam}
import org.locationtech.geomesa.utils.io.HadoopUtils

import scala.concurrent.duration.Duration

class FileSystemDataStoreFactory extends DataStoreFactorySpi {

  import FileSystemDataStoreFactory.FileSystemDataStoreParams._
  import FileSystemDataStoreFactory.fileContextCache

  override def createDataStore(params: java.util.Map[String, java.io.Serializable]): DataStore = {

    val xml = ConfigsParam.lookupOpt(params)
    val resources = ConfigPathsParam.lookupOpt(params).toSeq.flatMap(_.split(',')).map(_.trim).filterNot(_.isEmpty)

    val conf = if (xml.isEmpty && resources.isEmpty) { FileSystemDataStoreFactory.configuration } else {
      val conf = new Configuration(FileSystemDataStoreFactory.configuration)
      // add the explicit props first, they may be needed for loading the path resources
      xml.foreach(x => conf.addResource(new ByteArrayInputStream(x.getBytes(StandardCharsets.UTF_8))))
      resources.foreach(HadoopUtils.addResource(conf, _))
      conf
    }

    val fc = fileContextCache.get(conf)

    val path = new Path(PathParam.lookup(params))
    val encoding = EncodingParam.lookupOpt(params).filterNot(_.isEmpty)

    // Need to do more tuning here. On a local system 1 thread (so basic producer/consumer) was best
    // because Parquet is also threading the reads underneath I think. using prod/cons pattern was
    // about 30% faster but increasing beyond 1 thread slowed things down. This could be due to the
    // cost of serializing simple features though. need to investigate more.
    //
    // However, if you are doing lots of filtering it appears that bumping the threads up high
    // can be very useful. Seems possibly numcores/2 might is a good setting (which is a standard idea)

    val readThreads = ReadThreadsParam.lookup(params)
    val writeTimeout = WriteTimeoutParam.lookup(params)

    val namespace = NamespaceParam.lookupOpt(params)

    new FileSystemDataStore(fc, conf, path, readThreads, writeTimeout, encoding, namespace)
  }

  override def createNewDataStore(params: java.util.Map[String, java.io.Serializable]): DataStore =
    createDataStore(params)

  override def isAvailable: Boolean = true

  override def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean =
    FileSystemDataStoreFactory.canProcess(params)

  override def getDisplayName: String = FileSystemDataStoreFactory.DisplayName

  override def getDescription: String = FileSystemDataStoreFactory.Description

  override def getParametersInfo: Array[Param] = FileSystemDataStoreFactory.ParameterInfo :+ NamespaceParam

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = Collections.emptyMap()
}

object FileSystemDataStoreFactory extends GeoMesaDataStoreInfo {

  import scala.collection.JavaConverters._

  override val DisplayName: String = "File System (GeoMesa)"
  override val Description: String = "File System Data Store"

  override val ParameterInfo: Array[GeoMesaParam[_ <: AnyRef]] =
    Array(
      FileSystemDataStoreParams.PathParam,
      FileSystemDataStoreParams.EncodingParam,
      FileSystemDataStoreParams.ReadThreadsParam,
      FileSystemDataStoreParams.WriteTimeoutParam,
      FileSystemDataStoreParams.ConfigPathsParam,
      FileSystemDataStoreParams.ConfigsParam
    )

  override def canProcess(params: java.util.Map[String, _ <: java.io.Serializable]): Boolean =
    FileSystemDataStoreParams.PathParam.exists(params)

  private val configuration = new Configuration()

  private val fileContextCache = Caffeine.newBuilder().build(
    new CacheLoader[Configuration, FileContext]() {
      override def load(conf: Configuration): FileContext = FileContext.getFileContext(conf)
    }
  )

  object FileSystemDataStoreParams extends NamespaceParams {

    val WriterFileTimeout = SystemProperty("geomesa.fs.writer.partition.timeout", "60s")

    val DeprecatedConfParam = new ConvertedParam[String, String]("fs.config", convertPropsToXml)

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
        deprecatedParams = Seq(DeprecatedConfParam),
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
    private [fs] def convertPropsToXml(properties: String): String = {
      val conf = new Configuration(false)

      val props = new Properties()
      props.load(new StringReader(properties))
      props.asScala.foreach { case (k, v) => conf.set(k, v) }

      val out = new StringWriter()
      conf.writeXml(out)
      out.toString
    }
  }
}
