/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.CatalogUtil
import org.geotools.api.data.DataAccessFactory.Param
import org.geotools.api.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.fs.data.FileSystemDataStore.FileSystemDataStoreConfig
import org.locationtech.geomesa.fs.storage.converter.ConverterCatalog
import org.locationtech.geomesa.fs.storage.core.FileSystemContext
import org.locationtech.geomesa.fs.storage.core.iceberg.IcebergCatalog
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreInfo
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.hadoop.HadoopUtils
import org.locationtech.geomesa.utils.io.WithClose

import java.awt.RenderingHints
import java.io.ByteArrayInputStream
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util.Collections
import scala.util.control.NonFatal

class FileSystemDataStoreFactory extends DataStoreFactorySpi with LazyLogging {

  import org.locationtech.geomesa.fs.data.FileSystemDataStoreParams._

  import scala.collection.JavaConverters._

  override def createDataStore(params: java.util.Map[String, _]): DataStore = {
    // Need to do more tuning here. On a local system 1 thread (so basic producer/consumer) was best
    // because Parquet is also threading the reads underneath I think. using prod/cons pattern was
    // about 30% faster but increasing beyond 1 thread slowed things down. This could be due to the
    // cost of serializing simple features though. need to investigate more.
    //
    // However, if you are doing lots of filtering it appears that bumping the threads up high
    // can be very useful. Seems possibly numcores/2 might is a good setting (which is a standard idea)

    val readThreads = QueryThreadsParam.lookup(params)
    val queryTimeout = QueryTimeoutParam.lookupOpt(params).filter(_.isFinite)

    val namespace = NamespaceParam.lookupOpt(params)

    val path = new URI(PathParam.lookup(params))
    val context = FileSystemContext.create(path, buildConf(params), namespace)
    val config = FileSystemDataStoreConfig(context, readThreads, queryTimeout)

    lazy val encodingType =
      if (params.get("fs.encoding") == "converter") {
        logger.warn(s"Using deprecated parameter 'fs.encoding' - please switch to '${CatalogTypeParam.key}' instead")
        true
      } else {
        false
      }

    val catalogType = context.conf.getOrElse(CatalogUtil.ICEBERG_CATALOG_TYPE, null)
    val catalog =
      if (catalogType == ConverterCatalog.CatalogType || (catalogType == null && encodingType)) {
        new ConverterCatalog(context)
      } else {
        new IcebergCatalog(context)
      }

    new FileSystemDataStore(catalog, config)
  }

  override def createNewDataStore(params: java.util.Map[String, _]): DataStore = createDataStore(params)

  override def isAvailable: Boolean = true

  override def canProcess(params: java.util.Map[String, _]): Boolean = FileSystemDataStoreFactory.canProcess(params)

  override def getDisplayName: String = FileSystemDataStoreFactory.DisplayName

  override def getDescription: String = FileSystemDataStoreFactory.Description

  override def getParametersInfo: Array[Param] = Array(FileSystemDataStoreFactory.ParameterInfo :+ NamespaceParam: _*)

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = Collections.emptyMap()

  // noinspection ScalaDeprecation
  private def buildConf(params: java.util.Map[String, _]): Map[String, String] = {
    val builder = Map.newBuilder[String, String]
    // pick up any hadoop props, to e.g. make it a bit easier to configure s3 access based on s3a settings
    // only do this if hadoop file exist on the classpath, to avoid a hard runtime dependency on hadoop
    if (Seq("core-site.xml", "hdfs-site.xml").exists(getClass.getClassLoader.getResource(_) != null)) {
      FileSystemDataStoreFactory.configuration.forEach { e =>
        val key = e.getKey
        if (key.startsWith("geomesa.") || key.startsWith("fs.") || key.startsWith("parquet.")) {
          builder += e.getKey -> FileSystemDataStoreFactory.configuration.get(e.getKey) // use .get to resolve envs
        }
      }
    }
    ConfigXmlParam.lookupOpt(params).foreach { xml =>
      logger.warn(s"Parameter '${ConfigXmlParam.key}' is deprecated, please use '${ConfigParam.key}' instead")
      val conf = new Configuration(false)
      conf.addResource(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)))
      // note: need to call iterator() to force loading of the resource
      conf.iterator().forEachRemaining { e => builder += e.getKey -> conf.get(e.getKey) } // use .get to resolve envs
    }
    ConfigPathsParam.lookupOpt(params).map(_.split(',').map(_.trim).filterNot(_.isEmpty)).filterNot(_.isEmpty).foreach { resources =>
      logger.warn(s"Parameter '${ConfigPathsParam.key}' is deprecated, please use '${ConfigFileParam.key}' instead")
      val conf = new Configuration(false)
      resources.foreach(HadoopUtils.addResource(conf, _))
      // note: need to call iterator() to force loading of the resource
      conf.iterator().forEachRemaining { e => builder += e.getKey -> conf.get(e.getKey) }  // use .get to resolve envs
    }
    ConfigFileParam.lookupOpt(params).foreach { f =>
      var uri = new URI(f)
      if (uri.getScheme == null) {
        uri = new URI("file", uri.getHost, uri.getPath, uri.getFragment)
      }
      val asString = try { WithClose(uri.toURL.openStream())(IOUtils.toString(_, StandardCharsets.UTF_8)) } catch {
        case NonFatal(e) =>
          throw new IllegalArgumentException(s"Invalid parameter ${ConfigFileParam.key} - could not open file: $f", e)
      }
      // use our properties parameter parsing to evaluate env vars
      ConfigParam.lookup(java.util.Map.of(ConfigParam.key, asString)).asScala.foreach { case (k, v) => builder += k -> v }
    }
    ConfigParam.lookupOpt(params).foreach(_.asScala.foreach { case (k, v) => builder += k -> v })
    AuthProviderParam.lookupOpt(params).foreach(p => builder += (AuthsParam.key -> p.getClass.getName))
    AuthsParam.lookupOpt(params).foreach(auths => builder += (AuthsParam.key -> auths))
    CatalogTypeParam.lookupOpt(params).filter(_.nonEmpty).foreach(t => builder += (CatalogUtil.ICEBERG_CATALOG_TYPE -> t))
    WritersMaxOpenPartitionsParam.lookupOpt(params).foreach(m => builder += (WritersMaxOpenPartitionsParam.key -> m.toString))
    builder.result()
  }
}

object FileSystemDataStoreFactory extends GeoMesaDataStoreInfo {

  override val DisplayName: String = "File System (GeoMesa)"
  override val Description: String = "File System Data Store"

  override val ParameterInfo: Array[GeoMesaParam[_ <: AnyRef]] =
    Array(
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.PathParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.CatalogTypeParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.ConfigParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.ConfigFileParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.QueryThreadsParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.QueryTimeoutParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.AuthProviderParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.AuthsParam,
    )

  // lazy to avoid masking classpath errors with missing hadoop
  private lazy val configuration = new Configuration()

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.PathParam.exists(params)

  @deprecated("Moved to org.locationtech.geomesa.fs.data.FileSystemDataStoreParams")
  object FileSystemDataStoreParams extends org.locationtech.geomesa.fs.data.FileSystemDataStoreParams
}
