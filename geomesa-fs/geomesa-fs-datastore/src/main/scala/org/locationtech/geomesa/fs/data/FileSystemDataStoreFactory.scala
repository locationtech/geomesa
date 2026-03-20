/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.api.data.DataAccessFactory.Param
import org.geotools.api.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.fs.data.FileSystemDataStore.FileSystemDataStoreConfig
import org.locationtech.geomesa.fs.storage.api.{FileSystemContext, FileSystemStorageFactory}
import org.locationtech.geomesa.fs.storage.common.metadata.{ConverterMetadata, StorageMetadataCatalog}
import org.locationtech.geomesa.fs.storage.converter.ConverterStorageFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreInfo
import org.locationtech.geomesa.security.AuthUtils
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.hadoop.HadoopUtils

import java.awt.RenderingHints
import java.io.ByteArrayInputStream
import java.nio.charset.StandardCharsets
import java.util.{Collections, Properties}

class FileSystemDataStoreFactory extends DataStoreFactorySpi {

  import org.locationtech.geomesa.fs.data.FileSystemDataStoreParams._

  import scala.collection.JavaConverters._

  override def createDataStore(params: java.util.Map[String, _]): DataStore = {
    val xml = ConfigsParam.lookupOpt(params)
    val resources = ConfigPathsParam.lookupOpt(params).toSeq.flatMap(_.split(',')).map(_.trim).filterNot(_.isEmpty)

    val conf = if (xml.isEmpty && resources.isEmpty) { FileSystemDataStoreFactory.configuration } else {
      val conf = new Configuration(FileSystemDataStoreFactory.configuration)
      // add the explicit props first, they may be needed for loading the path resources
      xml.foreach(x => conf.addResource(new ByteArrayInputStream(x.getBytes(StandardCharsets.UTF_8))))
      resources.foreach(HadoopUtils.addResource(conf, _))
      conf
    }

    val path = new Path(PathParam.lookup(params))

    // Need to do more tuning here. On a local system 1 thread (so basic producer/consumer) was best
    // because Parquet is also threading the reads underneath I think. using prod/cons pattern was
    // about 30% faster but increasing beyond 1 thread slowed things down. This could be due to the
    // cost of serializing simple features though. need to investigate more.
    //
    // However, if you are doing lots of filtering it appears that bumping the threads up high
    // can be very useful. Seems possibly numcores/2 might is a good setting (which is a standard idea)

    val readThreads = ReadThreadsParam.lookup(params)
    val writeTimeout = WriteTimeoutParam.lookup(params)
    val queryTimeout = QueryTimeoutParam.lookupOpt(params).filter(_.isFinite)

    val authProvider =
      AuthUtils.getProvider(params, AuthsParam.lookupOpt(params).map(_.split(",").filterNot(_.isEmpty).toSeq).getOrElse(Seq.empty))

    val namespace = NamespaceParam.lookupOpt(params)

    val encoding = EncodingParam.lookup(params)
    val storageFactory = FileSystemStorageFactory.factories.find(_.encoding == encoding).getOrElse {
      // this shouldn't happen since encoding param lookup should fail if it doesn't match a factory
      throw new RuntimeException(
        s"Invalid encoding parameter, does not match a storage factory instance: $encoding\n" +
          s"  Available factories: ${FileSystemStorageFactory.factories.map(_.encoding).mkString(", ")}")
    }
    val fs = FileSystem.get(path.toUri, conf)
    val context = FileSystemContext(fs, conf, path, namespace)
    val config = FileSystemDataStoreConfig(context, readThreads, writeTimeout, queryTimeout, authProvider)

    val metadataConfig = MetadataConfigParam.lookupOpt(params).getOrElse(new Properties()).asScala.toMap
    val metadataType =
      if (ConverterStorageFactory.Encoding.equalsIgnoreCase(encoding) && !MetadataTypeParam.exists(params)) {
        // for back compatibility, don't require metadata type if using converter storage
        ConverterMetadata.MetadataType
      } else {
        MetadataTypeParam.lookup(params)
      }
    // note: closeable, ensure we create this last in case there is an error in the other params (or clean it up if there is an error)
    val metadata = StorageMetadataCatalog(context, metadataType, metadataConfig)

    new FileSystemDataStore(storageFactory, metadata, config)
  }

  override def createNewDataStore(params: java.util.Map[String, _]): DataStore = createDataStore(params)

  override def isAvailable: Boolean = true

  override def canProcess(params: java.util.Map[String, _]): Boolean = FileSystemDataStoreFactory.canProcess(params)

  override def getDisplayName: String = FileSystemDataStoreFactory.DisplayName

  override def getDescription: String = FileSystemDataStoreFactory.Description

  override def getParametersInfo: Array[Param] = Array(FileSystemDataStoreFactory.ParameterInfo :+ NamespaceParam: _*)

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = Collections.emptyMap()
}

object FileSystemDataStoreFactory extends GeoMesaDataStoreInfo {

  override val DisplayName: String = "File System (GeoMesa)"
  override val Description: String = "File System Data Store"

  override val ParameterInfo: Array[GeoMesaParam[_ <: AnyRef]] =
    Array(
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.PathParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.EncodingParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.MetadataTypeParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.MetadataConfigParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.ConfigPathsParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.ConfigsParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.ReadThreadsParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.WriteTimeoutParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.QueryTimeoutParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.AuthProviderParam,
      org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.AuthsParam,
    )

  // lazy to avoid masking classpath errors with missing hadoop
  private lazy val configuration = new Configuration()

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.PathParam.exists(params) &&
      (org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.MetadataTypeParam.exists(params) ||
        org.locationtech.geomesa.fs.data.FileSystemDataStoreParams.EncodingParam.lookupOpt(params)
          .exists(_.equalsIgnoreCase(ConverterStorageFactory.Encoding)))

  @deprecated("Moved to org.locationtech.geomesa.fs.data.FileSystemDataStoreParams")
  object FileSystemDataStoreParams extends org.locationtech.geomesa.fs.data.FileSystemDataStoreParams
}
