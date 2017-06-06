/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import java.awt.RenderingHints
import java.time.format.DateTimeFormatter
import java.util.ServiceLoader
import java.{io, util}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.data.{DataAccessFactory, DataStore, DataStoreFactorySpi, Query}
import org.locationtech.geomesa.fs.storage.api.{FileSystemStorage, FileSystemStorageFactory}
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

class FileSystemDataStore(fs: FileSystem,
                          root: Path,
                          fileSystemStorage: FileSystemStorage) extends ContentDataStore {
  import scala.collection.JavaConversions._
  override def createTypeNames(): util.List[Name] = fileSystemStorage.listFeatureTypes().map { _.getName }

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    val sft =
      fileSystemStorage.listFeatureTypes().find { f => f.getTypeName.equals(entry.getTypeName) }
        .getOrElse(throw new RuntimeException(s"Could not find feature type ${entry.getTypeName}"))
    val scheme = getPartitionScheme(sft)
    new FileSystemFeatureStore(entry, Query.ALL, scheme, fs, fileSystemStorage)
  }

  override def createSchema(featureType: SimpleFeatureType): Unit = {
    fileSystemStorage.createNewFeatureType(featureType)
  }

  private def getPartitionScheme(sft: SimpleFeatureType): PartitionScheme = {
    // TODO: load the partition scheme from the metadata
    new IntraHourPartitionScheme(15, DateTimeFormatter.ofPattern("yyyy/DDD/HHmm"), sft, "dtg")
  }

}

class FileSystemDataStoreFactory extends DataStoreFactorySpi {
  import FileSystemDataStoreParams._
  private val storageFactory = ServiceLoader.load(classOf[FileSystemStorageFactory])

  override def createDataStore(params: util.Map[String, io.Serializable]): DataStore = {
    import scala.collection.JavaConversions._
    val path = new Path(PathParam.lookUp(params).asInstanceOf[String])
    val encoding = EncodingParam.lookUp(params).asInstanceOf[String]
    // TODO: handle errors
    val storage = storageFactory.iterator().filter(_.canProcess(params)).map(_.build(params)).next()
    val fs = path.getFileSystem(new Configuration())
    // TODO: thread partitioning info through params
//    val partitionScheme = new IntraHourPartitionScheme(15, DateTimeFormatter.ofPattern("yyyy/DDD/HHmm"), storage.getSimpleFeatureType, "dtg")
    new FileSystemDataStore(fs, path, storage)
  }

  override def createNewDataStore(params: util.Map[String, io.Serializable]): DataStore =
    createDataStore(params)

  override def isAvailable: Boolean = true

  override def canProcess(params: util.Map[String, io.Serializable]): Boolean =
    params.containsKey(PathParam.getName) && params.containsKey(EncodingParam.getName)

  override def getParametersInfo: Array[DataAccessFactory.Param] = Array(PathParam, EncodingParam)

  override def getDescription: String = "GeoMesa FileSystem Data Store"

  override def getDisplayName: String = "GeoMesa-FS"

  override def getImplementationHints: util.Map[RenderingHints.Key, _] = new util.HashMap[RenderingHints.Key, Serializable]()
}

object FileSystemDataStoreParams {
  val PathParam = new Param("fs.path", classOf[String], "Root of the filesystem hierarchy", true)
  val EncodingParam = new Param("fs.encoding", classOf[String], "Encoding of data", true)

}
