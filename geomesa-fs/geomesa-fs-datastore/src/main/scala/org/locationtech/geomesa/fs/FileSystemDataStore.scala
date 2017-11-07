/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import java.awt.RenderingHints
import java.util.ServiceLoader
import java.{io, util}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.data.{DataAccessFactory, DataStore, DataStoreFactorySpi, Query}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.fs.storage.api.{FileSystemStorage, FileSystemStorageFactory}
import org.locationtech.geomesa.fs.storage.common.PartitionScheme
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.NamespaceParams
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._

class FileSystemDataStore(fs: FileSystem,
                          val root: Path,
                          val storage: FileSystemStorage,
                          readThreads: Int,
                          conf: Configuration) extends ContentDataStore {
  import scala.collection.JavaConversions._

  override def createTypeNames(): util.List[Name] = {
    storage.listFeatureTypes().map(name => new NameImpl(getNamespaceURI, name.getTypeName) : Name).asJava
  }

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    storage.listFeatureTypes().find { f => f.getTypeName.equals(entry.getTypeName) }
      .getOrElse(throw new RuntimeException(s"Could not find feature type ${entry.getTypeName}"))
    new FileSystemFeatureStore(entry, Query.ALL, fs, storage, readThreads)
  }

  override def createSchema(sft: SimpleFeatureType): Unit = {
    storage.createNewFeatureType(sft, PartitionScheme.extractFromSft(sft))
  }
}

class FileSystemDataStoreFactory extends DataStoreFactorySpi {
  import FileSystemDataStoreParams._
  private val storageFactory = ServiceLoader.load(classOf[FileSystemStorageFactory])

  override def createDataStore(params: util.Map[String, io.Serializable]): DataStore = {
    import org.locationtech.geomesa.utils.conversions.ScalaImplicits.RichIterator

    import scala.collection.JavaConversions._

    val path = new Path(PathParam.lookup(params))
    val encoding = EncodingParam.lookup(params)

    val conf = new Configuration()
    val storage = storageFactory.iterator().filter(_.canProcess(params)).map(_.build(params)).headOption.getOrElse {
      throw new IllegalArgumentException("Can't create storage factory with the provided params")
    }
    val fs = path.getFileSystem(conf)

    val readThreads = ReadThreadsParam.lookup(params)

    val ds = new FileSystemDataStore(fs, path, storage, readThreads, conf)
    NamespaceParam.lookupOpt(params).foreach(ds.setNamespaceURI)
    ds
  }

  override def createNewDataStore(params: util.Map[String, io.Serializable]): DataStore =
    createDataStore(params)

  override def isAvailable: Boolean = true

  override def canProcess(params: util.Map[String, io.Serializable]): Boolean =
    PathParam.exists(params) && EncodingParam.exists(params)

  override def getParametersInfo: Array[DataAccessFactory.Param] =
    Array(PathParam, EncodingParam, NamespaceParam)

  override def getDisplayName: String = "File System (GeoMesa)"

  override def getDescription: String = "File System Based Data Store"

  override def getImplementationHints: util.Map[RenderingHints.Key, _] =
    new util.HashMap[RenderingHints.Key, Serializable]()
}

object FileSystemDataStoreParams extends NamespaceParams {
  val PathParam            = new GeoMesaParam[String]("fs.path", "Root of the filesystem hierarchy", required = true)
  val EncodingParam        = new GeoMesaParam[String]("fs.encoding", "Encoding of data", required = true)

  val ConverterNameParam   = new GeoMesaParam[String]("fs.options.converter.name", "Converter Name")
  val ConverterConfigParam = new GeoMesaParam[String]("fs.options.converter.conf", "Converter Typesafe Config", largeText = true)
  val SftNameParam         = new GeoMesaParam[String]("fs.options.sft.name", "SimpleFeatureType Name")
  val SftConfigParam       = new GeoMesaParam[String]("fs.options.sft.conf", "SimpleFeatureType Typesafe Config", largeText = true)

  val ReadThreadsParam     = new GeoMesaParam[Integer]("fs.read-threads", "Read Threads", default = 4)
}
