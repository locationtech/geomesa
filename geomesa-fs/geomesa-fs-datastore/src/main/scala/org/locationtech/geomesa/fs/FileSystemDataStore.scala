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
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.data.{DataAccessFactory, DataStore, DataStoreFactorySpi, Query}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.fs.storage.api.{FileSystemStorage, FileSystemStorageFactory}
import org.locationtech.geomesa.fs.storage.common.PartitionScheme
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
    val sft =
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

    val path = new Path(PathParam.lookUp(params).asInstanceOf[String])
    val encoding = EncodingParam.lookUp(params).asInstanceOf[String]
    // TODO: handle errors

    val conf = new Configuration()
    val storage = storageFactory.iterator().filter(_.canProcess(params)).map(_.build(params)).headOption.getOrElse {
      throw new IllegalArgumentException("Can't create storage factory with the provided params")
    }
    val fs = path.getFileSystem(conf)

    val readThreads = Option(ReadThreadsParam.lookUp(params)).map(_.asInstanceOf[java.lang.Integer])
      .getOrElse(ReadThreadsParam.getDefaultValue.asInstanceOf[java.lang.Integer])

    val ds = new FileSystemDataStore(fs, path, storage, readThreads, conf)
    Option(NamespaceParam.lookUp(params).asInstanceOf[String]).foreach(ds.setNamespaceURI)
    ds
  }

  override def createNewDataStore(params: util.Map[String, io.Serializable]): DataStore =
    createDataStore(params)

  override def isAvailable: Boolean = true

  override def canProcess(params: util.Map[String, io.Serializable]): Boolean =
    params.containsKey(PathParam.getName) && params.containsKey(EncodingParam.getName)

  override def getParametersInfo: Array[DataAccessFactory.Param] =
    Array(PathParam, EncodingParam, NamespaceParam)

  override def getDisplayName: String = "File System (GeoMesa)"

  override def getDescription: String = "File System Based Data Store"

  override def getImplementationHints: util.Map[RenderingHints.Key, _] =
    new util.HashMap[RenderingHints.Key, Serializable]()
}

object FileSystemDataStoreParams {
  val PathParam            = new Param("fs.path", classOf[String], "Root of the filesystem hierarchy", true)
  val EncodingParam        = new Param("fs.encoding", classOf[String], "Encoding of data", true)

  val ConverterNameParam   = new Param("fs.options.converter.name", classOf[String], "Converter Name", false)
  val ConverterConfigParam = new Param("fs.options.converter.conf", classOf[String], "Converter Typesafe Config", false)
  val SftNameParam         = new Param("fs.options.sft.name", classOf[String], "SimpleFeatureType Name", false)
  val SftConfigParam       = new Param("fs.options.sft.conf", classOf[String], "SimpleFeatureType Typesafe Config", false)

  val ReadThreadsParam     = new Param("read-threads", classOf[java.lang.Integer], "Read Threads", false, 4)

  val NamespaceParam       = new Param("namespace", classOf[String], "Namespace", false)
}
