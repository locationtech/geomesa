/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import java.awt.RenderingHints
import java.{io, util}

import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.data.{DataAccessFactory, DataStore, DataStoreFactorySpi, Query}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.locationtech.geomesa.fs.storage.common.{FileSystemStorageFactory, PartitionScheme}
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.NamespaceParams
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, HasGeoMesaMetadata, NoOpMetadata}
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats, UnoptimizedRunnableStats}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.collection.JavaConverters._

class FileSystemDataStore(val storage: FileSystemStorage, readThreads: Int)
    extends ContentDataStore with HasGeoMesaStats with HasGeoMesaMetadata[String] {

  import scala.collection.JavaConversions._

  override val metadata: GeoMesaMetadata[String] = new NoOpMetadata[String]
  override val stats: GeoMesaStats = new UnoptimizedRunnableStats(this)

  override def createTypeNames(): util.List[Name] = {
    storage.getFeatureTypes.map(name => new NameImpl(getNamespaceURI, name.getTypeName) : Name).asJava
  }

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    storage.getFeatureTypes.find(_.getTypeName == entry.getTypeName)
      .getOrElse(throw new RuntimeException(s"Could not find feature type ${entry.getTypeName}"))
    new FileSystemFeatureStore(storage, entry, Query.ALL, readThreads)
  }

  override def createSchema(sft: SimpleFeatureType): Unit = {
    storage.createNewFeatureType(sft, PartitionScheme.extractFromSft(sft))
  }
}

class FileSystemDataStoreFactory extends DataStoreFactorySpi {

  import FileSystemDataStoreParams._

  override def createDataStore(params: java.util.Map[String, io.Serializable]): DataStore = {
    val storage = Option(FileSystemStorageFactory.getFileSystemStorage(params)).getOrElse {
      throw new IllegalArgumentException("Can't create storage factory with the provided params")
    }

    // Need to do more tuning here. On a local system 1 thread (so basic producer/consumer) was best
    // because Parquet is also threading the reads underneath I think. using prod/cons pattern was
    // about 30% faster but increasing beyond 1 thread slowed things down. This could be due to the
    // cost of serializing simple features though. need to investigate more.
    //
    // However, if you are doing lots of filtering it appears that bumping the threads up high
    // can be very useful. Seems possibly numcores/2 might is a good setting (which is a standard idea)

    val readThreads = ReadThreadsParam.lookup(params)

    val ds = new FileSystemDataStore(storage, readThreads)
    NamespaceParam.lookupOpt(params).foreach(ds.setNamespaceURI)
    ds
  }

  override def createNewDataStore(params: java.util.Map[String, io.Serializable]): DataStore =
    createDataStore(params)

  override def isAvailable: Boolean = true

  override def canProcess(params: java.util.Map[String, io.Serializable]): Boolean =
    FileSystemStorageFactory.canProcess(params)

  override def getParametersInfo: Array[DataAccessFactory.Param] = {
    import org.locationtech.geomesa.fs.storage.common.FileSystemStorageFactory.{ConfParam, EncodingParam, PathParam}
    Array(EncodingParam, PathParam, ReadThreadsParam, ConfParam, NamespaceParam)
  }

  override def getDisplayName: String = "File System (GeoMesa)"

  override def getDescription: String = "File System Based Data Store"

  override def getImplementationHints: util.Map[RenderingHints.Key, _] =
    new util.HashMap[RenderingHints.Key, Serializable]()
}

object FileSystemDataStoreParams extends NamespaceParams {
  val WriterFileTimeout = SystemProperty("geomesa.fs.writer.partition.timeout", "60s")
  val ReadThreadsParam  = new GeoMesaParam[Integer]("fs.read-threads", "Read Threads", default = 4)
}
