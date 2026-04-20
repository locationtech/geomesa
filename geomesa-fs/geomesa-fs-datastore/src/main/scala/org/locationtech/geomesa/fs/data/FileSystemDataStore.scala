/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import com.github.benmanes.caffeine.cache.{CacheLoader, Caffeine}
import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.data.Query
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.fs.data.FileSystemDataStore.FileSystemDataStoreConfig
import org.locationtech.geomesa.fs.storage.core.{FileSystemContext, FileSystemStorage, FileSystemStorageFactory, StorageMetadataCatalog}
import org.locationtech.geomesa.index.stats.RunnableStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.GeoMesaSchemaValidator
import org.locationtech.geomesa.utils.io.CloseWithLogging

import scala.concurrent.duration.Duration

/**
 * File system data store
 *
 * @param storageFactory storage factory
 * @param catalog metadata catalog
 * @param config config
 */
class FileSystemDataStore(storageFactory: FileSystemStorageFactory, catalog: StorageMetadataCatalog, config: FileSystemDataStoreConfig)
    extends ContentDataStore with HasGeoMesaStats with LazyLogging {

  import scala.collection.JavaConverters._

  private val cache = Caffeine.newBuilder().build(
    new CacheLoader[String, FileSystemStorage]() {
      override def load(k: String): FileSystemStorage = storageFactory.apply(config.context, catalog.load(k))
    }
  )

  config.context.namespace.foreach(setNamespaceURI)

  override val stats: GeoMesaStats = new UnoptimizedRunnableStats(this)

  override def createTypeNames(): java.util.List[Name] = {
    val names = new java.util.ArrayList[Name]()
    catalog.getTypeNames.foreach(name => names.add(new NameImpl(config.context.namespace.orNull, name)))
    names
  }

  override def createSchema(original: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.fs.storage.core.RichSimpleFeatureType
    if (catalog.getTypeNames.contains(original.getTypeName)) {
      logger.warn(
        s"Schema already exists: ${SimpleFeatureTypes.encodeType(cache.get(original.getTypeName).metadata.sft, includeUserData = true)}")
    } else {
      // copy the feature type so that we don't affect it when removing user data, below
      val sft = SimpleFeatureTypes.copy(original)

      GeoMesaSchemaValidator.validate(sft)
      // remove the configs in the user data as they're persisted separately
      val scheme = sft.removeScheme().getOrElse {
        throw new IllegalArgumentException("Partition scheme must be specified in the SimpleFeatureType user data")
      }
      val fileSize = sft.removeTargetFileSize()
      cache.put(sft.getTypeName, storageFactory.apply(config.context, catalog.create(sft, scheme, fileSize)))
    }
  }

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource =
    new FileSystemFeatureStore(storage(entry.getTypeName), entry, Query.ALL, config.readThreads, config.writeTimeout, config.queryTimeout)

  /**
   * Get a handle on the underlying storage instance for a given simple feature type
   *
   * @param typeName simple feature type name
   * @return
   */
  def storage(typeName: String): FileSystemStorage = {
    val storage = cache.get(typeName)
    if (storage == null) {
      throw new IllegalArgumentException(s"Schema '$typeName' doesn't exist or could not be loaded")
    }
    storage
  }

  override def dispose(): Unit = {
    try {
      CloseWithLogging(cache.asMap().asScala.values)
      cache.invalidateAll()
    } finally {
      super.dispose()
    }
  }
}

object FileSystemDataStore {
  case class FileSystemDataStoreConfig(
    context: FileSystemContext, // note, this is expected to be a shared resource, and is not cleaned up on data store dispose
    readThreads: Int,
    writeTimeout: Duration,
    queryTimeout: Option[Duration],
  )
}
