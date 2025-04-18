/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, FileSystem, Path}
import org.geotools.api.data.Query
import org.geotools.api.feature.`type`.Name
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.locationtech.geomesa.fs.data.FileSystemDataStore.FileSystemDataStoreConfig
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.StorageKeys
import org.locationtech.geomesa.fs.storage.common.metadata.FileBasedMetadata
import org.locationtech.geomesa.fs.storage.common.utils.PathCache
import org.locationtech.geomesa.index.stats.RunnableStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.GeoMesaSchemaValidator
import org.locationtech.geomesa.utils.io.CloseQuietly

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

/**
 * File system data store
 *
 * @param fs file system - note, this is expected to be a shared resource, and is not cleaned up on data store dispose
 * @param config config
 */
class FileSystemDataStore(fs: FileSystem, config: FileSystemDataStoreConfig)
    extends ContentDataStore with HasGeoMesaStats with LazyLogging {

  // noinspection ScalaUnusedSymbol
  @deprecated("Use FileSystem instead of FileContext")
  def this(
      fc: FileContext,
      conf: Configuration,
      root: Path,
      readThreads: Int,
      writeTimeout: Duration,
      defaultEncoding: Option[String],
      namespace: Option[String]) = {
    this(FileSystem.get(root.toUri, conf),
      FileSystemDataStoreConfig(conf, root, readThreads, writeTimeout, None, defaultEncoding, namespace))
  }

  config.namespace.foreach(setNamespaceURI)

  private val manager = FileSystemStorageManager(fs, config.conf, config.root, config.namespace)

  override val stats: GeoMesaStats = new UnoptimizedRunnableStats(this)

  override def createTypeNames(): java.util.List[Name] = {
    val names = new java.util.ArrayList[Name]()
    manager.storages().foreach(storage => names.add(storage.metadata.sft.getName))
    names
  }

  override def createSchema(original: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

    manager.storage(original.getTypeName) match {
      case Some(s) =>
        logger.warn(s"Schema already exists: ${SimpleFeatureTypes.encodeType(s.metadata.sft, includeUserData = true)}")

      case None =>
        // copy the feature type so that we don't affect it when removing user data, below
        val sft = SimpleFeatureTypes.copy(original)

        GeoMesaSchemaValidator.validate(sft)

        // remove the configs in the user data as they're persisted separately
        val meta = sft.removeMetadata().getOrElse(FileBasedMetadata.DefaultOptions)
        val scheme = sft.removeScheme().getOrElse {
          throw new IllegalArgumentException("Partition scheme must be specified in the SimpleFeatureType user data")
        }
        val encoding = sft.removeEncoding().orElse(config.defaultEncoding).getOrElse {
          throw new IllegalArgumentException("Encoding type must be specified in either " +
              "the SimpleFeatureType user data or the data store parameters")
        }
        val leafStorage = sft.removeLeafStorage().getOrElse {
          val deprecated = scheme.options.get("leaf-storage").map { s =>
            logger.warn("Using deprecated leaf-storage partition-scheme option. Please define leaf-storage using " +
                s"""`simpleFeatureType.getUserData.put("${StorageKeys.LeafStorageKey}", "$s")`""")
            s.toBoolean
          }
          deprecated.getOrElse(true)
        }
        val fileSize = sft.removeTargetFileSize()

        val path = manager.defaultPath(sft.getTypeName)
        val context = FileSystemContext(fs, config.conf, path, config.namespace)

        val metadata =
          StorageMetadataFactory.create(context, meta, Metadata(sft, encoding, scheme, leafStorage, fileSize))
        try { manager.register(path, FileSystemStorageFactory(context, metadata)) } catch {
          case NonFatal(e) => CloseQuietly(metadata).foreach(e.addSuppressed); throw e
        }
        PathCache.register(fs, config.root)
        PathCache.register(fs, path)
    }
  }

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource = {
    val storage = this.storage(entry.getTypeName)
    new FileSystemFeatureStore(storage, entry, Query.ALL, config.readThreads, config.writeTimeout, config.queryTimeout)
  }

  /**
    * Get a handle on the underlying storage instance for a given simple feature type
    *
    * @param typeName simple feature type name
    * @return
    */
  def storage(typeName: String): FileSystemStorage = manager.storage(typeName).getOrElse {
    throw new IllegalArgumentException(s"Schema '$typeName' doesn't exist or could not be loaded")
  }
}

object FileSystemDataStore {
  case class FileSystemDataStoreConfig(
    conf: Configuration,
    root: Path,
    readThreads: Int,
    writeTimeout: Duration,
    queryTimeout: Option[Duration],
    defaultEncoding: Option[String],
    namespace: Option[String],
  )
}
