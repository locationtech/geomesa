/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.data

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.geotools.data.Query
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.locationtech.geomesa.fs.storage.api._
import org.locationtech.geomesa.fs.storage.common.StorageKeys
import org.locationtech.geomesa.fs.storage.common.metadata.FileBasedMetadata
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, HasGeoMesaMetadata, NoOpMetadata}
import org.locationtech.geomesa.index.stats.MetadataBackedStats.UnoptimizedRunnableStats
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.GeoMesaSchemaValidator
import org.locationtech.geomesa.utils.io.CloseQuietly
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.concurrent.duration.Duration
import scala.util.control.NonFatal

class FileSystemDataStore(
    fc: FileContext,
    conf: Configuration,
    root: Path,
    readThreads: Int,
    writeTimeout: Duration,
    defaultEncoding: Option[String],
    namespace: Option[String]
  ) extends ContentDataStore with HasGeoMesaStats with HasGeoMesaMetadata[String] with LazyLogging {

  namespace.foreach(setNamespaceURI)

  private val manager = FileSystemStorageManager(fc, conf, root, namespace)

  override val metadata: GeoMesaMetadata[String] = new NoOpMetadata[String]
  override val stats: GeoMesaStats = new UnoptimizedRunnableStats(this)

  override def createTypeNames(): java.util.List[Name] = {
    val names = new java.util.ArrayList[Name]()
    manager.storages().foreach(storage => names.add(storage.metadata.sft.getName))
    names
  }

  override def createSchema(sft: SimpleFeatureType): Unit = {
    import org.locationtech.geomesa.fs.storage.common.RichSimpleFeatureType

    manager.storage(sft.getTypeName) match {
      case Some(s) =>
        logger.warn(s"Schema already exists: ${SimpleFeatureTypes.encodeType(s.metadata.sft, includeUserData = true)}")

      case None =>
        GeoMesaSchemaValidator.validate(sft)

        // remove the configs in the user data as they're persisted separately
        val meta = sft.removeMetadata().getOrElse(FileBasedMetadata.DefaultOptions)
        val scheme = sft.removeScheme().getOrElse {
          throw new IllegalArgumentException("Partition scheme must be specified in the SimpleFeatureType user data")
        }
        val encoding = sft.removeEncoding().orElse(defaultEncoding).getOrElse {
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

        val path = manager.defaultPath(sft.getTypeName)
        val context = FileSystemContext(fc, conf, path, namespace)

        val metadata = StorageMetadataFactory.create(context, meta, Metadata(sft, encoding, scheme, leafStorage))
        try { manager.register(path, FileSystemStorageFactory(context, metadata)) } catch {
          case NonFatal(e) => CloseQuietly(metadata).foreach(e.addSuppressed); throw e
        }
    }
  }

  override def createFeatureSource(entry: ContentEntry): ContentFeatureSource =
    new FileSystemFeatureStore(storage(entry.getTypeName), entry, Query.ALL, readThreads, writeTimeout)

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

