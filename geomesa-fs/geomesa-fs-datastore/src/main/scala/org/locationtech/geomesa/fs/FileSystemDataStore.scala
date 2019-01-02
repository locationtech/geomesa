/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileContext, Path}
import org.geotools.data.Query
import org.geotools.data.store.{ContentDataStore, ContentEntry, ContentFeatureSource}
import org.geotools.feature.NameImpl
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage
import org.locationtech.geomesa.fs.storage.common.{Encodings, FileSystemStorageFactory}
import org.locationtech.geomesa.index.metadata.{GeoMesaMetadata, HasGeoMesaMetadata, NoOpMetadata}
import org.locationtech.geomesa.index.stats.{GeoMesaStats, HasGeoMesaStats, UnoptimizedRunnableStats}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.locationtech.geomesa.utils.index.GeoMesaSchemaValidator
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.concurrent.duration.Duration

class FileSystemDataStore(fc: FileContext,
                          conf: Configuration,
                          root: Path,
                          readThreads: Int,
                          writeTimeout: Duration,
                          defaultEncoding: Option[String])
    extends ContentDataStore with HasGeoMesaStats with HasGeoMesaMetadata[String] with LazyLogging {

  private val manager = FileSystemStorageManager(fc, conf, root)

  override val metadata: GeoMesaMetadata[String] = new NoOpMetadata[String]
  override val stats: GeoMesaStats = new UnoptimizedRunnableStats(this)

  override def createTypeNames(): java.util.List[Name] = {
    val names = new java.util.ArrayList[Name]()
    manager.storages().foreach { storage =>
      names.add(new NameImpl(getNamespaceURI, storage.getMetadata.getSchema.getTypeName))
    }
    names
  }

  override def createSchema(sft: SimpleFeatureType): Unit = {
    manager.storage(sft.getTypeName) match {
      case Some(s) =>
        logger.warn("Schema already exists: " +
          SimpleFeatureTypes.encodeType(s.getMetadata.getSchema, includeUserData = true))

      case None =>
        GeoMesaSchemaValidator.validate(sft)
        val encoding = Encodings.getEncoding(sft).getOrElse {
          val enc = this.defaultEncoding.getOrElse {
            throw new IllegalArgumentException("Encoding type must be specified in either " +
                "the SimpleFeatureType user data or the data store parameters")
          }
          Encodings.setEncoding(sft, enc)
          enc
        }
        val path = manager.defaultPath(sft.getTypeName)
        val storage = FileSystemStorageFactory.factory(encoding).create(fc, conf, path, sft)
        manager.register(path, storage)
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
