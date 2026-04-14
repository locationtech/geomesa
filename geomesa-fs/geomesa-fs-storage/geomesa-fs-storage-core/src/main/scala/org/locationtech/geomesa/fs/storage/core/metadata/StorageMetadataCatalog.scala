/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.metadata

import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.FileSystemContext
import org.locationtech.geomesa.fs.storage.core.metadata.impl.{ConverterMetadata, ConverterMetadataCatalog, FileBasedMetadata, FileBasedMetadataCatalog, JdbcMetadata, JdbcMetadataCatalog}

import java.util.Locale

trait StorageMetadataCatalog {

  /**
   * Get the feature types known by this factory
   *
   * @return
   */
  def getTypeNames: Seq[String]

  /**
   * Load an existing metadata instance by name
   *
   * @param typeName feature type name
   * @return
   */
  def load(typeName: String): StorageMetadata

  /**
   * Create a metadata instance using the provided options
   *
   * @param sft simple feature type
   * @param partitions storage partitions
   * @param targetFileSize target file size, in bytes
   * @return
   */
  def create(sft: SimpleFeatureType, partitions: Seq[String], targetFileSize: Option[Long] = None): StorageMetadata
}

object StorageMetadataCatalog {

  /**
   * Create a new catalog instance
   *
   * @param context file system context
   * @param metadataType type of metadata
   * @param config catalog configuration options
   * @return
   */
  def apply(context: FileSystemContext, metadataType: String, config: Map[String, String]): StorageMetadataCatalog = {
    metadataType.toLowerCase(Locale.US) match {
      case FileBasedMetadata.MetadataType => new FileBasedMetadataCatalog(context)
      case JdbcMetadata.MetadataType      => new JdbcMetadataCatalog(context, config)
      case ConverterMetadata.MetadataType => new ConverterMetadataCatalog(context, config)
      case t => throw new UnsupportedOperationException(s"Metadata implementation not found for type: $t")
    }
  }
}
