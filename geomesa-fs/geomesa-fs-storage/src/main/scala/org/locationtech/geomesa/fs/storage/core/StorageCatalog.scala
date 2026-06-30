/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.iceberg.IcebergCatalog

import java.io.Closeable

/**
 * A catalog for creating, listing and retrieving storage instances
 */
trait StorageCatalog extends Closeable {

  /**
   * File system context
   *
   * @return
   */
  def context: FileSystemContext

  /**
   * Get the feature types known by this factory
   *
   * @return
   */
  def getTypeNames: Seq[String]

  /**
   * Load an existing storage instance by name
   *
   * @param typeName feature type name
   * @return
   */
  def load(typeName: String): FileSystemStorage

  /**
   * Create a storage instance using the provided options
   *
   * @param sft simple feature type
   * @param partitions storage partitions
   * @param targetFileSize target file size, in bytes
   * @return
   */
  def create(sft: SimpleFeatureType, partitions: Seq[String], targetFileSize: Option[Long] = None): FileSystemStorage
}

object StorageCatalog {

  /**
   * Create a new storage catalog instance
   *
   * @param context context
   * @return
   */
  def apply(context: FileSystemContext): StorageCatalog = new IcebergCatalog(context)
}
