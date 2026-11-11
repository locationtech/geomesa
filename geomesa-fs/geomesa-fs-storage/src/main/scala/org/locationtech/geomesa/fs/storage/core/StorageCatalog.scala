/***********************************************************************
 * Copyright (c) 2013-2026 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.`type`.AttributeDescriptor
import org.geotools.api.feature.simple.SimpleFeatureType
import org.locationtech.geomesa.fs.storage.core.StorageCatalog.CatalogUpdate
import org.locationtech.geomesa.fs.storage.core.iceberg.IcebergCatalog
import org.locationtech.geomesa.fs.storage.core.parquet.BloomFilterConfig

import java.io.Closeable

/**
 * A catalog for creating, listing and retrieving storage instances
 */
trait StorageCatalog extends Closeable {

  /**
   * Configuration properties
   *
   * @return
   */
  def conf: Map[String, String]

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
   * @param bloomFilters parquet bloom filters to write
   * @return
   */
  def create(
      sft: SimpleFeatureType,
      partitions: Seq[String],
      targetFileSize: Option[Long] = None,
      bloomFilters: Seq[BloomFilterConfig] = Seq.empty): FileSystemStorage

  /**
   * Update an existing storage instance
   *
   * @param typeName type to update
   */
  def update(typeName: String): CatalogUpdate

  /**
   * Delete a storage instance
   *
   * @param typeName feature type name
   * @param purge if true, all underlying data will be deleted
   */
  def delete(typeName: String, purge: Boolean): Unit
}

object StorageCatalog extends LazyLogging {

  val NamespaceConfigKey = "namespace"

  /**
   * Create a new storage catalog instance
   *
   * @param conf config
   * @return
   */
  def apply(conf: Map[String, String]): StorageCatalog = new IcebergCatalog(conf)

  /**
   * Update API. All operations are metadata only, in that they do not update any existing data files. Existing files
   * will still be accessible using the updated table, but they will not benefit from a new partition scheme, they will only
   * return null values for new columns, etc.
   */
  trait CatalogUpdate {

    /**
     * Updates to columns
     *
     * @return
     */
    def columns(): CatalogUpdate.ColumnUpdate

    /**
     * Updates to indices and partitioning
     *
     * @return
     */
    def indices(): CatalogUpdate.IndexUpdate

    /**
     * Updates to metadata values
     *
     * @return
     */
    def metadata(): CatalogUpdate.MetadataUpdate
  }

  object CatalogUpdate {

    trait ColumnUpdate {

      /**
       * Add a new column
       *
       * @param attribute attribute to add
       */
      def add(attribute: AttributeDescriptor): Unit

      /**
       * Update an existing column, by widening the type. Note: not all updates are supported. For structural types,
       * nested columns can be added, removed, or updated based solely on the column name.
       *
       * @param update updated attribute
       */
      def update(update: AttributeDescriptor): Unit

      /**
       * Rename an existing column
       *
       * @param attribute current attribute name
       * @param newName updated attribute name
       */
      def rename(attribute: String, newName: String): Unit

      /**
       * Remove a column
       *
       * @param attribute attribute name to remove
       */
      def remove(attribute: String): Unit

      /**
       * Change the position of an attribute
       *
       * @param attribute attribute to move
       * @param position new position, in the range [0, attributes.length - 1]. other attributes will be shifted left or right
       *                 as necessary, depending on the original and the destination position
       */
      def reorder(attribute: String, position: Int): Unit
    }

    trait IndexUpdate {

      /**
       * Overwrite the current partition spec
       *
       * @param partitions new partitions
       */
      def setPartitions(partitions: Seq[String]): Unit

      /**
       * Add bloom filters to columns
       *
       * @param filters filters
       */
      def addBloomFilters(filters: Seq[BloomFilterConfig]): Unit

      /**
       * Remove bloom filters from columns
       *
       * @param attributes columns to remove the filters from
       */
      def removeBloomFilters(attributes: Seq[String]): Unit
    }

    trait MetadataUpdate {

      /**
       * Set or clear the target file size
       *
       * @param size target size, in bytes, or None to clear an existing target size
       */
      def setTargetFileSize(size: Option[Long]): Unit
    }
  }
}
