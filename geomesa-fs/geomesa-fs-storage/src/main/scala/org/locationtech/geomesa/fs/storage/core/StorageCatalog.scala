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
   *
   * A given update instance is mutable - methods allow for operation chaining, but will return the same original instance.
   */
  trait CatalogUpdate {

    /**
     * Updates to columns
     *
     * @return
     */
    def columns(): CatalogUpdate.ColumnUpdate

    /**
     * Non-column table updates
     *
     * @return
     */
    def table(): CatalogUpdate.TableUpdate

    /**
     * Updates to metadata values
     *
     * @return
     */
    def metadata(): CatalogUpdate.MetadataUpdate

    /**
     * Apply the changes. An update can only be applied once, even if the apply fails - to re-try a failed apply,
     * a new update must be created.
     */
    def apply(): Unit
  }

  object CatalogUpdate {

    trait ColumnUpdate {

      /**
       * Add a new column
       *
       * @param attribute attribute to add
       * @return the parent update, for operation chaining
       */
      def add(attribute: AttributeDescriptor): CatalogUpdate

      /**
       * Update an existing column, by widening the type. Note: not all updates are supported. For structural types,
       * nested columns can be added, removed, or updated based solely on the column name.
       *
       * @param update updated attribute
       * @return the parent update, for operation chaining
       */
      def update(update: AttributeDescriptor): CatalogUpdate

      /**
       * Rename an existing column
       *
       * @param attribute current attribute name
       * @param newName updated attribute name
       * @return the parent update, for operation chaining
       */
      def rename(attribute: String, newName: String): CatalogUpdate

      /**
       * Remove a column
       *
       * @param attribute attribute name to remove
       * @return the parent update, for operation chaining
       */
      def remove(attribute: String): CatalogUpdate

      /**
       * Change the position of an attribute
       *
       * @param attribute attribute to move
       * @param position new position, in the range [0, attributes.length - 1]. other attributes will be shifted left or right
       *                 as necessary, depending on the original and the destination position
       * @return the parent update, for operation chaining
       */
      def reorder(attribute: String, position: Int): CatalogUpdate
    }

    trait TableUpdate {

      /**
       * Rename the table
       *
       * @param newName new name
       * @return the parent update, for operation chaining
       */
      def rename(newName: String): CatalogUpdate

      /**
       * Overwrite the current partition spec
       *
       * @param partitions new partitions
       * @return the parent update, for operation chaining
       */
      def setPartitions(partitions: Seq[String]): CatalogUpdate

      /**
       * Add bloom filters to columns
       *
       * @param filters filters
       * @return the parent update, for operation chaining
       */
      def addBloomFilters(filters: Seq[BloomFilterConfig]): CatalogUpdate

      /**
       * Remove bloom filters from columns
       *
       * @param attributes columns to remove the filters from
       * @return the parent update, for operation chaining
       */
      def removeBloomFilters(attributes: Seq[String]): CatalogUpdate
    }

    trait MetadataUpdate {

      /**
       * Set or clear the target file size
       *
       * @param size target size, in bytes, or None to clear an existing target size
       * @return the parent update, for operation chaining
       */
      def setTargetFileSize(size: Option[Long]): CatalogUpdate

      /**
       * Sets a user data property
       *
       * @param key key
       * @param value value, empty to remove an existing key
       * @return the parent update, for operation chaining
       */
      def setUserData(key: String, value: Option[String]): CatalogUpdate
    }
  }
}
