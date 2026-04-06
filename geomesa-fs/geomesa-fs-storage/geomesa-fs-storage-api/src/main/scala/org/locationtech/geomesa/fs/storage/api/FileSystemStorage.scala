/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api

import org.apache.hadoop.fs.Path
import org.geotools.api.data.Query
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.{FileSystemUpdateWriter, FileSystemWriter}
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{Partition, StorageFile}

import java.io.{Closeable, Flushable}

/**
  * Persists simple features to a file system and provides query access. Storage implementations are fairly
  * 'lightweight', in that all state is captured in the metadata instance
  */
trait FileSystemStorage extends Compactable with Closeable {

  /**
   * The file encoding used by this storage
   *
   * @return encoding
   */
  def encoding: String

  /**
    * Handle to the file context, root path and configuration
    *
    * @return
    */
  def context: FileSystemContext

  /**
    * The metadata for this storage instance
    *
    * @return metadata
    */
  def metadata: StorageMetadata

  /**
    * Get a reader for all relevant partitions
    *
    * @param query query
    * @param threads suggested threads used for reading data files
    * @return reader
    */
  def getReader(query: Query, threads: Int = 1): CloseableFeatureIterator

  /**
    * Get a writer for a given partition. This method is thread-safe and can be called multiple times,
    * although this can result in multiple data files.
    *
    * @param partition partitions
    * @return writer
    */
  def getWriter(partition: Partition): FileSystemWriter

  /**
    * Gets a modifying writer. This method is thread-safe and can be called multiple times,
    * although if a feature is modified multiple times concurrently, the last update 'wins'.
    * There is no guarantee that any concurrent modifications will be reflected in the returned
    * writer.
    *
    * @param filter the filter used to select features for modification
    * @param threads suggested threads used for reading data files
    * @return
    */
  def getWriter(filter: Filter, threads: Int = 1): FileSystemUpdateWriter

  /**
   * Register a new file with this storage instance. The file must already be in a compatible format.
   *
   * @param file file to register
   * @return registered file
   */
  def register(file: Path): StorageFile

  override def close(): Unit = metadata.close()
}

object FileSystemStorage {

  /**
   * Append writer
   */
  trait FileSystemWriter extends Closeable with Flushable {

    /**
      * Write a feature
      *
      * @param feature feature
      */
    def write(feature: SimpleFeature): Unit
  }

  /**
   * Update writer
   */
  trait FileSystemUpdateWriter extends Iterator[SimpleFeature] with Closeable with Flushable {

    /**
      * Writes a modification to the last feature returned by `next`
      */
    def write(): Unit

    /**
      * Deletes the last feature returned by `next`
      */
    def remove(): Unit
  }

  /**
   * Reader trait
   */
  trait FileSystemPathReader {
    def root: Path
    def read(file: Path): Iterator[SimpleFeature] with Closeable
  }
}
