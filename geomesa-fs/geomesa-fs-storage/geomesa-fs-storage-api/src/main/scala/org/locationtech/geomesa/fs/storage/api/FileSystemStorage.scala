/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api

import java.io.{Closeable, Flushable}

import org.geotools.data.Query
import org.locationtech.geomesa.fs.storage.api.FileSystemStorage.{FileSystemUpdateWriter, FileSystemWriter}
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.SimplifiedFilter
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{PartitionMetadata, StorageFilePath}
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

/**
  * Persists simple features to a file system and provides query access. Storage implementations are fairly
  * 'lightweight', in that all state is captured in the metadata instance
  */
trait FileSystemStorage extends Compactable with Closeable {

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
    * Pass through to get all partitions from the metadata
    *
    * @return
    */
  def getPartitions: Seq[PartitionMetadata] = metadata.getPartitions()

  /**
    * Gets a list of partitions that match the given filter
    *
    * @return partitions
    */
  def getPartitions(filter: Filter): Seq[PartitionMetadata] = {
    if (filter == Filter.INCLUDE) {
      return metadata.getPartitions()
    }

    val filters = metadata.scheme.getSimplifiedFilters(filter).orNull
    if (filters == null) {
      return metadata.getPartitions()
    }

    filters.flatMap { f =>
      if (f.partial) {
        f.partitions.flatMap(p => metadata.getPartitions(Some(p)))
      } else {
        f.partitions.flatMap(metadata.getPartition)
      }
    }
  }

  /**
    * Get partitions that match a given filter. Each set of partitions will have a simplified
    * filter that should be applied to that set
    *
    * If there are no partitions that match the filter, an empty list will be returned
    *
    * @return partitions and predicates for each partition
    */
  def getPartitionFilters(filter: Filter, partition: Option[String] = None): Seq[PartitionFilter] = {
    val filters = metadata.scheme.getSimplifiedFilters(filter).orNull
    if (filters == null) {
      return Seq(PartitionFilter(filter, partition.map(Seq(_)).getOrElse(metadata.getPartitions().map(_.name))))
    }

    partition match {
      case None =>
        filters.flatMap { f =>
          val partitions = if (f.partial) {
            f.partitions.flatMap(p => metadata.getPartitions(Some(p)))
          } else {
            f.partitions.flatMap(metadata.getPartition)
          }
          if (partitions.isEmpty) { Seq.empty } else {
            Seq(PartitionFilter(f.filter, partitions.map(_.name)))
          }
        }

      case Some(p) =>
        def matches(f: SimplifiedFilter): Boolean =
          if (f.partial) { f.partitions.exists(p.startsWith) } else { f.partitions.contains(p) }
        filters.collectFirst { case f if matches(f) => PartitionFilter(f.filter, Seq(p)) }.toSeq
    }
  }

  /**
    * Get the full paths to any files contained in the partition
    *
    * @param partition partition
    * @return file paths
    */
  def getFilePaths(partition: String): Seq[StorageFilePath]

  /**
    * Get a reader for all relevant partitions
    *
    * @param query query
    * @param partition restrict results to a single partition
    * @param threads suggested threads used for reading data files
    * @return reader
    */
  def getReader(query: Query, partition: Option[String] = None, threads: Int = 1): CloseableFeatureIterator

  /**
    * Get a writer for a given partition. This method is thread-safe and can be called multiple times,
    * although this can result in multiple data files.
    *
    * @param partition partition
    * @return writer
    */
  def getWriter(partition: String): FileSystemWriter

  /**
    * Gets a modifying writer. This method is thread-safe and can be called multiple times,
    * although if a feature is modified multiple times concurrently, the last update 'wins'.
    * There is no guarantee that any concurrent modifications will be reflected in the returned
    * writer.
    *
    * @param filter the filter used to select features for modification
    * @param partition restrict results to a single partition (writes will go to the appropriate partition, regardless)
    * @param threads suggested threads used for reading data files
    * @return
    */
  def getWriter(filter: Filter, partition: Option[String] = None, threads: Int = 1): FileSystemUpdateWriter

  override def close(): Unit = metadata.close()
}

object FileSystemStorage {

  trait FileSystemWriter extends Closeable with Flushable {

    /**
      * Write a feature
      *
      * @param feature feature
      */
    def write(feature: SimpleFeature): Unit
  }

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
}
