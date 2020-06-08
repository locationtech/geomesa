/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api

import java.io.Closeable

import org.apache.hadoop.fs.Path
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.PartitionMetadata
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.StorageFileAction.StorageFileAction
import org.locationtech.jts.geom.Envelope
import org.opengis.feature.simple.SimpleFeatureType

/**
  * Metadata interface for managing storage partitions. Metadata implementations can be fairly expensive to
  * instantiate, as they maintain all the partitions and files for a given storage instance. Generally,
  * they may not load any partition state until `reload` is invoked - this allows for fast access in the cases
  * where partition state is not required (e.g. access to partition scheme, blind writes, etc)
  */
trait StorageMetadata extends Compactable with Closeable {

  /**
    * The schema for SimpleFeatures stored in the file system storage
    *
    * @return schema
    */
  def sft: SimpleFeatureType

  /**
    * The encoding of the underlying data files
    *
    * @return encoding
    */
  def encoding: String

  /**
    * The partition scheme used to partition features for storage and querying
    *
    * @return partition scheme
    */
  def scheme: PartitionScheme

  /**
    * Are partitions stored as leaves (multiple partitions in a single folder), or does each
    * partition have a unique folder. Using leaf storage can reduce the level of nesting and make
    * file system operations faster in some cases.
    *
    * @return leaf
    */
  def leafStorage: Boolean

  /**
    * Get a partition by name. Ensure that `reload` has been invoked at least once before calling this method
    *
    * @param name partition name
    * @return partition metadata, if partition exists
    */
  def getPartition(name: String): Option[PartitionMetadata]

  /**
    * Get all partitions, with an optional prefix filter. Ensure that `reload` has been invoked at least
    * once before calling this method
    *
    * @param prefix prefix used to match partition names
    * @return all partitions
    */
  def getPartitions(prefix: Option[String] = None): Seq[PartitionMetadata]

  /**
    * Add (or update) metadata for a partition
    *
    * @param partition partition
    */
  def addPartition(partition: PartitionMetadata): Unit

  /**
    * Update (or delete) metadata for a partition
    *
    * @param partition partition
    */
  def removePartition(partition: PartitionMetadata): Unit

  @deprecated("deprecated with no replacement")
  def reload(): Unit = {}
}

object StorageMetadata {

  implicit val StorageFileOrdering: Ordering[StorageFile] = Ordering.by[StorageFile, Long](_.timestamp).reverse

  implicit val StorageFilePathOrdering: Ordering[StorageFilePath] =
    Ordering.by[StorageFilePath, Long](_.file.timestamp).reverse

  /**
    * Metadata for a given partition
    *
    * @param name partition name
    * @param files list of files in the partition (relative to the root directory)
    * @param bounds estimated spatial bounds for this partition, if known
    * @param count estimated count of features in this partition
    */
  case class PartitionMetadata(name: String, files: Seq[StorageFile], bounds: Option[PartitionBounds], count: Long) {

    /**
      * Combine two metadata instances for the same partition
      *
      * @param other metadata to combine
      * @return
      */
    def +(other: PartitionMetadata): PartitionMetadata = {
      val merged = bounds.map(b => other.bounds.map(_ + b).getOrElse(b)).orElse(other.bounds)
      copy(files = files ++ other.files, bounds = merged, count = count + other.count)
    }

    /**
      * Remove some metadata for the same partition.
      *
      * Note that this is a lossy operation, as the reduced bounds aren't known
      *
      * @param other metadata to remove
      * @return
      */
    def -(other: PartitionMetadata): PartitionMetadata =
      copy(files = files.diff(other.files), count = math.max(0, count - other.count))
  }

  /**
    * Holds a storage file
    *
    * @param name file name (relative to the root path)
    * @param timestamp timestamp for the file
    * @param action type of file (append, modify, delete)
    */
  case class StorageFile(name: String, timestamp: Long, action: StorageFileAction = StorageFileAction.Append)

  /**
    * Holds a storage file path
    *
    * @param file storage file
    * @param path full path to the file
    */
  case class StorageFilePath(file: StorageFile, path: Path)

  /**
    * Action related to a storage file
    */
  object StorageFileAction extends Enumeration {
    type StorageFileAction = Value
    val Append, Modify, Delete = Value
  }

  /**
    * Immutable representation of an envelope
    *
    * Note that conversions to/from 'null' envelopes should be handled carefully, as envelopes are considered
    * null if xmin > xmax, however, when instantiating an envelope it will re-order the coordinates:
    *
    * ```
    *   val env = new Envelope()
    *   val copy = new Envelope(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
    *   copy == env // false
    * ```
    *
    * Thus, ensure that 'null' envelopes are converted to `None` and not directly to a bounds object. See
    * `PartitionBounds.apply`
    *
    * @param xmin min x dimension
    * @param ymin min y dimension
    * @param xmax max x dimension
    * @param ymax max y dimension
    */
  case class PartitionBounds(xmin: Double, ymin: Double, xmax: Double, ymax: Double) {

    /**
      * Calculate the minimal bounds encompassing both bounds
      *
      * @param b other bounds
      * @return
      */
    def +(b: PartitionBounds): PartitionBounds =
      PartitionBounds(math.min(xmin, b.xmin), math.min(ymin, b.ymin), math.max(xmax, b.xmax), math.max(ymax, b.ymax))

    /**
      * Convert to a mutable envelope
      *
      * @return
      */
    def envelope: Envelope = new Envelope(xmin, xmax, ymin, ymax)
  }

  object PartitionBounds {

    /**
      * Converts an envelope to a bounds, handling 'null' (empty) envelopes
      *
      * @param env envelope
      * @return
      */
    def apply(env: Envelope): Option[PartitionBounds] =
      if (env.isNull) { None } else { Some(PartitionBounds(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)) }
  }
}
