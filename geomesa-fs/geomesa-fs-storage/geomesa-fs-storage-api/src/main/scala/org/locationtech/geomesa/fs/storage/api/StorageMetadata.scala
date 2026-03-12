/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api

import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.{Partition, StorageFile, StorageFileFilter}
import org.locationtech.jts.geom.Envelope

import java.io.Closeable
import scala.util.control.NonFatal

/**
  * Metadata interface for managing storage partitions
  */
trait StorageMetadata extends Closeable {

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
    * The partition scheme(s) used to partition features for storage and querying
    *
    * @return partition schemes
    */
  def schemes: Set[PartitionScheme]

  /**
   * Add a file
   *
   * @param file file
   */
  def addFile(file: StorageFile): Unit

  /**
   * Delete a file
   *
   * @param file file
   */
  def removeFile(file: StorageFile): Unit

  /**
   * Get all files
   *
   * @return all files
   */
  // noinspection AccessorLikeMethodIsEmptyParen
  def getFiles(): Seq[StorageFile]

  /**
   * Get files for a given partition by name
   *
   * @param partition partition
   * @return files for the given partition
   */
  def getFiles(partition: Partition): Seq[StorageFile]

  /**
   * Get files matching a given filter
   *
   * @param filter filter
   */
  def getFiles(filter: Filter): Seq[StorageFileFilter]

  /**
   * Get a previously set key-value pair
   *
   * @param key key
   * @return
   */
  def get(key: String): Option[String] = None

  /**
   * Set a key-value pair
   *
   * @param key key
   * @param value value
   */
  def set(key: String, value: String): Unit = throw new UnsupportedOperationException()

//  /**
//   * Gets a list of partitions that match the given filter
//   *
//   * @return partitions
//   */
//  def getPartitions(filter: Filter): Seq[PartitionMetadata] = {
//    if (filter == Filter.INCLUDE) {
//      return metadata.getPartitions()
//    }
//
//    val filters = metadata.scheme.getSimplifiedFilters(filter).orNull
//    if (filters == null) {
//      return metadata.getPartitions()
//    }
//
//    filters.flatMap { f =>
//      if (f.partial) {
//        f.partitions.flatMap(p => metadata.getPartitions(Some(p)))
//      } else {
//        f.partitions.flatMap(metadata.getPartition)
//      }
//    }
//  }
//
//  /**
//   * Get partitions that match a given filter. Each set of partitions will have a simplified
//   * filter that should be applied to that set
//   *
//   * If there are no partitions that match the filter, an empty list will be returned
//   *
//   * @return partitions and predicates for each partition
//   */
//  def getPartitionFilters(filter: Filter, partition: Option[String] = None): Seq[PartitionFilter] = {
//    val filters = metadata.scheme.getSimplifiedFilters(filter).orNull
//    if (filters == null) {
//      return Seq(PartitionFilter(filter, partition.map(Seq(_)).getOrElse(metadata.getPartitions().map(_.name))))
//    }
//
//    partition match {
//      case None =>
//        filters.flatMap { f =>
//          val partitions = if (f.partial) {
//            f.partitions.flatMap(p => metadata.getPartitions(Some(p)))
//          } else {
//            f.partitions.flatMap(metadata.getPartition)
//          }
//          if (partitions.isEmpty) { Seq.empty } else {
//            Seq(PartitionFilter(f.filter, partitions.map(_.name)))
//          }
//        }
//
//      case Some(p) =>
//        def matches(f: SimplifiedFilter): Boolean =
//          if (f.partial) { f.partitions.exists(p.startsWith) } else { f.partitions.contains(p) }
//        filters.collectFirst { case f if matches(f) => PartitionFilter(f.filter, Seq(p)) }.toSeq
//    }
//  }
}

object StorageMetadata {

  implicit val StorageFileOrdering: Ordering[StorageFile] = Ordering.by[StorageFile, Long](_.timestamp).reverse

  /**
   * Holds a storage file
   *
   * @param file file name (relative to the root path)
   * @param partition list of partitions that the file belongs to
   * @param count number of entries in the file
   * @param action type of file (append, modify, delete)
   * @param spatialBounds known bounds, if any, keyed by feature type attribute number
   * @param attributeBounds known bounds, if any, keyed by feature type attribute number
   * @param sort sort fields for the file, if any, as feature type attribute number
   * @param timestamp timestamp for the file
   */
  case class StorageFile(
      file: String,
      partition: Partition,
      count: Long,
      action: StorageFileAction.StorageFileAction = StorageFileAction.Append,
      spatialBounds: Seq[SpatialBounds] = Seq.empty,
      attributeBounds: Seq[AttributeBounds] = Seq.empty,
      sort: Seq[Int] = Seq.empty,
      timestamp: Long = System.currentTimeMillis(),
    )

  /**
   * A partition
   *
   * @param dims set of dimensions that make up the partition
   */
  case class Partition(dims: Set[PartitionDimension]) {
    def id: String = dims.map(_.encode).toSeq.sorted.mkString(",")
  }

  object Partition {

    val None: Partition = Partition(Set.empty[PartitionDimension])

    def apply(id: String): Partition = {
      try {
        Partition(id.split(",").map(PartitionDimension.apply).toSet)
      } catch {
        case NonFatal(e) => throw new RuntimeException(s"Invalid partition '$id'", e)
      }
    }
  }

  /**
   * A partition tag
   *
   * @param name partition scheme
   * @param value partition value
   */
  case class PartitionDimension(name: String, value: String) {
    def encode: String = s"$name:value=$value"
  }

  object PartitionDimension {
    def apply(encoded: String): PartitionDimension = {
      try {
        val valueFlag = encoded.indexOf(":value=")
        PartitionDimension(encoded.substring(0, valueFlag), encoded.substring(valueFlag + 7))
      } catch {
        case NonFatal(e) => throw new RuntimeException(s"Invalid dimension '$encoded'", e)
      }
    }
  }

  /**
    * Holds a storage file and a filter associated with it
    *
    * @param file storage file
    * @param filter filter associated with the file
    */
  case class StorageFileFilter(file: StorageFile, filter: Option[Filter])

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
   * {{{
   *   val env = new Envelope()
   *   val copy = new Envelope(env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)
   *   copy == env // false
   * }}}
   *
   * Thus, ensure that 'null' envelopes are converted to `None` and not directly to a bounds object. See
   * `PartitionBounds.apply`
   *
   * @param xmin min x dimension
   * @param ymin min y dimension
   * @param xmax max x dimension
   * @param ymax max y dimension
   */
  case class SpatialBounds(attribute: Int, xmin: Double, ymin: Double, xmax: Double, ymax: Double) {

    /**
     * Calculate the minimal bounds encompassing both bounds
     *
     * @param b other bounds
     * @return
     */
    def +(b: SpatialBounds): SpatialBounds = {
      require(attribute == b.attribute, "Trying to merge bounds from different attributes")
      SpatialBounds(attribute, math.min(xmin, b.xmin), math.min(ymin, b.ymin), math.max(xmax, b.xmax), math.max(ymax, b.ymax))
    }

    /**
     * Convert to a mutable envelope
     *
     * @return
     */
    def envelope: Envelope = new Envelope(xmin, xmax, ymin, ymax)
  }

  object SpatialBounds {

    def empty(attribute: Int): SpatialBounds = SpatialBounds(attribute, 0, 0, 0, 0) // TODO this is kind of hacky

    /**
     * Converts an envelope to a bounds, handling 'null' (empty) envelopes
     *
     * @param env envelope
     * @return
     */
    def apply(attribute: Int, env: Envelope): Option[SpatialBounds] =
      if (env.isNull) { None } else { Some(SpatialBounds(attribute, env.getMinX, env.getMinY, env.getMaxX, env.getMaxY)) }
  }

  case class AttributeBounds(attribute: Int, lower: String, upper: String)
}
