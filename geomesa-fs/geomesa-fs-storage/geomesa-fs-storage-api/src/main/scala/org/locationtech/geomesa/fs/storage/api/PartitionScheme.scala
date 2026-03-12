/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api

import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.PartitionFilter

/**
  * Scheme for partitioning features into various named partitions (e.g. file paths) on disk, for
  * faster querying. Partition schemes do not have any persistent state, they only know how to map features
  * and filters to partition names
  */
trait PartitionScheme {

  /**
   * Name of this partition scheme
   *
   * @return
   */
  def name: String

  /**
    * Return the partition in which a SimpleFeature should be stored
    *
    * @param feature simple feature
    * @return partition name
    */
  def getPartition(feature: SimpleFeature): String

  /**
   * Get partitions that intersect the given filter
   *
   * If the filter does not constrain partitions at all, then an empty option will be returned. If
   * the filter excludes all potential partitions, then an empty list will be returned
   *
   * @param filter filter
   * @return list of intersecting filters
   */
  def getIntersectingPartitions(filter: Filter): Option[Seq[PartitionFilter]]

  /**
   * Get a filter that will cover a partitions, i.e. the filter will return all features
   * in the given partition and none from other partitions
   *
   * @param partition partition to cover
   * @return filter
   */
  def getCoveringFilter(partition: String): Filter
}

object PartitionScheme {

  /**
   * Partition filter
   *
   * @param bounds list of bounds that satisfy the filter
   * @param filter additional filter (not captured by the bounds) to apply to any results
   */
  case class PartitionFilter(bounds: Seq[PartitionBounds], filter: Option[Filter]) {
    def contains(value: String): Boolean = bounds.exists(_.contains(value))
  }

  /**
   * Bounds for a partition query
   */
  sealed trait PartitionBounds {

    /**
     * Name of the partition scheme
     *
     * @return
     */
    def name: String

    /**
     * Is the value contained in this bounds
     *
     * @param value partition value
     * @return
     */
    def contains(value: String): Boolean
  }

  /**
   * Ranged bounds
   *
   * @param name partition scheme name
   * @param lower lower bound, inclusive
   * @param upper upper bound, exclusive
   */
  case class PartitionRange(name: String, lower: String, upper: String) extends PartitionBounds {
    override def contains(value: String): Boolean = value >= lower && value < upper
  }

  /**
   * Single row bound
   *
   * @param name partition scheme name
   * @param value single row
   */
  case class SinglePartition(name: String, value: String) extends PartitionBounds {
    override def contains(value: String): Boolean = value == this.value
  }
}
