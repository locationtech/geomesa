/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api

import com.typesafe.scalalogging.LazyLogging
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.PartitionRange
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.RangeBuilder.BoundsOrdering
import org.locationtech.geomesa.fs.storage.api.StorageMetadata.PartitionKey

import java.util.Collections

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
  def getPartition(feature: SimpleFeature): PartitionKey

  /**
   * Get partitions that intersect the given filter
   *
   * If the filter does not constrain partitions at all, then an empty option will be returned. If
   * the filter excludes all potential partitions, then an empty list will be returned
   *
   * @param filter filter
   * @return list of intersecting filters
   */
  def getRangesForFilter(filter: Filter): Option[Seq[PartitionRange]]

  /**
   * Enumerate all the partitions that intersect with the given filter
   *
   * If the filter does not constrain partitions at all, then an empty option will be returned. If
   * the filter excludes all potential partitions, then an empty list will be returned
   *
   * Note that this may return a large number of partitions if the filter is not very selective
   *
   * @param filter filter
   * @return
   */
  def getPartitionsForFilter(filter: Filter): Option[Seq[PartitionKey]]

  /**
   * Get a filter that will cover a partitions, i.e. the filter will return all features
   * in the given partition and none from other partitions
   *
   * @param partition partition to cover
   * @return filter
   */
  def getCoveringFilter(partition: String): Filter
}

object PartitionScheme extends LazyLogging {

  /**
   * Ranged bounds
   *
   * @param name partition scheme name
   * @param lower lower bound, inclusive
   * @param upper upper bound, exclusive
   */
  case class PartitionRange(name: String, lower: String, upper: String) {

    /**
     * Is the value contained in this bounds
     *
     * @param value partition value
     * @return
     */
    def contains(value: String): Boolean = value >= lower && value < upper

    /**
     * Attempt to merge two bounds. Only overlapping bounds will result in a successful merge. Trying to merge
     * bounds from a different partition scheme is a logical error.
     *
     * @param other bounds to merge
     * @return
     */
    def merge(other: PartitionRange): Option[PartitionRange] = {
      if (lower <= other.lower) {
        if (upper >= other.upper) {
          Some(this)
        } else if (upper >= other.lower) {
          Some(PartitionRange(name, lower, other.upper))
        } else {
          None
        }
      } else if (lower > other.upper) {
        None
      } else if (upper >= other.upper) {
        Some(PartitionRange(name, other.lower, upper))
      } else {
        Some(other)
      }
    }
  }

  // there should be no duplicates in covered partitions, as our bounds will not overlap,
  // but there may be multiple partial intersects with a given partition

  /**
   * Class to merge overlapping ranges.
   *
   * Our bounds extraction does not produce any overlapping ranges, but once converted to partitions there
   * may be some overlap.
   *
   * This class keeps ranges in sorted order (based on the lower bound), and uses binary search to identify
   * which range(s) may overlap with each newly generated range.
   */
  class RangeBuilder {

    import scala.collection.JavaConverters._

    private val ranges = new java.util.ArrayList[PartitionRange]()

    def +=(range: PartitionRange): Unit = {
      if (ranges.isEmpty) {
        ranges.add(range)
      } else {
        val i = Collections.binarySearch(ranges, range, BoundsOrdering)
        if (i < 0) {
          val insertionPoint = -1 * (i + 1)
          if (insertionPoint == 0) {
            ranges.get(0).merge(range) match {
              case Some(r) => ranges.set(0, r)
              case None => ranges.add(0, range)
            }
          } else if (insertionPoint == ranges.size()) {
            ranges.get(insertionPoint - 1).merge(range) match {
              case Some(r) => ranges.set(insertionPoint - 1, r)
              case None => ranges.add(range)
            }
          } else {
            ranges.get(insertionPoint - 1).merge(range) match {
              case Some(r) => ranges.set(insertionPoint - 1, r)
              case None =>
                ranges.get(insertionPoint).merge(range) match {
                  case Some(r) => ranges.set(insertionPoint, r)
                  case None => ranges.add(insertionPoint, range)
                }
            }
          }
        } else {
          ranges.get(i).merge(range) match {
            case Some(r) => ranges.set(i, r)
            case None =>
              logger.warn(s"Found a matching range that doesn't merge: ${ranges.get(i)} and $range")
              ranges.add(i, range)
          }
        }
      }
    }

    def result(): Seq[PartitionRange] = ranges.asScala.toSeq
  }

  object RangeBuilder {
    private val BoundsOrdering = Ordering.by[PartitionRange, String](_.lower)
  }
}
