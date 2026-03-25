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
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.PartitionFilter
import org.locationtech.geomesa.fs.storage.api.PartitionScheme.RangeBuilder.BoundsOrdering

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

object PartitionScheme extends LazyLogging {

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

    /**
     * Attempt to merge two bounds. Only overlapping bounds will result in a successful merge. Trying to merge
     * bounds from a different partition scheme is a logical error.
     *
     * @param other bounds to merge
     * @return
     */
    def merge(other: PartitionBounds): Option[PartitionBounds]
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

    override def merge(other: PartitionBounds): Option[PartitionBounds] = other match {
      case PartitionRange(_, lo, up) =>
        if (lower <= lo) {
          if (upper >= up) {
            Some(this)
          } else if (upper >= lo) {
            Some(PartitionRange(name, lower, up))
          } else {
            None
          }
        } else if (lower > up) {
          None
        } else if (upper >= up) {
          Some(PartitionRange(name, lo, upper))
        } else {
          Some(other)
        }

      case SinglePartition(_, value) if contains(value) => Some(this)

      case _ => None
    }
  }

  /**
   * Single row bound
   *
   * @param name partition scheme name
   * @param value single row
   */
  case class SinglePartition(name: String, value: String) extends PartitionBounds {
    override def contains(value: String): Boolean = value == this.value
    override def merge(other: PartitionBounds): Option[PartitionBounds] = if (other.contains(value)) { Some(other) } else { None }
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

    private val ranges = new java.util.ArrayList[PartitionBounds]()

    def +=(range: PartitionBounds): Unit = {
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

    def result(): Seq[PartitionBounds] = ranges.asScala.toSeq
  }

  object RangeBuilder {
    private val BoundsOrdering = Ordering.by[PartitionBounds, String] {
      case p: SinglePartition => p.value
      case p: PartitionRange => p.lower
    }
  }
}
