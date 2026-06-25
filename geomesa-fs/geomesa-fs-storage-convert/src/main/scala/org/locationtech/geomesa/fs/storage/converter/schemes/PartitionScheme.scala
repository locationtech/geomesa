/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.converter.schemes

import org.geotools.api.filter.Filter

/**
  * Scheme for partitioning features into various named partitions (e.g. file paths) on disk, for
  * faster querying. Partition schemes do not have any persistent state, they only know how to map features
  * and filters to partition names
  */
trait PartitionScheme {

  /**
    *
    * @return the max depth this partition scheme goes to
    */
  def depth: Int

  /**
   * Indication of the directory structure, suitable for displaying to a user
   *
   * @return
   */
  def pattern: String

  /**
   * Get partitions that intersect the given filter
   *
   * If the filter does not constrain partitions at all, then an empty option will be returned. If
   * the filter excludes all potential partitions, then an empty list will be returned
   *
   * @param filter filter
   * @return list of intersecting filters
   */
  def getIntersectingPartitions(filter: Filter): Option[Seq[String]]
}

object PartitionScheme {

  /**
    * Simplified filter used to optimize queries
    *
    * @param filter filter that applies to these partitions
    * @param partitions list of partitions
    * @param partial partitions are partial matches (prefixes), or exact partition names
    */
  case class SimplifiedFilter(filter: Filter, partitions: Seq[String], partial: Boolean)
}
