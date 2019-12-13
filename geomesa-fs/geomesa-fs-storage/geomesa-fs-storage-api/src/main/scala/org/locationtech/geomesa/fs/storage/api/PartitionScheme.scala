/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.api

import org.locationtech.geomesa.fs.storage.api.PartitionScheme.SimplifiedFilter
import org.opengis.feature.simple.SimpleFeature
import org.opengis.filter.Filter

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
    * Return the partition in which a SimpleFeature should be stored
    *
    * @param feature simple feature
    * @return partition name
    */
  def getPartitionName(feature: SimpleFeature): String

  /**
    * Return a list of modified filters and partitions. Each filter will have been simplified to
    * remove any predicates that are implicitly true for the associated partitions
    *
    * If the filter does not constrain partitions at all, then an empty option will be returned,
    * indicating all partitions must be searched. If the filter excludes all potential partitions,
    * then an empty list of partitions will be returned
    *
    * Note that this operation is based solely on the partition scheme, so may return partitions
    * that do not actually exist in a given storage instance
    *
    * @param filter filter
    * @param partition query a single partition
    * @return list of simplified filters and partitions
    */
  def getSimplifiedFilters(filter: Filter, partition: Option[String] = None): Option[Seq[SimplifiedFilter]] = None
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
