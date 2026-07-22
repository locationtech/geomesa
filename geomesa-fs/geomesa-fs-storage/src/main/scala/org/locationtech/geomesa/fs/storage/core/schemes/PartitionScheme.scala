/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.schemes

import org.apache.iceberg.expressions.Expression
import org.apache.iceberg.{PartitionSpec, StructLike}
import org.geotools.api.feature.simple.SimpleFeature
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.core.schema.ColumnName
import org.locationtech.geomesa.fs.storage.core.{Partition, PartitionKey}

import java.time.ZonedDateTime

/**
 * Scheme for partitioning features into various named partitions (e.g. buckets) on disk, for
 * faster querying. Partition schemes do not have any persistent state, they only know how to map features
 * and filters to partition names.
 *
 * Note that generally partitioning is implemented to align with iceberg's partitioning logic, so
 * that partitions in iceberg have a 1:1 mapping with partitions in geomesa (the reverse is not true,
 * as geomesa supports more partition schemes than iceberg, e.g. spatial partitions)
 */
trait PartitionScheme {

  /**
   * Name of this partition scheme
   *
   * @return
   */
  def name: String

  /**
   * Name of the attribute used for partitioning
   *
   * @return
   */
  def attribute: String

  /**
   * Name of the column used for expressions
   *
   * @return
   */
  def column: String

  /**
    * Return the partition in which a SimpleFeature should be stored
    *
    * @param feature simple feature
    * @return partition name
    */
  def getPartition(feature: SimpleFeature): PartitionKey

  /**
   * Gets the geomesa partition value for a given iceberg partition value
   *
   * @param partition iceberg partition struct
   * @param i offest into the partition struct
   * @return geomesa partition value
   */
  def getPartition(partition: StructLike, i: Int): PartitionKey

  /**
   * Get a filter that will cover a partitions, i.e. the filter will return all features
   * in the given partition and none from other partitions
   *
   * @param partition partition to cover
   * @return filter
   */
  def getCoveringFilter(partition: PartitionKey): Filter

  /**
   * Get a filter that will cover a partitions, i.e. the filter will return all features
   * in the given partition and none from other partitions
   *
   * @param partition partition to cover
   * @return filter
   */
  def getCoveringExpression(partition: PartitionKey): Expression

  /**
   * Creates the iceberg partition spec
   *
   * @param b spec builder
   * @return
   */
  def spec(b: PartitionSpec.Builder): PartitionSpec.Builder
}

object PartitionScheme {

  /**
   * Enumerate all possible partitions for a given date. Note that not all partition schemes support enumeration
   *
   * @param schemes partition schemes
   * @param date date
   * @return all possible partitions for the given date
   * @throws UnsupportedOperationException if the partition scheme can't be enumerated
   */
  def enumerate(schemes: Seq[PartitionScheme], date: ZonedDateTime): Seq[Partition] = {
    val keys = schemes.map {
      case s: TemporalScheme => Seq(s.partition(date).partition)
      case s: EnumeratedScheme => s.partitions
      case s => throw new UnsupportedOperationException(s"The partition scheme '${s.name}' does not support enumerating partitions")
    }
    keys.foldLeft(Seq(Partition.None)) { case (partitions, keys) =>
      for { partition <- partitions; key <- keys } yield {
        Partition(partition.values :+ key)
      }
    }
  }

  /**
   * A scheme with a fixed set of partitions
   */
  trait EnumeratedScheme extends PartitionScheme {

    /**
     * Get all potential partitions for this scheme
     *
     * @return
     */
    def partitions: Seq[PartitionKey]
  }

  /**
   * A schemed with a fixed set of partitions per time period
   */
  trait TemporalScheme extends PartitionScheme {

    /**
     * Gets the partitions for this scheme for the given time
     *
     * @param time date to get partitions for
     * @return
     */
    def partition(time: ZonedDateTime): TemporalPartition
  }

  /**
   * A partition, with the associated start and end period
   *
   * @param partition partition
   * @param start start of partitions (inclusive)
   * @param end end of partitions (exclusive)
   */
  case class TemporalPartition(partition: PartitionKey, start: ZonedDateTime, end: ZonedDateTime)
}
