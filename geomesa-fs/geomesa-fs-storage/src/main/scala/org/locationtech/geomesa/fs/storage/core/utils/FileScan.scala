/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.fs.storage.core.utils

import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.{DataFile, Table, TableScan}
import org.geotools.api.feature.simple.SimpleFeatureType
import org.geotools.api.filter.Filter
import org.locationtech.geomesa.fs.storage.core.Partition
import org.locationtech.geomesa.fs.storage.core.iceberg.IcebergFilterConverter
import org.locationtech.geomesa.fs.storage.core.schemes.PartitionScheme
import org.locationtech.geomesa.utils.io.WithClose

class FileScan(protected val tableScan: TableScan) {

  import scala.collection.JavaConverters._

  def scan(): Seq[DataFile] = WithClose(tableScan.planFiles())(_.asScala.map(_.file()).toSeq)
}

object FileScan {

  type FluentScan = FileScan with RetrieveMetadata[PartitionScan[FileScan] with FilterScan[FileScan]] with PartitionScan[RetrieveMetadata[FileScan]] with FilterScan[RetrieveMetadata[FileScan]]

  trait RetrieveMetadata[T <: FileScan] extends FileScan {
    protected def tableScan: TableScan
    protected def metadataStep(scan: TableScan): T

    def withMetadata(): T = metadataStep(tableScan.includeColumnStats())
  }

  trait PartitionScan[T <: FileScan] extends FileScan {

    protected def tableScan: TableScan
    protected def schemes: Seq[PartitionScheme]
    protected def filterStep(scan: TableScan): T

    def ofPartition(partition: Partition): T = {
      val filters = schemes.zip(partition.values).map { case (s, p) => s.getCoveringExpression(p) }
      filterStep(tableScan.filter(filters.reduce(Expressions.and)))
    }
  }

  trait FilterScan[T <: FileScan] extends FileScan {

    protected def tableScan: TableScan
    protected def sft: SimpleFeatureType
    protected def filterStep(scan: TableScan): T

    def filter(filter: Filter): FileScan = filterStep(tableScan.filter(IcebergFilterConverter(sft, filter).expression))
  }

  /**
   * Create a new fluent file scan builder
   *
   * @param table table
   * @param sft simple feature type
   * @param schemes partition schemes
   * @return
   */
  def apply(table: Table, sft: SimpleFeatureType, schemes: Seq[PartitionScheme]): FluentScan =
    new InitialScan(table.newScan(), sft, schemes)

  private class InitialScan(scan: TableScan, protected val sft: SimpleFeatureType, protected val schemes: Seq[PartitionScheme])
      extends FileScan(scan)
        with RetrieveMetadata[PartitionScan[FileScan] with FilterScan[FileScan]]
        with PartitionScan[RetrieveMetadata[FileScan]]
        with FilterScan[RetrieveMetadata[FileScan]] {
    override protected def metadataStep(scan: TableScan): FilterableScan = new FilterableScan(scan, sft, schemes)
    override protected def filterStep(scan: TableScan): OptionalMetadataScan = new OptionalMetadataScan(scan)
  }

  private class FilterableScan(scan: TableScan, protected val sft: SimpleFeatureType, protected val schemes: Seq[PartitionScheme])
      extends FileScan(scan) with PartitionScan[FileScan] with FilterScan[FileScan] {
    override protected def filterStep(scan: TableScan): FileScan = new FileScan(scan)
  }

  private class OptionalMetadataScan(scan: TableScan) extends FileScan(scan) with RetrieveMetadata[FileScan] {
    override protected def metadataStep(scan: TableScan): FileScan = new FileScan(scan)
  }
}
