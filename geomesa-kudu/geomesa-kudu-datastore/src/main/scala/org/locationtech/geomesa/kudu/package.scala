/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.apache.kudu.client.{AlterTableOptions, KuduSession, Operation, PartialRow}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter._
import org.locationtech.geomesa.index.geotools.{GeoMesaDataStore, GeoMesaFeatureWriter}
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.index.strategies.{AttributeFilterStrategy, IdFilterStrategy, SpatialFilterStrategy, SpatioTemporalFilterStrategy}
import org.locationtech.geomesa.kudu.data.{KuduDataStore, KuduFeature}
import org.locationtech.geomesa.kudu.schema.KuduColumnAdapter
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

package object kudu {

  type KuduDataStoreType = GeoMesaDataStore[KuduDataStore, KuduFeature, WriteOperation]
  type KuduFeatureIndexType = GeoMesaFeatureIndex[KuduDataStore, KuduFeature, WriteOperation]
  type KuduFilterPlanType = FilterPlan[KuduDataStore, KuduFeature, WriteOperation]
  type KuduFilterStrategyType = FilterStrategy[KuduDataStore, KuduFeature, WriteOperation]
  type KuduQueryPlannerType = QueryPlanner[KuduDataStore, KuduFeature, WriteOperation]
  type KuduQueryPlanType = QueryPlan[KuduDataStore, KuduFeature, WriteOperation]
  type KuduIndexManagerType = GeoMesaIndexManager[KuduDataStore, KuduFeature, WriteOperation]
  type KuduFeatureWriterFactoryType = FeatureWriterFactory[KuduDataStore, KuduFeature, WriteOperation]
  type KuduFeatureWriterType = GeoMesaFeatureWriter[KuduDataStore, KuduFeature, WriteOperation, KuduSession]
  type KuduTableFeatureWriterType = TableFeatureWriter[KuduDataStore, KuduFeature, WriteOperation, KuduSession]
  type KuduPartitionedFeatureWriterType = PartitionedFeatureWriter[KuduDataStore, KuduFeature, WriteOperation, KuduSession]
  type KuduAppendFeatureWriterType = GeoMesaAppendFeatureWriter[KuduDataStore, KuduFeature, WriteOperation, KuduSession]
  type KuduModifyFeatureWriterType = GeoMesaModifyFeatureWriter[KuduDataStore, KuduFeature, WriteOperation, KuduSession]
  type KuduSpatioTemporalFilterStrategy = SpatioTemporalFilterStrategy[KuduDataStore, KuduFeature, WriteOperation]
  type KuduSpatialFilterStrategy = SpatialFilterStrategy[KuduDataStore, KuduFeature, WriteOperation]
  type KuduIdFilterStrategy = IdFilterStrategy[KuduDataStore, KuduFeature, WriteOperation]
  type KuduAttributeFilterStrategy = AttributeFilterStrategy[KuduDataStore, KuduFeature, WriteOperation]

  case class KuduValue[T](value: T, adapter: KuduColumnAdapter[T]) {
    def writeToRow(row: PartialRow): Unit = adapter.writeToRow(row, value)
  }

  case class WriteOperation(op: Operation, partition: String, partitioning: () => Option[Partitioning])

  case class Partitioning(table: String, alteration: AlterTableOptions)

  object KuduSystemProperties {
    val AdminOperationTimeout = SystemProperty("geomesa.kudu.admin.timeout")
    val OperationTimeout      = SystemProperty("geomesa.kudu.operation.timeout")
    val SocketReadTimeout     = SystemProperty("geomesa.kudu.socket.timeout")
    val BlockSize             = SystemProperty("geomesa.kudu.block.size")
    val Compression           = SystemProperty("geomesa.kudu.compression", "lz4")
    val MutationBufferSpace   = SystemProperty("geomesa.kudu.mutation.buffer", "10000")
  }
}
