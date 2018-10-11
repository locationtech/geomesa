/***********************************************************************
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.apache.hadoop.hbase.client.{BufferedMutator, Mutation}
import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseFeature}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.GeoMesaFeatureWriter._
import org.locationtech.geomesa.index.geotools.{GeoMesaDataStore, GeoMesaFeatureWriter}
import org.locationtech.geomesa.index.planning.QueryPlanner
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

package object hbase {
  type HBaseDataStoreType = GeoMesaDataStore[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseFeatureIndexType = GeoMesaFeatureIndex[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseFilterPlanType = FilterPlan[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseFilterStrategyType = FilterStrategy[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseQueryPlannerType = QueryPlanner[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseQueryPlanType = QueryPlan[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseIndexManagerType = GeoMesaIndexManager[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseFeatureWriterFactoryType = FeatureWriterFactory[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseFeatureWriterType = GeoMesaFeatureWriter[HBaseDataStore, HBaseFeature, Mutation, BufferedMutator]
  type HBaseTableFeatureWriterType = TableFeatureWriter[HBaseDataStore, HBaseFeature, Mutation, BufferedMutator]
  type HBasePartitionedFeatureWriterType = PartitionedFeatureWriter[HBaseDataStore, HBaseFeature, Mutation, BufferedMutator]
  type HBaseAppendFeatureWriterType = GeoMesaAppendFeatureWriter[HBaseDataStore, HBaseFeature, Mutation, BufferedMutator]
  type HBaseModifyFeatureWriterType = GeoMesaModifyFeatureWriter[HBaseDataStore, HBaseFeature, Mutation, BufferedMutator]

  object HBaseSystemProperties {
    val CoprocessorPath = SystemProperty("geomesa.hbase.coprocessor.path")
    val WriteBatchSize = SystemProperty("geomesa.hbase.write.batch")
    val WalDurability = SystemProperty("geomesa.hbase.wal.durability")
    val ScannerCaching = SystemProperty("geomesa.hbase.client.scanner.caching.size")
    val ScannerBlockCaching = SystemProperty("geomesa.hbase.query.block.caching.enabled", "true")
    val TableAvailabilityTimeout = SystemProperty("geomesa.hbase.table.availability.timeout", "30 minutes")
  }
}
