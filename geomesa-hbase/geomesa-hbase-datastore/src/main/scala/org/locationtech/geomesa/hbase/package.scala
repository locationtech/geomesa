/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa

import org.apache.hadoop.hbase.client.{BufferedMutator, Mutation, Result}
import org.locationtech.geomesa.hbase.data.{HBaseDataStore, HBaseFeature}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.{GeoMesaAppendFeatureWriter, GeoMesaDataStore, GeoMesaFeatureWriter, GeoMesaModifyFeatureWriter}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

package object hbase {
  type HBaseDataStoreType = GeoMesaDataStore[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseFeatureIndexType = GeoMesaFeatureIndex[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseFilterPlanType = FilterPlan[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseFilterStrategyType = FilterStrategy[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseQueryPlannerType = QueryPlanner[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseQueryPlanType = QueryPlan[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseIndexManagerType = GeoMesaIndexManager[HBaseDataStore, HBaseFeature, Mutation]
  type HBaseFeatureWriterType = GeoMesaFeatureWriter[HBaseDataStore, HBaseFeature, Mutation, BufferedMutator]
  type HBaseAppendFeatureWriterType = GeoMesaAppendFeatureWriter[HBaseDataStore, HBaseFeature, Mutation, BufferedMutator]
  type HBaseModifyFeatureWriterType = GeoMesaModifyFeatureWriter[HBaseDataStore, HBaseFeature, Mutation, BufferedMutator]

  object HBaseSystemProperties {
    val WriteBatchSize = SystemProperty("geomesa.hbase.write.batch")
  }
}
