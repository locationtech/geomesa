/***********************************************************************
* Copyright (c) 2016  IBM
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa

import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature, CassandraRow}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.{GeoMesaAppendFeatureWriter, GeoMesaDataStore, GeoMesaFeatureWriter, GeoMesaModifyFeatureWriter}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import com.datastax.driver.core._

package object cassandra {
  type CassandraDataStoreType = GeoMesaDataStore[CassandraDataStore, CassandraFeature, CassandraRow]
  type CassandraFeatureIndexType = GeoMesaFeatureIndex[CassandraDataStore, CassandraFeature, CassandraRow]
  type CassandraFilterPlanType = FilterPlan[CassandraDataStore, CassandraFeature, CassandraRow]
  type CassandraFilterStrategyType = FilterStrategy[CassandraDataStore, CassandraFeature, CassandraRow]
  type CassandraQueryPlannerType = QueryPlanner[CassandraDataStore, CassandraFeature, CassandraRow]
  type CassandraQueryPlanType = QueryPlan[CassandraDataStore, CassandraFeature, CassandraRow]
  type CassandraIndexManagerType = GeoMesaIndexManager[CassandraDataStore, CassandraFeature, CassandraRow]
  type CassandraFeatureWriterType = GeoMesaFeatureWriter[CassandraDataStore, CassandraFeature, CassandraRow, String]
  type CassandraAppendFeatureWriterType = GeoMesaAppendFeatureWriter[CassandraDataStore, CassandraFeature, CassandraRow, String]
  type CassandraModifyFeatureWriterType = GeoMesaModifyFeatureWriter[CassandraDataStore, CassandraFeature, CassandraRow, String]
}
