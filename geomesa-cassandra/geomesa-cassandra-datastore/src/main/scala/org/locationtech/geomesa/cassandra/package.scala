/***********************************************************************
* Copyright (c) 2016  IBM
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa

import org.locationtech.geomesa.cassandra.data.{CassandraDataStore, CassandraFeature}
import org.locationtech.geomesa.index.api._
import org.locationtech.geomesa.index.geotools.{GeoMesaAppendFeatureWriter, GeoMesaDataStore, GeoMesaFeatureWriter, GeoMesaModifyFeatureWriter}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import com.datastax.driver.core._

package object cassandra {
  type CassandraDataStoreType = GeoMesaDataStore[CassandraDataStore, CassandraFeature, Statement]
  type CassandraFeatureIndexType = GeoMesaFeatureIndex[CassandraDataStore, CassandraFeature, Statement]
  type CassandraFilterPlanType = FilterPlan[CassandraDataStore, CassandraFeature, Statement]
  type CassandraFilterStrategyType = FilterStrategy[CassandraDataStore, CassandraFeature, Statement]
  type CassandraQueryPlannerType = QueryPlanner[CassandraDataStore, CassandraFeature, Statement]
  type CassandraQueryPlanType = QueryPlan[CassandraDataStore, CassandraFeature, Statement]
  type CassandraIndexManagerType = GeoMesaIndexManager[CassandraDataStore, CassandraFeature, Statement]
  type CassandraFeatureWriterType = GeoMesaFeatureWriter[CassandraDataStore, CassandraFeature, Statement, Any]
  type CassandraAppendFeatureWriterType = GeoMesaAppendFeatureWriter[CassandraDataStore, CassandraFeature, Statement, Any]
  type CassandraModifyFeatureWriterType = GeoMesaModifyFeatureWriter[CassandraDataStore, CassandraFeature, Statement, Any]
}
