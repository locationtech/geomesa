/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.bigtable.data

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.geotools.data.DataAccessFactory.Param
import org.locationtech.geomesa.bigtable.index.BigtableFeatureIndex
import org.locationtech.geomesa.hbase.HBaseIndexManagerType
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.HBaseDataStoreConfig
import org.locationtech.geomesa.hbase.data._

class BigtableDataStore(connection: Connection, config: HBaseDataStoreConfig)
    extends HBaseDataStore(connection, config) {
  override def manager: HBaseIndexManagerType = BigtableFeatureIndex
}

class BigtableDataStoreFactory extends HBaseDataStoreFactory {

  override def getDisplayName: String = BigtableDataStoreFactory.DisplayName
  override def getDescription: String = BigtableDataStoreFactory.Description

  override def buildDataStore(connection: Connection, config: HBaseDataStoreConfig): BigtableDataStore = {
    // note: bigtable never has push-down predicates
    new BigtableDataStore(connection, config.copy(remoteFilter = false))
  }

  override def canProcess(params: java.util.Map[java.lang.String,java.io.Serializable]): Boolean =
    BigtableDataStoreFactory.canProcess(params)

  override def getParametersInfo: Array[Param] =
    super.getParametersInfo.filterNot(_.eq(HBaseDataStoreParams.RemoteFiltersParam))
}

object BigtableDataStoreFactory {
  val DisplayName = "Google Bigtable (GeoMesa)"
  val Description = "Google Bigtable\u2122 distributed key/value store"

  // verify that the hbase-site.xml exists and contains the appropriate bigtable keys
  def canProcess(params: java.util.Map[java.lang.String,java.io.Serializable]): Boolean = {
    params.containsKey(HBaseDataStoreParams.BigTableNameParam.key) &&
      Option(HBaseConfiguration.create().get(HBaseDataStoreFactory.BigTableParamCheck)).exists(_.trim.nonEmpty)
  }
}
