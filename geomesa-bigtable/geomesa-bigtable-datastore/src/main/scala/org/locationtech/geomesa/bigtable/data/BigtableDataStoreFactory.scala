/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.bigtable.data

import java.io.Serializable

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.geotools.data.DataAccessFactory.Param
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.HBaseDataStoreConfig
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams._
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{GeoMesaDataStoreInfo, NamespaceParams}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

class BigtableDataStoreFactory extends HBaseDataStoreFactory {

  import BigtableDataStoreFactory.BigtableCatalogParam

  override def getDisplayName: String = BigtableDataStoreFactory.DisplayName
  override def getDescription: String = BigtableDataStoreFactory.Description

  override protected def getCatalog(params: java.util.Map[String, Serializable]): String =
    BigtableCatalogParam.lookup(params)

  override protected def buildDataStore(connection: Connection, config: HBaseDataStoreConfig): BigtableDataStore = {
    // note: bigtable never has push-down predicates
    new BigtableDataStore(connection, config.copy(remoteFilter = false))
  }

  override protected def validateConnection: Boolean = false

  override def canProcess(params: java.util.Map[java.lang.String,java.io.Serializable]): Boolean =
    BigtableDataStoreFactory.canProcess(params)

  override def getParametersInfo: Array[Param] =
    BigtableDataStoreFactory.ParameterInfo :+ BigtableDataStoreFactory.NamespaceParam
}

object BigtableDataStoreFactory extends GeoMesaDataStoreInfo with NamespaceParams {

  val BigtableCatalogParam = new GeoMesaParam[String]("bigtable.catalog", "Catalog table name", optional = false, deprecatedKeys = Seq("bigtable.table.name"))

  override val DisplayName = "Google Bigtable (GeoMesa)"
  override val Description = "Google Bigtable\u2122 distributed key/value store"

  override val ParameterInfo: Array[GeoMesaParam[_]] =
    Array(
      BigtableCatalogParam,
      QueryThreadsParam,
      QueryTimeoutParam,
      GenerateStatsParam,
      AuditQueriesParam,
      LooseBBoxParam,
      CachingParam
    )

  // verify that the hbase-site.xml exists and contains the appropriate bigtable keys
  override def canProcess(params: java.util.Map[String, java.io.Serializable]): Boolean = {
    BigtableCatalogParam.exists(params) &&
        Option(HBaseConfiguration.create().get(HBaseDataStoreFactory.BigTableParamCheck)).exists(_.trim.nonEmpty)
  }
}
