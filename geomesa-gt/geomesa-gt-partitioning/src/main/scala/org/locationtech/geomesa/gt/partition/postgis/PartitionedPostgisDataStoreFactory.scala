/***********************************************************************
 * Copyright (c) 2013-2022 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import org.geotools.data.postgis.{PostGISDialect, PostGISPSDialect, PostgisNGDataStoreFactory}
import org.geotools.jdbc.{JDBCDataStore, SQLDialect}
import org.locationtech.geomesa.gt.partition.postgis.dialect.PartitionedPostgisDialect

class PartitionedPostgisDataStoreFactory extends PostgisNGDataStoreFactory {

  import PartitionedPostgisDataStoreParams.DbType

  override def getDisplayName: String = "PostGIS (partitioned)"

  override def getDescription: String = "PostGIS Database with time-partitioned tables"

  override protected def getDatabaseID: String = DbType.sample.asInstanceOf[String]

  override protected def setupParameters(parameters: java.util.Map[_, _]): Unit = {
    super.setupParameters(parameters)
    // override postgis dbkey
    parameters.asInstanceOf[java.util.Map[AnyRef, AnyRef]].put(DbType.key, DbType)
  }

  override protected def createDataStoreInternal(store: JDBCDataStore, params: java.util.Map[_, _]): JDBCDataStore = {

    val ds = super.createDataStoreInternal(store, params)
    val dialect = new PartitionedPostgisDialect(ds)

    // TODO copy any other configs?
    ds.getSQLDialect match {
      case d: PostGISDialect =>
        dialect.setLooseBBOXEnabled(d.isLooseBBOXEnabled)
        dialect.setEstimatedExtentsEnabled(d.isEstimatedExtentsEnabled)
        dialect.setFunctionEncodingEnabled(d.isFunctionEncodingEnabled)
        dialect.setSimplifyEnabled(d.isSimplifyEnabled)
        dialect.setEncodeBBOXFilterAsEnvelope(d.isEncodeBBOXFilterAsEnvelope)
        ds.setSQLDialect(dialect)

      case d: PostGISPSDialect =>
        dialect.setLooseBBOXEnabled(d.isLooseBBOXEnabled)
        dialect.setFunctionEncodingEnabled(d.isFunctionEncodingEnabled)
        // TODO dialect.setEstimatedExtentsEnabled(d.isEstimatedExtentsEnabled)
        // TODO dialect.setSimplifyEnabled(d.isSimplifyEnabled)
        dialect.setEncodeBBOXFilterAsEnvelope(d.isEncodeBBOXFilterAsEnvelope)
        ds.setSQLDialect(new PostGISPSDialect(ds, dialect))

      case d => throw new IllegalArgumentException(s"Expected PostGISDialect but got: ${d.getClass.getName}")
    }
    ds
  }

  // these will get replaced in createDataStoreInternal, above
  override protected def createSQLDialect(dataStore: JDBCDataStore): SQLDialect = new PostGISDialect(dataStore)
  override protected def createSQLDialect(dataStore: JDBCDataStore, params: java.util.Map[_, _]): SQLDialect =
    new PostGISDialect(dataStore)
}
