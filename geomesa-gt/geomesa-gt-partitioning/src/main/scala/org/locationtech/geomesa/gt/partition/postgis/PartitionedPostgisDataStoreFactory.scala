/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.dbcp.BasicDataSource
import org.geotools.data.postgis.{PostGISDialect, PostGISPSDialect, PostgisNGDataStoreFactory}
import org.geotools.jdbc.{JDBCDataStore, SQLDialect}
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect}

class PartitionedPostgisDataStoreFactory extends PostgisNGDataStoreFactory with LazyLogging {

  import PartitionedPostgisDataStoreParams.{DbType, IdleInTransactionTimeout}

  override def getDisplayName: String = "PostGIS (partitioned)"

  override def getDescription: String = "PostGIS Database with time-partitioned tables"

  override protected def getDatabaseID: String = DbType.sample.asInstanceOf[String]

  override protected def setupParameters(parameters: java.util.Map[String, AnyRef]): Unit = {
    super.setupParameters(parameters)
    Seq(DbType, IdleInTransactionTimeout)
        .foreach(p => parameters.put(p.key, p))
  }

  override def createDataSource(params: java.util.Map[String, _]): BasicDataSource = {
    val source = super.createDataSource(params)
    val options =
      Seq(IdleInTransactionTimeout)
          .flatMap(p => p.opt(params).map(t => s"-c ${p.key}=${t.millis}"))

    logger.debug(s"Connection options: ${options.mkString(" ")}")

    if (options.nonEmpty) {
      source.addConnectionProperty("options", options.mkString(" "))
    }
    source
  }

  override protected def createDataStoreInternal(store: JDBCDataStore, baseParams: java.util.Map[String, _]): JDBCDataStore = {
    val params = new java.util.HashMap[String, Any](baseParams)
    // set default schema, if not specified - postgis store doesn't actually use its own default
    if (!params.containsKey(PostgisNGDataStoreFactory.SCHEMA.key)) {
      // need to set it in the store, as the key has already been processed
      store.setDatabaseSchema("public")
      // also set in the params for consistency, although it's not used anywhere
      params.put(PostgisNGDataStoreFactory.SCHEMA.key, "public")
    }
    val ds = super.createDataStoreInternal(store, params)
    val dialect = new PartitionedPostgisDialect(ds)

    ds.getSQLDialect match {
      case d: PostGISDialect =>
        dialect.setEncodeBBOXFilterAsEnvelope(d.isEncodeBBOXFilterAsEnvelope)
        dialect.setEstimatedExtentsEnabled(d.isEstimatedExtentsEnabled)
        dialect.setFunctionEncodingEnabled(d.isFunctionEncodingEnabled)
        dialect.setLooseBBOXEnabled(d.isLooseBBOXEnabled)
        dialect.setSimplifyEnabled(d.isSimplifyEnabled)
        ds.setSQLDialect(dialect)

      case d: PostGISPSDialect =>
        dialect.setEncodeBBOXFilterAsEnvelope(d.isEncodeBBOXFilterAsEnvelope)
        dialect.setFunctionEncodingEnabled(d.isFunctionEncodingEnabled)
        dialect.setLooseBBOXEnabled(d.isLooseBBOXEnabled)

        // these configs aren't exposed through the PS dialect so re-calculate them from the params
        val est = PostgisNGDataStoreFactory.ESTIMATED_EXTENTS.lookUp(params)
        dialect.setEstimatedExtentsEnabled(est == null || est == java.lang.Boolean.TRUE)
        val simplify = PostgisNGDataStoreFactory.SIMPLIFY.lookUp(params)
        dialect.setSimplifyEnabled(simplify == null || simplify == java.lang.Boolean.TRUE)

        ds.setSQLDialect(new PartitionedPostgisPsDialect(ds, dialect))

      case d => throw new IllegalArgumentException(s"Expected PostGISDialect but got: ${d.getClass.getName}")
    }

    ds
  }

  // these will get replaced in createDataStoreInternal, above
  override protected def createSQLDialect(dataStore: JDBCDataStore): SQLDialect = new PostGISDialect(dataStore)
  override protected def createSQLDialect(dataStore: JDBCDataStore, params: java.util.Map[String, _]): SQLDialect =
    new PostGISDialect(dataStore)
}
