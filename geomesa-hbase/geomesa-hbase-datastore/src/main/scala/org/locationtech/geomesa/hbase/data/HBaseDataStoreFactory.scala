/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.io.Serializable

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.HBaseDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider}


class HBaseDataStoreFactory extends DataStoreFactorySpi {

  import HBaseDataStoreFactory.Params._

  // TODO: investigate multiple HBase connections per jvm
  private lazy val globalConnection = {
    val ret = ConnectionFactory.createConnection(HBaseConfiguration.create())
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        ret.close()
      }
    })
    ret
  }

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: java.util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
    import GeoMesaDataStoreFactory.RichParam

    val connection = ConnectionParam.lookupOpt[Connection](params).getOrElse(globalConnection)

    val catalog = BigTableNameParam.lookup[String](params)

    val generateStats = GenerateStatsParam.lookupWithDefault[Boolean](params)
    val audit = if (AuditQueriesParam.lookupWithDefault[Boolean](params)) {
      Some(AuditLogger, Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider), "hbase")
    } else {
      None
    }
    val queryThreads = QueryThreadsParam.lookupWithDefault[Int](params)
    val queryTimeout = GeoMesaDataStoreFactory.queryTimeout(params)
    val looseBBox = LooseBBoxParam.lookupWithDefault[Boolean](params)
    val caching = CachingParam.lookupWithDefault[Boolean](params)
    val config = HBaseDataStoreConfig(catalog, generateStats, audit, queryThreads, queryTimeout, looseBBox, caching)

    new HBaseDataStore(connection, config)
  }

  override def getDisplayName: String = HBaseDataStoreFactory.DisplayName

  override def getDescription: String = HBaseDataStoreFactory.Description

  override def getParametersInfo: Array[Param] =
    Array(BigTableNameParam, QueryThreadsParam, QueryTimeoutParam, GenerateStatsParam,
      AuditQueriesParam, LooseBBoxParam, CachingParam)

  override def canProcess(params: java.util.Map[String,Serializable]): Boolean = HBaseDataStoreFactory.canProcess(params)

  override def isAvailable = true

  override def getImplementationHints = null
}

object HBaseDataStoreFactory {

  val DisplayName = "HBase (GeoMesa)"
  val Description = "Apache HBase\u2122 distributed key/value store"

  object Params {
    val BigTableNameParam  = new Param("bigtable.table.name", classOf[String], "Table name", true)
    val ConnectionParam    = new Param("connection", classOf[Connection], "Connection", false)
    val LooseBBoxParam     = GeoMesaDataStoreFactory.LooseBBoxParam
    val QueryThreadsParam  = GeoMesaDataStoreFactory.QueryThreadsParam
    val GenerateStatsParam = GeoMesaDataStoreFactory.GenerateStatsParam
    val AuditQueriesParam  = GeoMesaDataStoreFactory.AuditQueriesParam
    val QueryTimeoutParam  = GeoMesaDataStoreFactory.QueryTimeoutParam
    val CachingParam       = GeoMesaDataStoreFactory.CachingParam
  }

  case class HBaseDataStoreConfig(catalog: String,
                                  generateStats: Boolean,
                                  audit: Option[(AuditWriter, AuditProvider, String)],
                                  queryThreads: Int,
                                  queryTimeout: Option[Long],
                                  looseBBox: Boolean,
                                  caching: Boolean) extends GeoMesaDataStoreConfig

  def canProcess(params: java.util.Map[String,Serializable]): Boolean =
    params.containsKey(Params.BigTableNameParam.key)
}
