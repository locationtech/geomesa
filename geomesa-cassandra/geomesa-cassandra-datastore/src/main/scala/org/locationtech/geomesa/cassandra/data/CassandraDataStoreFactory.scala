/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.io.Serializable
import java.net.URI
import java.util

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, DefaultRetryPolicy, TokenAwarePolicy}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi, Parameter}
import org.geotools.util.KVP
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.CassandraDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider}

class CassandraDataStoreFactory extends DataStoreFactorySpi {
  import CassandraDataStoreFactory.Params._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: util.Map[String, Serializable]) = createDataStore(params)

  override def createDataStore(params: util.Map[String, Serializable]): DataStore = {
    import GeoMesaDataStoreFactory.RichParam

    val Array(cp, port) = CPParam.lookUp(params).asInstanceOf[String].split(":")
    val ks = KSParam.lookUp(params).asInstanceOf[String]
    val ns = NSParam.lookUp(params).asInstanceOf[URI]
    val generateStats = GenerateStatsParam.lookupWithDefault[Boolean](params)
    val audit = if (AuditQueriesParam.lookupWithDefault[Boolean](params)) {
          Some(AuditLogger, Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider), "cassandra")
        } else {
          None
        }
    val caching = CachingParam.lookupWithDefault[Boolean](params)
    val cluster =
      Cluster.builder()
        .addContactPoint(cp)
        .withPort(port.toInt)
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
        .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
        .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
        .build()

    val session = cluster.connect(ks)
    val catalog = CatalogParam.lookUp(params).asInstanceOf[String]

    // not used but required for config inheritance
    val queryThreads = QueryThreadsParam.lookupWithDefault[Int](params)
    val queryTimeout = GeoMesaDataStoreFactory.queryTimeout(params)
    val looseBBox = LooseBBoxParam.lookupWithDefault[Boolean](params)

    val cfg = CassandraDataStoreConfig(catalog, generateStats, audit, caching, queryThreads, queryTimeout, looseBBox)

    new CassandraDataStore(session, cfg)
  }

  override def getDisplayName: String = "Cassandra (GeoMesa)"

  override def getDescription: String = "GeoMesa Cassandra Data Store"

  override def getParametersInfo: Array[Param] = Array(CPParam, KSParam, NSParam, CatalogParam, GenerateStatsParam, AuditQueriesParam, CachingParam, LooseBBoxParam, QueryThreadsParam, QueryTimeoutParam)

  override def canProcess(params: java.util.Map[String,Serializable]) = params.containsKey(KSParam.key)

  override def isAvailable = true

  override def getImplementationHints = null

}

object CassandraDataStoreFactory {

  val DisplayName = "Cassandra (GeoMesa)"
  val Description = "Apache Cassandra\u2122 distributed key/value store"

  object Params {
    val CPParam            = new Param("geomesa.cassandra.contact.point"  , classOf[String], "HOST:PORT to Cassandra",   true)
    val KSParam            = new Param("geomesa.cassandra.keyspace"       , classOf[String], "Cassandra Keyspace", true)
    val NSParam            = new Param("geomesa.cassandra.namespace", classOf[URI], "uri to a the namespace", false, null, new KVP(Parameter.LEVEL, "advanced"))
    val CatalogParam       = new Param("geomesa.cassandra.catalog.table", classOf[String], "Name of GeoMesa catalog table", true)
    val GenerateStatsParam = GeoMesaDataStoreFactory.GenerateStatsParam
    val AuditQueriesParam  = GeoMesaDataStoreFactory.AuditQueriesParam
    val CachingParam       = GeoMesaDataStoreFactory.CachingParam
    val LooseBBoxParam     = GeoMesaDataStoreFactory.LooseBBoxParam
    val QueryThreadsParam  = GeoMesaDataStoreFactory.QueryThreadsParam
    val QueryTimeoutParam  = GeoMesaDataStoreFactory.QueryTimeoutParam
  }

  case class CassandraDataStoreConfig(catalog: String,
                                  generateStats: Boolean,
                                  audit: Option[(AuditWriter, AuditProvider, String)],
                                  caching: Boolean,
                                  queryThreads: Int,
                                  queryTimeout: Option[Long],
                                  looseBBox: Boolean) extends GeoMesaDataStoreConfig
}
