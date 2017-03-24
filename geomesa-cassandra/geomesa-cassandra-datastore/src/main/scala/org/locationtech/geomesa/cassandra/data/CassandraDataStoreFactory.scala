/***********************************************************************
 * Copyright (c) 2017 IBM
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.io.Serializable
import java.util
import java.util.Collections

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, DefaultRetryPolicy, TokenAwarePolicy}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.CassandraDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider}

class CassandraDataStoreFactory extends DataStoreFactorySpi {
  import CassandraDataStoreFactory.Params._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def createDataStore(params: util.Map[String, Serializable]): DataStore = {
    import GeoMesaDataStoreFactory.RichParam
    import org.locationtech.geomesa.cassandra.CassandraSystemProperties.{ConnectionTimeoutMillis, ReadTimeoutMillis}

    val Array(cp, port) = ContactPointParam.lookUp(params).asInstanceOf[String].split(":")
    val ks = KeySpaceParam.lookUp(params).asInstanceOf[String]
    val generateStats = GenerateStatsParam.lookupWithDefault[Boolean](params)
    val audit = if (AuditQueriesParam.lookupWithDefault[Boolean](params)) {
      Some(AuditLogger, Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider), "cassandra")
    } else {
      None
    }
    val caching = CachingParam.lookupWithDefault[Boolean](params)

    val clusterBuilder =
      Cluster.builder()
        .addContactPoint(cp)
        .withPort(port.toInt)
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
        .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
        .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))

    val socketOptions = (ReadTimeoutMillis.option, ConnectionTimeoutMillis.option) match {
      case (Some(r), Some(c)) => Some(new SocketOptions().setConnectTimeoutMillis(c.toInt).setReadTimeoutMillis(r.toInt))
      case (Some(r), None) => Some(new SocketOptions().setReadTimeoutMillis(r.toInt))
      case (None, Some(c)) => Some(new SocketOptions().setConnectTimeoutMillis(c.toInt))
      case _ => None
    }
    socketOptions.foreach(clusterBuilder.withSocketOptions)

    val user = UserNameParam.lookUp(params).asInstanceOf[String]
    val password = PasswordParam.lookUp(params).asInstanceOf[String]
    if (user != null && password != null) {
      clusterBuilder.withCredentials(user, password)
    }

    val cluster = clusterBuilder.build()
    val session = cluster.connect(ks)
    val catalog = CatalogParam.lookUp(params).asInstanceOf[String]

    // not used but required for config inheritance
    val queryThreads = QueryThreadsParam.lookupWithDefault[Int](params)
    val queryTimeout = GeoMesaDataStoreFactory.queryTimeout(params)
    val looseBBox = LooseBBoxParam.lookupWithDefault[Boolean](params)

    val cfg = CassandraDataStoreConfig(catalog, generateStats, audit, caching, queryThreads, queryTimeout, looseBBox)

    new CassandraDataStore(session, cfg)
  }

  override def getDisplayName: String = CassandraDataStoreFactory.DisplayName

  override def getDescription: String = CassandraDataStoreFactory.Description

  override def getParametersInfo: Array[Param] = Array(ContactPointParam, KeySpaceParam, CatalogParam,
    UserNameParam, PasswordParam, GenerateStatsParam, AuditQueriesParam, LooseBBoxParam, CachingParam,
    QueryThreadsParam, QueryTimeoutParam)

  override def canProcess(params: java.util.Map[String,Serializable]): Boolean = params.containsKey(KeySpaceParam.key)

  override def isAvailable = true

  override def getImplementationHints = null

}

object CassandraDataStoreFactory {

  val DisplayName = "Cassandra (GeoMesa)"
  val Description = "Apache Cassandra\u2122 distributed key/value store"

  object Params {
    val ContactPointParam  = new Param("geomesa.cassandra.contact.point", classOf[String], "HOST:PORT to Cassandra",   true)
    val KeySpaceParam      = new Param("geomesa.cassandra.keyspace",      classOf[String], "Cassandra Keyspace", true)
    val CatalogParam       = new Param("geomesa.cassandra.catalog.table", classOf[String], "Name of GeoMesa catalog table", true)
    val UserNameParam      = new Param("geomesa.cassandra.username", classOf[String], "Username to connect with", false)
    val PasswordParam      = new Param("geomesa.cassandra.password", classOf[String], "Password to connect with", false, null, Collections.singletonMap(Parameter.IS_PASSWORD, java.lang.Boolean.TRUE))
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
