/***********************************************************************
 * Copyright (c) 2017 IBM
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import java.awt.RenderingHints
import java.io.Serializable
import java.util

import com.datastax.driver.core._
import com.datastax.driver.core.policies.{DCAwareRoundRobinPolicy, DefaultRetryPolicy, TokenAwarePolicy}
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.CassandraDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreConfig
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

class CassandraDataStoreFactory extends DataStoreFactorySpi {
  import CassandraDataStoreFactory.Params._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def createDataStore(params: util.Map[String, Serializable]): DataStore = {
    import org.locationtech.geomesa.cassandra.CassandraSystemProperties.{ConnectionTimeoutMillis, ReadTimeoutMillis}

    val Array(cp, port) = ContactPointParam.lookup(params).split(":")
    val ks = KeySpaceParam.lookup(params)
    val generateStats = GenerateStatsParam.lookup(params)
    val audit = if (AuditQueriesParam.lookup(params)) {
      Some(AuditLogger, Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider), "cassandra")
    } else {
      None
    }
    val caching = CachingParam.lookup(params)

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

    val user = UserNameParam.lookup(params)
    val password = PasswordParam.lookup(params)
    if (user != null && password != null) {
      clusterBuilder.withCredentials(user, password)
    }

    val cluster = clusterBuilder.build()
    val session = cluster.connect(ks)
    val catalog = CatalogParam.lookup(params)

    // not used but required for config inheritance
    val queryThreads = QueryThreadsParam.lookup(params)
    val queryTimeout = QueryTimeoutParam.lookupOpt(params).map(_ * 1000L)
    val looseBBox = LooseBBoxParam.lookup(params)

    val ns = Option(NamespaceParam.lookUp(params).asInstanceOf[String])

    val cfg = CassandraDataStoreConfig(catalog, generateStats, audit, caching, queryThreads, queryTimeout, looseBBox, ns)

    new CassandraDataStore(session, cfg)
  }

  override def getDisplayName: String = CassandraDataStoreFactory.DisplayName

  override def getDescription: String = CassandraDataStoreFactory.Description

  override def getParametersInfo: Array[Param] = Array(ContactPointParam, KeySpaceParam, CatalogParam,
    UserNameParam, PasswordParam, GenerateStatsParam, AuditQueriesParam, LooseBBoxParam, CachingParam,
    QueryThreadsParam, QueryTimeoutParam, NamespaceParam)

  override def canProcess(params: java.util.Map[String,Serializable]): Boolean = KeySpaceParam.exists(params)

  override def isAvailable = true

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null

}

object CassandraDataStoreFactory {

  val DisplayName = "Cassandra (GeoMesa)"
  val Description = "Apache Cassandra\u2122 distributed key/value store"

  // noinspection TypeAnnotation
  object Params {
    val ContactPointParam  = new GeoMesaParam[String]("cassandra.contact.point", "HOST:PORT to Cassandra", required = true, deprecated = Seq("geomesa.cassandra.contact.point"))
    val KeySpaceParam      = new GeoMesaParam[String]("cassandra.keyspace", "Cassandra Keyspace", required = true, deprecated = Seq("geomesa.cassandra.keyspace"))
    val CatalogParam       = new GeoMesaParam[String]("cassandra.catalog", "Name of GeoMesa catalog table", required = true, deprecated = Seq("geomesa.cassandra.catalog.table"))
    val UserNameParam      = new GeoMesaParam[String]("cassandra.username", "Username to connect with", deprecated = Seq("geomesa.cassandra.username"))
    val PasswordParam      = new GeoMesaParam[String]("cassandra.password", "Password to connect with", metadata = Map(Parameter.IS_PASSWORD -> java.lang.Boolean.TRUE), deprecated = Seq("geomesa.cassandra.password"))
    val GenerateStatsParam = GeoMesaDataStoreFactory.GenerateStatsParam
    val AuditQueriesParam  = GeoMesaDataStoreFactory.AuditQueriesParam
    val CachingParam       = GeoMesaDataStoreFactory.CachingParam
    val LooseBBoxParam     = GeoMesaDataStoreFactory.LooseBBoxParam
    val QueryThreadsParam  = GeoMesaDataStoreFactory.QueryThreadsParam
    val QueryTimeoutParam  = GeoMesaDataStoreFactory.QueryTimeoutParam
    val NamespaceParam     = GeoMesaDataStoreFactory.NamespaceParam
  }

  case class CassandraDataStoreConfig(catalog: String,
                                  generateStats: Boolean,
                                  audit: Option[(AuditWriter, AuditProvider, String)],
                                  caching: Boolean,
                                  queryThreads: Int,
                                  queryTimeout: Option[Long],
                                  looseBBox: Boolean,
                                  namespace: Option[String]) extends GeoMesaDataStoreConfig
}
