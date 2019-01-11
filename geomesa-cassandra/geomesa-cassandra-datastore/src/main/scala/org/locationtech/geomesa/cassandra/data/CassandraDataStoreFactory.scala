/***********************************************************************
 * Copyright (c) 2017-2019 IBM
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import com.google.common.collect.ImmutableMap
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.CassandraDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{GeoMesaDataStoreConfig, GeoMesaDataStoreInfo, GeoMesaDataStoreParams}
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider}
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

import scala.util.control.NonFatal

class CassandraDataStoreFactory extends DataStoreFactorySpi {

  import CassandraDataStoreFactory.Params._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def createDataStore(params: util.Map[String, Serializable]): DataStore = {
    import org.locationtech.geomesa.cassandra.CassandraSystemProperties.{ConnectionTimeoutMillis, ReadTimeoutMillis}

    val (cp, portString) = ContactPointParam.lookup(params).split(":") match {
      case Array(one, two) => (one, two)
      case parts => throw new IllegalArgumentException(s"Invalid parameter '${ContactPointParam.key}', " +
          s"expected '<host>:<port>' but got '${parts.mkString(":")}'")
    }
    val port = try { portString.toInt } catch {
      case NonFatal(e) => throw new IllegalArgumentException(s"Invalid parameter '${ContactPointParam.key}', " +
          s"expected '<host>:<port>' but port is not a number: '$cp:$portString'")
    }
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
        .withPort(port)
        .withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE))
        .withRetryPolicy(DefaultRetryPolicy.INSTANCE)
        .withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))

    val socketOptions = {
      var options: SocketOptions = null
      def ensureOptions(): SocketOptions = {
        if (options == null) {
          options = new SocketOptions()
        }
        options
      }
      ReadTimeoutMillis.toDuration.foreach(timeout => ensureOptions().setReadTimeoutMillis(timeout.toMillis.toInt))
      ConnectionTimeoutMillis.toDuration.foreach(timeout => ensureOptions().setConnectTimeoutMillis(timeout.toMillis.toInt))
      Option(options)
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

    val looseBBox = LooseBBoxParam.lookup(params)

    // not used but required for config inheritance
    val queryThreads = QueryThreadsParam.lookup(params)
    val queryTimeout = QueryTimeoutParam.lookupOpt(params).map(_.toMillis)

    val ns = Option(NamespaceParam.lookUp(params).asInstanceOf[String])

    val cfg = CassandraDataStoreConfig(catalog, generateStats, audit, caching, queryThreads, queryTimeout, looseBBox, ns)

    new CassandraDataStore(session, cfg)
  }

  override def isAvailable = true

  override def getDisplayName: String = CassandraDataStoreFactory.DisplayName

  override def getDescription: String = CassandraDataStoreFactory.Description

  override def getParametersInfo: Array[Param] =
    CassandraDataStoreFactory.ParameterInfo ++ Array(NamespaceParam, DeprecatedGeoServerPasswordParam)

  override def canProcess(params: java.util.Map[String,Serializable]): Boolean =
    CassandraDataStoreFactory.canProcess(params)

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object CassandraDataStoreFactory extends GeoMesaDataStoreInfo {

  override val DisplayName = "Cassandra (GeoMesa)"
  override val Description = "Apache Cassandra\u2122 distributed key/value store"

  override val ParameterInfo: Array[GeoMesaParam[_]] =
    Array(
      Params.ContactPointParam,
      Params.KeySpaceParam,
      Params.CatalogParam,
      Params.UserNameParam,
      Params.PasswordParam,
      Params.GenerateStatsParam,
      Params.AuditQueriesParam,
      Params.LooseBBoxParam,
      Params.CachingParam,
      Params.QueryThreadsParam,
      Params.QueryTimeoutParam
    )

  override def canProcess(params: java.util.Map[String,Serializable]): Boolean = Params.KeySpaceParam.exists(params)

  object Params extends GeoMesaDataStoreParams {

    override protected def looseBBoxDefault = false

    val ContactPointParam = new GeoMesaParam[String]("cassandra.contact.point", "HOST:PORT to Cassandra", optional = false, deprecatedKeys = Seq("geomesa.cassandra.contact.point"))
    val KeySpaceParam     = new GeoMesaParam[String]("cassandra.keyspace", "Cassandra Keyspace", optional = false, deprecatedKeys = Seq("geomesa.cassandra.keyspace"))
    val CatalogParam      = new GeoMesaParam[String]("cassandra.catalog", "Name of GeoMesa catalog table", optional = false, deprecatedKeys = Seq("geomesa.cassandra.catalog.table"))
    val UserNameParam     = new GeoMesaParam[String]("cassandra.username", "Username to connect with", deprecatedKeys = Seq("geomesa.cassandra.username"))
    val PasswordParam     = new GeoMesaParam[String]("cassandra.password", "Password to connect with", password = true, deprecatedKeys = Seq("geomesa.cassandra.password"))

    // used to handle geoserver password encryption in persisted ds params
    val DeprecatedGeoServerPasswordParam = new Param("password", classOf[String], "", false, null, ImmutableMap.of(Parameter.DEPRECATED, true, Parameter.IS_PASSWORD, true))
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
