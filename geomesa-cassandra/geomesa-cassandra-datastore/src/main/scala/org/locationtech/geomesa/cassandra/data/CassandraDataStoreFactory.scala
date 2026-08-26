/***********************************************************************
 * Copyright (c) 2017-2025 IBM
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.cassandra.data

import com.datastax.oss.driver.api.core._
import org.geotools.api.data.DataAccessFactory.Param
import org.geotools.api.data.{DataStore, DataStoreFactorySpi, Parameter}
import org.locationtech.geomesa.cassandra.data.CassandraDataStoreFactory.{CassandraDataStoreConfig, CassandraQueryConfig}
import org.locationtech.geomesa.index.audit.AuditWriter
import org.locationtech.geomesa.index.audit.AuditWriter.AuditLogger
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory._
import org.locationtech.geomesa.security.{AuthorizationsProvider, DefaultAuthorizationsProvider}
import org.locationtech.geomesa.utils.audit.AuditProvider
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

import java.awt.RenderingHints
import java.net.InetSocketAddress
import java.util
import scala.util.control.NonFatal

class CassandraDataStoreFactory extends DataStoreFactorySpi {

  import CassandraDataStoreFactory.Params._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: util.Map[String, _]): DataStore = createDataStore(params)

  override def createDataStore(params: util.Map[String, _]): DataStore = {
    val (cp, portString) = ContactPointParam.lookup(params).split(":") match {
      case Array(one, two) => (one, two)
      case parts => throw new IllegalArgumentException(s"Invalid parameter '${ContactPointParam.key}', " +
          s"expected '<host>:<port>' but got '${parts.mkString(":")}'")
    }
    val port = try { portString.toInt } catch {
      case NonFatal(_) => throw new IllegalArgumentException(s"Invalid parameter '${ContactPointParam.key}', " +
          s"expected '<host>:<port>' but port is not a number: '$cp:$portString'")
    }
    val localDatacenter = LocalDatacenterParam.lookup(params)
    val ks = KeySpaceParam.lookup(params)
    val generateStats = GenerateStatsParam.lookup(params)
    val audit = if (AuditQueriesParam.lookup(params)) {
      Some(new AuditLogger("cassandra", AuditProvider.Loader.loadOrNone(params)))
    } else {
      None
    }
    val metrics = MetricsRegistryParam.lookupRegistry(params)

    val sessionBuilder = CqlSession.builder()
      .addContactPoint(new InetSocketAddress(cp, port))
      .withLocalDatacenter(localDatacenter)
      .withKeyspace(ks)

    val user = UserNameParam.lookup(params)
    val password = PasswordParam.lookup(params)
    if (user != null && password != null) {
      sessionBuilder.withAuthCredentials(user, password)
    }

    val session = sessionBuilder.build()
    val catalog = CatalogParam.lookup(params)

    val queries = CassandraQueryConfig(
      threads = QueryThreadsParam.lookup(params),
      timeout = QueryTimeoutParam.lookupOpt(params).map(_.toMillis),
      looseBBox = LooseBBoxParam.lookup(params),
      parallelPartitionScans = PartitionParallelScansParam.lookup(params)
    )

    val authProvider = new DefaultAuthorizationsProvider(Seq.empty)

    val ns = Option(NamespaceParam.lookUp(params).asInstanceOf[String])

    val cfg = CassandraDataStoreConfig(catalog, generateStats, authProvider, audit, metrics, queries, ns)

    new CassandraDataStore(session, cfg)
  }

  override def isAvailable = true

  override def getDisplayName: String = CassandraDataStoreFactory.DisplayName

  override def getDescription: String = CassandraDataStoreFactory.Description

  override def getParametersInfo: Array[Param] =
    CassandraDataStoreFactory.ParameterInfo ++
        Array(NamespaceParam, CassandraDataStoreFactory.DeprecatedGeoServerPasswordParam)

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    CassandraDataStoreFactory.canProcess(params)

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object CassandraDataStoreFactory extends GeoMesaDataStoreInfo {

  import scala.collection.JavaConverters._

  // used to handle geoserver password encryption in persisted ds params
  private val DeprecatedGeoServerPasswordParam =
    new Param(
      "password",
      classOf[String],
      "",
      false,
      null,
      Map(Parameter.DEPRECATED -> true, Parameter.IS_PASSWORD -> true).asJava)

  override val DisplayName = "Cassandra (GeoMesa)"
  override val Description = "Apache Cassandra\u2122 distributed key/value store"

  override val ParameterInfo: Array[GeoMesaParam[_ <: AnyRef]] =
    Array(
      Params.ContactPointParam,
      Params.LocalDatacenterParam,
      Params.KeySpaceParam,
      Params.CatalogParam,
      Params.UserNameParam,
      Params.PasswordParam,
      Params.GenerateStatsParam,
      Params.AuditQueriesParam,
      Params.MetricsRegistryParam,
      Params.MetricsRegistryConfigParam,
      Params.LooseBBoxParam,
      Params.PartitionParallelScansParam,
      Params.QueryThreadsParam,
      Params.QueryTimeoutParam
    )

  override def canProcess(params: java.util.Map[String, _]): Boolean =
    Params.KeySpaceParam.exists(params)

  object Params extends GeoMesaDataStoreParams {

    override protected def looseBBoxDefault = false

    val ContactPointParam =
      new GeoMesaParam[String](
        "cassandra.contact.point",
        "HOST:PORT to Cassandra",
        optional = false,
        deprecatedKeys = Seq("geomesa.cassandra.contact.point"),
        supportsNiFiExpressions = true)

    val LocalDatacenterParam =
      new GeoMesaParam[String](
        "cassandra.local-datacenter",
        "Cassandra local datacenter",
        optional = false,
        deprecatedKeys = Seq.empty,
        supportsNiFiExpressions = true)

    val KeySpaceParam =
      new GeoMesaParam[String](
        "cassandra.keyspace",
        "Cassandra Keyspace",
        optional = false,
        deprecatedKeys = Seq("geomesa.cassandra.keyspace"),
        supportsNiFiExpressions = true)

    val CatalogParam =
      new GeoMesaParam[String](
        "cassandra.catalog",
        "Name of GeoMesa catalog table",
        optional = false,
        deprecatedKeys = Seq("geomesa.cassandra.catalog.table"),
        supportsNiFiExpressions = true)

    val UserNameParam =
      new GeoMesaParam[String](
        "cassandra.username",
        "Username to connect with",
        deprecatedKeys = Seq("geomesa.cassandra.username"),
        supportsNiFiExpressions = true)

    val PasswordParam =
      new GeoMesaParam[String](
        "cassandra.password",
        "Password to connect with",
        password = true,
        deprecatedKeys = Seq("geomesa.cassandra.password"),
        supportsNiFiExpressions = true)
  }

  case class CassandraDataStoreConfig(
      catalog: String,
      generateStats: Boolean,
      authProvider: AuthorizationsProvider,
      audit: Option[AuditWriter],
      metrics: Option[MetricsConfig],
      queries: CassandraQueryConfig,
      namespace: Option[String]
    ) extends GeoMesaDataStoreConfig

  case class CassandraQueryConfig(
      threads: Int,
      timeout: Option[Long],
      looseBBox: Boolean,
      parallelPartitionScans: Boolean
    ) extends DataStoreQueryConfig
}
