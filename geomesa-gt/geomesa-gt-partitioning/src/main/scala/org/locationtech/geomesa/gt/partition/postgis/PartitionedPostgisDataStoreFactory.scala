/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.gt.partition.postgis

import com.typesafe.config.ConfigFactory
import com.typesafe.scalalogging.LazyLogging
import io.micrometer.core.instrument.binder.db.PostgreSQLDatabaseMetrics
import io.micrometer.core.instrument.{Metrics, Tag, Tags}
import org.apache.commons.dbcp.BasicDataSource
import org.geotools.data.jdbc.datasource.AbstractManageableDataSource
import org.geotools.data.postgis.{PostGISDialect, PostGISPSDialect, PostgisNGDataStoreFactory}
import org.geotools.jdbc.{JDBCDataStore, JDBCDataStoreFactory, SQLDialect}
import org.locationtech.geomesa.gt.partition.postgis.PartitionedPostgisDataStoreFactory.Dbcp2ManagedDataSource
import org.locationtech.geomesa.gt.partition.postgis.dialect.{PartitionedPostgisDialect, PartitionedPostgisPsDialect, RoleName}
import org.locationtech.geomesa.metrics.micrometer.dbcp2.MetricsDataSource
import org.locationtech.geomesa.utils.io.CloseWithLogging

import java.io.Closeable
import java.time.Duration
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap
import javax.sql.DataSource
import scala.util.control.NonFatal

class PartitionedPostgisDataStoreFactory extends PostgisNGDataStoreFactory with LazyLogging {

  import JDBCDataStoreFactory.{DATABASE, USER}
  import PartitionedPostgisDataStoreParams.{DbType, IdleInTransactionTimeout, PreparedStatements, ReadAccessRoles}
  import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{MetricsRegistryConfigParam, MetricsRegistryParam}

  import scala.collection.JavaConverters._

  override def getDisplayName: String = "PostGIS (partitioned)"

  override def getDescription: String = "PostGIS Database with time-partitioned tables"

  override protected def getDatabaseID: String = DbType.sample.asInstanceOf[String]

  override protected def setupParameters(parameters: java.util.Map[String, AnyRef]): Unit = {
    super.setupParameters(parameters)
    Seq(DbType, IdleInTransactionTimeout, PreparedStatements, ReadAccessRoles, MetricsRegistryParam, MetricsRegistryConfigParam)
        .foreach(p => parameters.put(p.key, p))
  }

  override protected def createDataSource(params: java.util.Map[String, _], dialect: SQLDialect): DataSource = {
    val dataSource = super.createDataSource(params, dialect)

    val metrics = MetricsRegistryParam.lookupRegistry(params).flatMap { registry =>
      var metrics: MetricsDataSource = null
      try {
        // try to re-create the datasource as a commons-dbcp2 instance so we can instrument it
        val basic = dataSource match {
          case ds: AbstractManageableDataSource => ds.unwrap(classOf[BasicDataSource])
          case ds: BasicDataSource => ds
          case ds => throw new UnsupportedOperationException(s"Unexpected data source class: ${ds.getClass.getName} $ds")
        }
        metrics = new MetricsDataSource("postgis")

        // these are the options that are configured in our parent factories
        metrics.setDriverClassName(basic.getDriverClassName)
        metrics.setUrl(basic.getUrl)
        metrics.setUsername(basic.getUsername)
        metrics.setPassword(basic.getPassword)
        metrics.setMaxWait(Duration.ofMillis(basic.getMaxWait))
        metrics.setMinIdle(basic.getMinIdle)
        metrics.setMaxTotal(basic.getMaxActive)
        metrics.setTestOnBorrow(basic.getTestOnBorrow)
        metrics.setValidationQuery(basic.getValidationQuery)
        metrics.setTestWhileIdle(basic.getTestWhileIdle)
        metrics.setDurationBetweenEvictionRuns(Duration.ofMillis(basic.getTimeBetweenEvictionRunsMillis))
        metrics.setMinEvictableIdle(Duration.ofMillis(basic.getMinEvictableIdleTimeMillis))
        metrics.setNumTestsPerEvictionRun(basic.getNumTestsPerEvictionRun)
        metrics.setAccessToUnderlyingConnectionAllowed(basic.isAccessToUnderlyingConnectionAllowed)
        metrics.setMaxOpenPreparedStatements(basic.getMaxOpenPreparedStatements)
        metrics.setPoolPreparedStatements(basic.isPoolPreparedStatements)

        // re-add connection properties, as we can't access those from the BasicDataSource
        createConnectionOptions(params).foreach { case (k, v) => metrics.addConnectionProperty(k, v) }

        val registration = registry.register()

        tryBindPostgresMetrics(metrics, params)

        // we attach the registry to the ManagedDataSource b/c that's the only hook we have into datastore.dispose()
        Some(new Dbcp2ManagedDataSource(metrics, registration))
      } catch {
        case NonFatal(e) =>
          logger.warn("Unable to instrument data source, metrics will not be available:", e)
          CloseWithLogging(Option(metrics))
          None
      }
    }

    metrics.getOrElse(dataSource)
  }

  private def tryBindPostgresMetrics(dataSource: MetricsDataSource, params: java.util.Map[String, _]): Unit = {
    try {
      val conf =
        MetricsRegistryConfigParam.lookupOpt(params)
          .getOrElse(ConfigFactory.empty)
          .withFallback(ConfigFactory.load("postgres-metrics"))
          .getConfig("postgres")
      if (conf.getBoolean("enabled")) {
        val tags = Tags.of(conf.getConfig("tags").root().unwrapped().asScala.map { case (k, v) => Tag.of(k, v.toString) }.asJava)
        val db =
          Option(DATABASE.lookUp(params).asInstanceOf[String])
            .orElse(Option(USER.lookUp(params).asInstanceOf[String]))
            .getOrElse(throw new IllegalArgumentException(s"${DATABASE.key} was not found in the parameter map"))
        if (PartitionedPostgisDataStoreFactory.bindings.add(db -> tags)) {
          logger.info("Adding postgres metrics")
          new PostgreSQLDatabaseMetrics(dataSource, db, tags).bindTo(Metrics.globalRegistry)
        }
      }
    } catch {
      case NonFatal(e) => logger.warn("Error binding postgres metrics:", e)
    }
  }

  override def createDataSource(params: java.util.Map[String, _]): BasicDataSource = {
    val source = super.createDataSource(params)
    createConnectionOptions(params).foreach { case (k, v) => source.addConnectionProperty(k, v) }
    source
  }

  override protected def createDataStoreInternal(store: JDBCDataStore, baseParams: java.util.Map[String, _]): JDBCDataStore = {
    val params = new java.util.HashMap[String, Any](baseParams)
    // default to using prepared statements, if not specified
    if (!params.containsKey(PreparedStatements.key)) {
      params.put(PreparedStatements.key, java.lang.Boolean.TRUE)
    }
    // set default schema, if not specified - postgis store doesn't actually use its own default
    if (!params.containsKey(PostgisNGDataStoreFactory.SCHEMA.key)) {
      // need to set it in the store, as the key has already been processed
      store.setDatabaseSchema("public")
      // also set in the params for consistency, although it's not used anywhere
      params.put(PostgisNGDataStoreFactory.SCHEMA.key, "public")
    }
    val roles = Option(ReadAccessRoles.lookUp(params).asInstanceOf[String]).toSeq.flatMap(_.split(",")).map(r => RoleName(r.trim))

    val ds = super.createDataStoreInternal(store, params)
    val dialect = new PartitionedPostgisDialect(ds, roles)

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

  private def createConnectionOptions(params: java.util.Map[String, _]): Map[String, String] = {
    val options =
      Seq(IdleInTransactionTimeout)
        .flatMap(p => p.opt(params).map(t => s"-c ${p.key}=${t.millis}"))

    logger.debug(s"Connection options: ${options.mkString(" ")}")

    if (options.isEmpty) {
      Map.empty
    } else {
      Map("options" -> options.mkString(" "))
    }
  }
}

object PartitionedPostgisDataStoreFactory {

  // instrumentations that we've already bound, to prevent duplicates
  private val bindings = Collections.newSetFromMap(new ConcurrentHashMap[(String, Tags), java.lang.Boolean]())

  /**
   * Managed data source for apache dbcp2. Managed data sources expose a `close` method on data sources.
   *
   * @param wrapped wrapped data source
   * @param registry metrics registry
   */
  class Dbcp2ManagedDataSource(wrapped: org.apache.commons.dbcp2.BasicDataSource, registry: Closeable)
      extends AbstractManageableDataSource(wrapped) {

    override def close(): Unit = {
      CloseWithLogging(registry)
      wrapped.close()
    }

    override def isWrapperFor(c: Class[_]): Boolean = classOf[DataSource].isAssignableFrom(c)

    override def unwrap[T](c: Class[T]): T = {
      if (isWrapperFor(c)) {
        wrapped.asInstanceOf[T]
      } else {
        null.asInstanceOf[T]
      }
    }
  }
}
