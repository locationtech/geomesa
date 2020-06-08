/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.awt.RenderingHints
import java.io.Serializable

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.security.visibility.VisibilityClient
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.hbase.data.HBaseConnectionPool.ConnectionWrapper
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.{CoprocessorConfig, EnabledCoprocessors, HBaseDataStoreConfig, HBaseQueryConfig}
import org.locationtech.geomesa.hbase.data.HBaseDataStoreParams.CacheConnectionsParam
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{DataStoreQueryConfig, GeoMesaDataStoreConfig, GeoMesaDataStoreInfo}
import org.locationtech.geomesa.security
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

class HBaseDataStoreFactory extends DataStoreFactorySpi with LazyLogging {

  import HBaseDataStoreParams._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: java.util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {
    // TODO HBase Connections don't seem to be Serializable...deal with it
    val connection = HBaseConnectionPool.getConnection(params, validateConnection)

    val remoteFilters = RemoteFilteringParam.lookup(params).booleanValue

    val audit = if (!AuditQueriesParam.lookup(params)) { None } else {
      Some(AuditLogger, Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider), "hbase")
    }
    val auths = if (!EnableSecurityParam.lookup(params)) { None } else {
      Some(HBaseDataStoreFactory.buildAuthsProvider(connection.connection, params))
    }
    val queries = HBaseQueryConfig(
      threads = QueryThreadsParam.lookup(params),
      timeout = QueryTimeoutParam.lookupOpt(params).map(_.toMillis),
      looseBBox = LooseBBoxParam.lookup(params),
      caching = CachingParam.lookup(params),
      maxRangesPerExtendedScan = MaxRangesPerExtendedScanParam.lookup(params)
    )
    val enabledCoprocessors = EnabledCoprocessors(
      arrow = ArrowCoprocessorParam.lookup(params),
      bin = BinCoprocessorParam.lookup(params),
      density = DensityCoprocessorParam.lookup(params),
      stats = StatsCoprocessorParam.lookup(params)
    )
    val coprocessors = CoprocessorConfig(
      enabled = enabledCoprocessors,
      threads = CoprocessorThreadsParam.lookup(params),
      yieldPartialResults = YieldPartialResultsParam.lookup(params),
      maxRangesPerExtendedScan = MaxRangesPerCoprocessorScanParam.lookup(params),
      url = CoprocessorUrlParam.lookupOpt(params)
    )
    val config = HBaseDataStoreConfig(
      catalog = getCatalog(params),
      remoteFilter = remoteFilters,
      generateStats = GenerateStatsParam.lookup(params),
      queries = queries,
      coprocessors = coprocessors,
      authProvider = auths,
      audit = audit,
      namespace = NamespaceParam.lookupOpt(params)
    )

    logger.debug(s"Using ${if (remoteFilters) "remote" else "local" } filtering")
    lazy val enabled =
      Seq(ArrowCoprocessorParam, BinCoprocessorParam, DensityCoprocessorParam, StatsCoprocessorParam).collect {
        case p if p.exists(params) && p.lookup(params).booleanValue() => p.key
      }
    if (!remoteFilters && enabled.nonEmpty) {
      logger.warn(s"Ignoring configs '${enabled.mkString("', '")}' due to remote filtering being disabled")
    }

    val ds = buildDataStore(connection, config)
    GeoMesaDataStore.initRemoteVersion(ds)
    ds
  }

  // overridden by BigtableFactory
  protected def getCatalog(params: java.util.Map[String, Serializable]): String = HBaseCatalogParam.lookup(params)

  // overridden by BigtableFactory
  protected def buildDataStore(connection: ConnectionWrapper, config: HBaseDataStoreConfig): HBaseDataStore =
    new HBaseDataStore(connection, config)

  // overridden by BigtableFactory
  protected def validateConnection: Boolean = true

  override def isAvailable = true

  override def getDisplayName: String = HBaseDataStoreFactory.DisplayName

  override def getDescription: String = HBaseDataStoreFactory.Description

  override def getParametersInfo: Array[Param] = HBaseDataStoreFactory.ParameterInfo :+ NamespaceParam

  override def canProcess(params: java.util.Map[String,Serializable]): Boolean =
    HBaseDataStoreFactory.canProcess(params)

  override def getImplementationHints: java.util.Map[RenderingHints.Key, _] = null
}

object HBaseDataStoreFactory extends GeoMesaDataStoreInfo with LazyLogging {

  import HBaseDataStoreParams._

  import scala.collection.JavaConverters._

  val HBaseGeoMesaPrincipal = "hbase.geomesa.principal"
  val HBaseGeoMesaKeyTab    = "hbase.geomesa.keytab"

  val ConfigPathProperty          : SystemProperty = SystemProperty("geomesa.hbase.config.paths")
  val RemoteFilterProperty        : SystemProperty = SystemProperty("geomesa.hbase.remote.filtering", "true")
  val RemoteArrowProperty         : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.arrow.enable")
  val RemoteBinProperty           : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.bin.enable")
  val RemoteDensityProperty       : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.density.enable")
  val RemoteStatsProperty         : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.stats.enable")
  val YieldPartialResultsProperty : SystemProperty = SystemProperty("geomesa.hbase.coprocessor.yield.partial.results")

  override val DisplayName = "HBase (GeoMesa)"
  override val Description = "Apache HBase\u2122 distributed key/value store"

  override val ParameterInfo: Array[GeoMesaParam[_]] =
    Array(
      HBaseCatalogParam,
      ZookeeperParam,
      ConfigPathsParam,
      ConfigsParam,
      CoprocessorUrlParam,
      QueryThreadsParam,
      CoprocessorThreadsParam,
      QueryTimeoutParam,
      MaxRangesPerExtendedScanParam,
      MaxRangesPerCoprocessorScanParam,
      CacheConnectionsParam,
      RemoteFilteringParam,
      ArrowCoprocessorParam,
      BinCoprocessorParam,
      DensityCoprocessorParam,
      StatsCoprocessorParam,
      YieldPartialResultsParam,
      EnableSecurityParam,
      GenerateStatsParam,
      AuditQueriesParam,
      LooseBBoxParam,
      CachingParam,
      AuthsParam,
      ForceEmptyAuthsParam
    )

  private [geomesa] val BigTableParamCheck = "google.bigtable.instance.id"

  // check that the hbase-site.xml does not have bigtable keys
  override def canProcess(params: java.util.Map[String, _ <: java.io.Serializable]): Boolean = {
    HBaseCatalogParam.exists(params) &&
        Option(HBaseConfiguration.create().get(BigTableParamCheck)).forall(_.trim.isEmpty)
  }

  case class HBaseDataStoreConfig(
      catalog: String,
      remoteFilter: Boolean,
      generateStats: Boolean,
      queries: HBaseQueryConfig,
      coprocessors: CoprocessorConfig,
      authProvider: Option[AuthorizationsProvider],
      audit: Option[(AuditWriter, AuditProvider, String)],
      namespace: Option[String]
    ) extends GeoMesaDataStoreConfig

  case class HBaseQueryConfig(
      threads: Int,
      timeout: Option[Long],
      looseBBox: Boolean,
      caching: Boolean,
      maxRangesPerExtendedScan: Int
    ) extends DataStoreQueryConfig

  case class CoprocessorConfig(
      enabled: EnabledCoprocessors,
      threads: Int,
      yieldPartialResults: Boolean,
      maxRangesPerExtendedScan: Int,
      url: Option[Path]
    )

  case class EnabledCoprocessors(arrow: Boolean, bin: Boolean, density: Boolean, stats: Boolean)

  def buildAuthsProvider(connection: Connection, params: java.util.Map[String, Serializable]): AuthorizationsProvider = {
    val forceEmptyOpt: Option[java.lang.Boolean] = ForceEmptyAuthsParam.lookupOpt(params)
    val forceEmptyAuths = forceEmptyOpt.getOrElse(java.lang.Boolean.FALSE).asInstanceOf[Boolean]

    if (!VisibilityClient.isCellVisibilityEnabled(connection)) {
      throw new IllegalArgumentException("HBase cell visibility is not enabled on cluster")
    }

    // master auths is the superset of auths this connector/user can support
    val userName = User.getCurrent.getName
    val masterAuths = VisibilityClient.getAuths(connection, userName).getAuthList.asScala.map(_.toStringUtf8)

    // get the auth params passed in as a comma-delimited string
    val configuredAuths = AuthsParam.lookupOpt(params).getOrElse("").split(",").filter(s => !s.isEmpty)

    // verify that the configured auths are valid for the connector we are using (fail-fast)
    val invalidAuths = configuredAuths.filterNot(masterAuths.contains)
    if (invalidAuths.nonEmpty) {
      val msg = s"The authorizations '${invalidAuths.mkString("', '")}' are not valid for the HBase user '$userName'"
      if (masterAuths.isEmpty) {
        // looking up auths requires a system-level user - likely the user does not have permission
        logger.warn(s"$msg. This may be due to the user not having permissions" +
            " to read its own authorizations, in which case this warning can be ignored.")
      } else {
        throw new IllegalArgumentException(s"$msg. Available authorizations are: ${masterAuths.mkString(", ")}")
      }
    }

    // if the caller provided any non-null string for authorizations, use it;
    // otherwise, grab all authorizations to which the user is entitled
    if (configuredAuths.length != 0 && forceEmptyAuths) {
      throw new IllegalArgumentException("Forcing empty auths is checked, but explicit auths are provided")
    }
    val auths = if (forceEmptyAuths || configuredAuths.nonEmpty) { configuredAuths.toList } else { masterAuths.toList }

    security.getAuthorizationsProvider(params, auths)
  }
}
