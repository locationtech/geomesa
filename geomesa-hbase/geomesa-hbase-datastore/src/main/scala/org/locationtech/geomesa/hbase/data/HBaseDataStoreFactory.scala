/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
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
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.HBaseDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaDataStore
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{GeoMesaDataStoreConfig, GeoMesaDataStoreInfo, GeoMesaDataStoreParams}
import org.locationtech.geomesa.security
import org.locationtech.geomesa.security.{AuthorizationsProvider, SecurityParams}
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

import scala.collection.JavaConversions._


class HBaseDataStoreFactory extends DataStoreFactorySpi with LazyLogging {

  import HBaseDataStoreParams._

  // this is a pass-through required of the ancestor interface
  override def createNewDataStore(params: java.util.Map[String, Serializable]): DataStore = createDataStore(params)

  override def createDataStore(params: java.util.Map[String, Serializable]): DataStore = {

    // TODO HBase Connections don't seem to be Serializable...deal with it
    val connection = HBaseConnectionPool.getConnection(params, validateConnection)

    val catalog = getCatalog(params)

    val remoteFilters = RemoteFilteringParam.lookupOpt(params).map(_.booleanValue)
      .getOrElse(HBaseDataStoreFactory.RemoteFilterProperty.get.toBoolean)
    logger.debug(s"Using ${if (remoteFilters) "remote" else "local" } filtering")

    val generateStats = GenerateStatsParam.lookup(params)
    val audit = if (AuditQueriesParam.lookup(params)) {
      Some(AuditLogger, Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider), "hbase")
    } else {
      None
    }
    val queryThreads = QueryThreadsParam.lookup(params)
    val queryTimeout = QueryTimeoutParam.lookupOpt(params).map(_.toMillis)
    val maxRangesPerExtendedScan = MaxRangesPerExtendedScanParam.lookup(params)
    val looseBBox = LooseBBoxParam.lookup(params)
    val caching = CachingParam.lookup(params)
    val authsProvider = if (!EnableSecurityParam.lookup(params)) { None } else {
      Some(HBaseDataStoreFactory.buildAuthsProvider(connection, params))
    }
    val coprocessorUrl = CoprocessorUrlParam.lookupOpt(params)

    val ns = NamespaceParam.lookupOpt(params)

    val config = HBaseDataStoreConfig(catalog, remoteFilters, generateStats, audit, queryThreads, queryTimeout,
      maxRangesPerExtendedScan, looseBBox, caching, authsProvider, coprocessorUrl, ns)

    val ds = buildDataStore(connection, config)
    GeoMesaDataStore.initRemoteVersion(ds)
    ds
  }

  // overridden by BigtableFactory
  protected def getCatalog(params: java.util.Map[String, Serializable]): String = HBaseCatalogParam.lookup(params)

  // overridden by BigtableFactory
  protected def buildDataStore(connection: Connection, config: HBaseDataStoreConfig): HBaseDataStore =
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

  val HBaseGeoMesaPrincipal = "hbase.geomesa.principal"
  val HBaseGeoMesaKeyTab    = "hbase.geomesa.keytab"

  val RemoteFilterProperty = SystemProperty("geomesa.hbase.remote.filtering", "true")
  val ConfigPathProperty   = SystemProperty("geomesa.hbase.config.paths")

  override val DisplayName = "HBase (GeoMesa)"
  override val Description = "Apache HBase\u2122 distributed key/value store"

  override val ParameterInfo: Array[GeoMesaParam[_]] =
    Array(
      HBaseCatalogParam,
      ZookeeperParam,
      RemoteFilteringParam,
      CoprocessorUrlParam,
      ConfigPathsParam,
      QueryThreadsParam,
      QueryTimeoutParam,
      GenerateStatsParam,
      AuditQueriesParam,
      LooseBBoxParam,
      CachingParam,
      EnableSecurityParam,
      AuthsParam,
      ForceEmptyAuthsParam
    )

  private [geomesa] val BigTableParamCheck = "google.bigtable.instance.id"

  // check that the hbase-site.xml does not have bigtable keys
  override def canProcess(params: java.util.Map[java.lang.String,Serializable]): Boolean = {
    HBaseCatalogParam.exists(params) &&
        Option(HBaseConfiguration.create().get(BigTableParamCheck)).forall(_.trim.isEmpty)
  }

  case class HBaseDataStoreConfig(catalog: String,
                                  remoteFilter: Boolean,
                                  generateStats: Boolean,
                                  audit: Option[(AuditWriter, AuditProvider, String)],
                                  queryThreads: Int,
                                  queryTimeout: Option[Long],
                                  maxRangesPerExtendedScan: Int,
                                  looseBBox: Boolean,
                                  caching: Boolean,
                                  authProvider: Option[AuthorizationsProvider],
                                  coprocessorUrl: Option[Path],
                                  namespace: Option[String]) extends GeoMesaDataStoreConfig

  def buildAuthsProvider(connection: Connection, params: java.util.Map[String, Serializable]): AuthorizationsProvider = {
    val forceEmptyOpt: Option[java.lang.Boolean] = ForceEmptyAuthsParam.lookupOpt(params)
    val forceEmptyAuths = forceEmptyOpt.getOrElse(java.lang.Boolean.FALSE).asInstanceOf[Boolean]

    if (!VisibilityClient.isCellVisibilityEnabled(connection)) {
      throw new IllegalArgumentException("HBase cell visibility is not enabled on cluster")
    }

    // master auths is the superset of auths this connector/user can support
    val userName = User.getCurrent.getName
    val masterAuths = VisibilityClient.getAuths(connection, userName).getAuthList.map(_.toStringUtf8)

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
    val auths: List[String] =
      if (forceEmptyAuths || configuredAuths.length > 0) configuredAuths.toList
      else masterAuths.toList

    security.getAuthorizationsProvider(params, auths)
  }
}

object HBaseDataStoreParams extends GeoMesaDataStoreParams with SecurityParams {
  val HBaseCatalogParam             = new GeoMesaParam[String]("hbase.catalog", "Catalog table name", optional = false, deprecatedKeys = Seq("bigtable.table.name"))
  val ConnectionParam               = new GeoMesaParam[Connection]("hbase.connection", "Connection", deprecatedKeys = Seq("connection"))
  val ZookeeperParam                = new GeoMesaParam[String]("hbase.zookeepers", "List of HBase Zookeeper ensemble servers, comma-separated. Prefer including a valid 'hbase-site.xml' on the classpath over setting this parameter")
  val CoprocessorUrlParam           = new GeoMesaParam[Path]("hbase.coprocessor.url", "Coprocessor Url", deprecatedKeys = Seq("coprocessor.url"))
  val RemoteFilteringParam          = new GeoMesaParam[java.lang.Boolean]("hbase.remote.filtering", "Remote filtering", default = true, deprecatedKeys = Seq("remote.filtering"))
  val MaxRangesPerExtendedScanParam = new GeoMesaParam[java.lang.Integer]("hbase.ranges.max-per-extended-scan", "Max Ranges per Extended Scan", default = 100, deprecatedKeys = Seq("max.ranges.per.extended.scan"))
  val EnableSecurityParam           = new GeoMesaParam[java.lang.Boolean]("hbase.security.enabled", "Enable HBase Security (Visibilities)", default = false, deprecatedKeys = Seq("security.enabled"))
  val ConfigPathsParam              = new GeoMesaParam[String]("hbase.config.paths", "Additional HBase configuration resource files (comma-delimited)")
}
