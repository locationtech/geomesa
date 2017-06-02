/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import java.io.Serializable

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.hbase.security.User
import org.apache.hadoop.hbase.security.visibility.VisibilityClient
import org.apache.hadoop.hbase.util.Bytes
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.{DataStore, DataStoreFactorySpi}
import org.locationtech.geomesa.hbase.data.HBaseDataStoreFactory.HBaseDataStoreConfig
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.{GeoMesaDataStoreConfig, _}
import org.locationtech.geomesa.security
import org.locationtech.geomesa.security.AuthorizationsProvider
import org.locationtech.geomesa.utils.audit.{AuditLogger, AuditProvider, AuditWriter, NoOpAuditProvider}
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty

import scala.collection.JavaConversions._


class HBaseDataStoreFactory extends DataStoreFactorySpi with LazyLogging {

  import HBaseDataStoreParams._

  // TODO: investigate multiple HBase connections per jvm
  private lazy val globalConnection: Connection = {
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

    // TODO HBase Connections don't seem to be Serializable...deal with it
    val connection = ConnectionParam.lookupOpt[Connection](params).getOrElse(globalConnection)

    val catalog = BigTableNameParam.lookup[String](params)

    val remoteFilters = RemoteFiltersParam.lookupOpt[Boolean](params)
      .getOrElse(SystemProperty("geomesa.hbase.remote.filtering", "true").get.toBoolean)
    logger.debug(s"Using ${if (remoteFilters) "remote" else "local" } filtering")

    val generateStats = GenerateStatsParam.lookupWithDefault[Boolean](params)
    val audit = if (AuditQueriesParam.lookupWithDefault[Boolean](params)) {
      Some(AuditLogger, Option(AuditProvider.Loader.load(params)).getOrElse(NoOpAuditProvider), "hbase")
    } else {
      None
    }
    val queryThreads = QueryThreadsParam.lookupWithDefault[Int](params)
    val queryTimeout = GeoMesaDataStoreFactory.queryTimeout(params)
    val maxRangesPerExtendedScan = MaxRangesPerExtendedScan.lookupWithDefault[java.lang.Integer](params)
    val looseBBox = LooseBBoxParam.lookupWithDefault[Boolean](params)
    val caching = CachingParam.lookupWithDefault[Boolean](params)
    val security = EnableSecurityParam.lookup[Boolean](params)
    val authsProvider =
      if (security) {
       Some(HBaseDataStoreFactory.buildAuthsProvider(connection, params))
      } else None
    val coprocessorUrl = CoprocessorUrl.lookupOpt[Path](params)

    val config = HBaseDataStoreConfig(catalog, remoteFilters, generateStats, audit, queryThreads, queryTimeout,
      maxRangesPerExtendedScan, looseBBox, caching, authsProvider, coprocessorUrl)
    buildDataStore(connection, config)
  }

  // overidden by BigtableFactory
  def buildDataStore(connection: Connection, config: HBaseDataStoreConfig): HBaseDataStore =
    new HBaseDataStore(connection, config)

  override def getDisplayName: String = HBaseDataStoreFactory.DisplayName

  override def getDescription: String = HBaseDataStoreFactory.Description

  override def getParametersInfo: Array[Param] =
    Array(
      BigTableNameParam,
      RemoteFiltersParam,
      QueryThreadsParam,
      QueryTimeoutParam,
      CoprocessorUrl,
      GenerateStatsParam,
      AuditQueriesParam,
      LooseBBoxParam,
      CachingParam,
      EnableSecurityParam,
      AuthsParam,
      ForceEmptyAuthsParam)

  override def canProcess(params: java.util.Map[String,Serializable]): Boolean =
    HBaseDataStoreFactory.canProcess(params)

  override def isAvailable = true

  override def getImplementationHints = null
}

object HBaseDataStoreParams {
  val BigTableNameParam    = new Param("bigtable.table.name", classOf[String], "Table name", true)
  val ConnectionParam      = new Param("connection", classOf[Connection], "Connection", false)
  val CoprocessorUrl       = new Param("coprocessor.url", classOf[Path], "Coprocessor Url", false, null)
  val RemoteFiltersParam   = new Param("remote.filtering", classOf[java.lang.Boolean], "Remote filtering", false)
  val LooseBBoxParam       = GeoMesaDataStoreFactory.LooseBBoxParam
  val QueryThreadsParam    = GeoMesaDataStoreFactory.QueryThreadsParam
  val MaxRangesPerExtendedScan    = new Param("max.ranges.per.extended.scan", classOf[java.lang.Integer], "Max Ranges per Extended Scan", false, 100)
  val GenerateStatsParam   = GeoMesaDataStoreFactory.GenerateStatsParam
  val AuditQueriesParam    = GeoMesaDataStoreFactory.AuditQueriesParam
  val QueryTimeoutParam    = GeoMesaDataStoreFactory.QueryTimeoutParam
  val CachingParam         = GeoMesaDataStoreFactory.CachingParam
  val EnableSecurityParam  = new Param("security.enabled", classOf[java.lang.Boolean], "Enable HBase Security (Visibilities)", false, false)
  val AuthsParam           = org.locationtech.geomesa.security.AuthsParam
  val ForceEmptyAuthsParam = org.locationtech.geomesa.security.ForceEmptyAuthsParam

}

object HBaseDataStoreFactory {

  import HBaseDataStoreParams._
  val DisplayName = "HBase (GeoMesa)"
  val Description = "Apache HBase\u2122 distributed key/value store"

  private [geomesa] val BigTableParamCheck = "google.bigtable.instance.id"

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
                                  coprocessorUrl: Option[Path]) extends GeoMesaDataStoreConfig

  // check that the hbase-site.xml does not have bigtable keys
  def canProcess(params: java.util.Map[java.lang.String,Serializable]): Boolean = {
    params.containsKey(BigTableNameParam.key) &&
      Option(HBaseConfiguration.create().get(BigTableParamCheck)).forall(_.trim.isEmpty)
  }

  def buildAuthsProvider(connection: Connection, params: java.util.Map[String, Serializable]): AuthorizationsProvider = {
    val forceEmptyOpt: Option[java.lang.Boolean] = security.ForceEmptyAuthsParam.lookupOpt[java.lang.Boolean](params)
    val forceEmptyAuths = forceEmptyOpt.getOrElse(java.lang.Boolean.FALSE).asInstanceOf[Boolean]

    if (!VisibilityClient.isCellVisibilityEnabled(connection)) {
      throw new IllegalArgumentException("HBase cell visibility is not enabled on cluster")
    }

    // master auths is the superset of auths this connector/user can support
    val userName = User.getCurrent.getName
    val masterAuths = VisibilityClient.getAuths(connection, userName).getAuthList.map(a => Bytes.toString(a.toByteArray))

    // get the auth params passed in as a comma-delimited string
    val configuredAuths = AuthsParam.lookupOpt[String](params).getOrElse("").split(",").filter(s => !s.isEmpty)

    // verify that the configured auths are valid for the connector we are using (fail-fast)
    val invalidAuths = configuredAuths.filterNot(masterAuths.contains)
    if (invalidAuths.nonEmpty) {
      throw new IllegalArgumentException(s"The authorizations '${invalidAuths.mkString(",")}' " +
        "are not valid for the HBase user and connection being used")
    }

    // if the caller provided any non-null string for authorizations, use it;
    // otherwise, grab all authorizations to which the Accumulo user is entitled
    if (configuredAuths.length != 0 && forceEmptyAuths) {
      throw new IllegalArgumentException("Forcing empty auths is checked, but explicit auths are provided")
    }
    val auths: List[String] =
      if (forceEmptyAuths || configuredAuths.length > 0) configuredAuths.toList
      else masterAuths.toList

    security.getAuthorizationsProvider(params, auths)
  }
}
