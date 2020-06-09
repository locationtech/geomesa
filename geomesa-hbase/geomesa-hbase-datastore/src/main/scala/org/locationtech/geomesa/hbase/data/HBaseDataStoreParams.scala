/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.hbase.data

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Connection
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreParams
import org.locationtech.geomesa.security.SecurityParams
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.geotools.GeoMesaParam.SystemPropertyBooleanParam

object HBaseDataStoreParams extends GeoMesaDataStoreParams with SecurityParams {

  val HBaseCatalogParam =
    new GeoMesaParam[String](
      "hbase.catalog",
      "Catalog table name",
      optional = false,
      deprecatedKeys = Seq("bigtable.table.name"),
      supportsNiFiExpressions = true)

  val ConnectionParam =
    new GeoMesaParam[Connection](
      "hbase.connection",
      "Connection",
      deprecatedKeys = Seq("connection"))

  val ZookeeperParam =
    new GeoMesaParam[String](
      "hbase.zookeepers",
      "List of HBase Zookeeper ensemble servers, comma-separated. " +
          "Prefer including a valid 'hbase-site.xml' on the classpath over setting this parameter",
      supportsNiFiExpressions = true)

  val CoprocessorUrlParam =
    new GeoMesaParam[Path](
      "hbase.coprocessor.url",
      "URL pointing to the GeoMesa coprocessor JAR",
      deprecatedKeys = Seq("coprocessor.url"),
      supportsNiFiExpressions = true)

  val CoprocessorThreadsParam =
    new GeoMesaParam[Integer](
      "hbase.coprocessor.threads",
      "The number of HBase RPC threads to use per coprocessor query",
      default = Int.box(16),
      supportsNiFiExpressions = true)

  val CacheConnectionsParam =
    new GeoMesaParam[java.lang.Boolean](
      "hbase.connections.reuse",
      "Use a shared HBase connection, or create a new connection",
      default = java.lang.Boolean.TRUE)

  val RemoteFilteringParam =
    new GeoMesaParam[java.lang.Boolean](
      "hbase.remote.filtering",
      "Enable remote filtering, i.e. filters and coprocessors",
      default = java.lang.Boolean.TRUE,
      deprecatedKeys = Seq("remote.filtering"),
      systemProperty = Some(SystemPropertyBooleanParam(HBaseDataStoreFactory.RemoteFilterProperty)))

  val MaxRangesPerExtendedScanParam =
    new GeoMesaParam[java.lang.Integer](
      "hbase.ranges.max-per-extended-scan",
      "Max ranges per extended scan. Ranges will be grouped into scans based on this setting",
      default = 100,
      deprecatedKeys = Seq("max.ranges.per.extended.scan"),
      supportsNiFiExpressions = true)

  val MaxRangesPerCoprocessorScanParam =
    new GeoMesaParam[java.lang.Integer](
      "hbase.ranges.max-per-coprocessor-scan",
      "Max ranges per coprocessor scan. Ranges will be grouped into scans based on this setting",
      default = Int.MaxValue,
      supportsNiFiExpressions = true)

  val EnableSecurityParam =
    new GeoMesaParam[java.lang.Boolean](
      "hbase.security.enabled",
      "Enable HBase Security (Visibilities)",
      default = java.lang.Boolean.FALSE,
      deprecatedKeys = Seq("security.enabled"))

  val ConfigPathsParam =
    new GeoMesaParam[String](
      "hbase.config.paths",
      "Additional HBase configuration resource files (comma-delimited)",
      supportsNiFiExpressions = true)

  val ConfigsParam =
    new GeoMesaParam[String](
      "hbase.config.xml",
      "Additional HBase configuration properties, as a standard XML `<configuration>` element",
      largeText = true,
      supportsNiFiExpressions = true)

  val ArrowCoprocessorParam =
    new GeoMesaParam[java.lang.Boolean](
      "hbase.coprocessor.arrow.enable",
      "Processes Arrow encoding in HBase region servers as a coprocessor call",
      default = java.lang.Boolean.TRUE,
      systemProperty = Some(SystemPropertyBooleanParam(HBaseDataStoreFactory.RemoteArrowProperty))
    )

  val BinCoprocessorParam =
    new GeoMesaParam[java.lang.Boolean](
      "hbase.coprocessor.bin.enable",
      "Processes binary encoding in HBase region servers as a coprocessor call",
      default = java.lang.Boolean.TRUE,
      systemProperty = Some(SystemPropertyBooleanParam(HBaseDataStoreFactory.RemoteBinProperty))
    )

  val DensityCoprocessorParam =
    new GeoMesaParam[java.lang.Boolean](
      "hbase.coprocessor.density.enable",
      "Processes heatmap encoding in HBase region servers as a coprocessor call",
      default = java.lang.Boolean.TRUE,
      systemProperty = Some(SystemPropertyBooleanParam(HBaseDataStoreFactory.RemoteDensityProperty))
    )

  val StatsCoprocessorParam =
    new GeoMesaParam[java.lang.Boolean](
      "hbase.coprocessor.stats.enable",
      "Processes statistical calculations in HBase region servers as a coprocessor call",
      default = java.lang.Boolean.TRUE,
      systemProperty = Some(SystemPropertyBooleanParam(HBaseDataStoreFactory.RemoteStatsProperty))
    )

  val YieldPartialResultsParam =
    new GeoMesaParam[java.lang.Boolean](
      "hbase.coprocessor.yield.partial.results",
      "Yield Partial Results",
      default = java.lang.Boolean.FALSE,
      deprecatedKeys = Seq(),
      systemProperty = Some(SystemPropertyBooleanParam(HBaseDataStoreFactory.YieldPartialResultsProperty)))
}
