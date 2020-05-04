/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.data


import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreParams
import org.locationtech.geomesa.security.SecurityParams
import org.locationtech.geomesa.utils.conf.GeoMesaSystemProperties.SystemProperty
import org.locationtech.geomesa.utils.geotools.GeoMesaParam
import org.locationtech.geomesa.utils.geotools.GeoMesaParam.{SystemPropertyBooleanParam, SystemPropertyStringParam}

// keep params in a separate object so we don't require accumulo classes on the build path to access it
object AccumuloDataStoreParams extends GeoMesaDataStoreParams with SecurityParams {

  val InstanceIdParam =
    new GeoMesaParam[String](
      "accumulo.instance.id",
      "Accumulo Instance ID",
      deprecatedKeys = Seq("instanceId", "accumulo.instanceId"),
      supportsNiFiExpressions = true)

  val ZookeepersParam =
    new GeoMesaParam[String](
      "accumulo.zookeepers",
      "Zookeepers",
      deprecatedKeys = Seq("zookeepers"),
      supportsNiFiExpressions = true)

  val ZookeeperTimeoutParam =
    new GeoMesaParam[String](
      "accumulo.zookeepers.timeout",
      "The timeout used for connections to Zookeeper",
      supportsNiFiExpressions = true,
      systemProperty = Some(SystemPropertyStringParam(SystemProperty("instance.zookeeper.timeout"))))

  val UserParam =
    new GeoMesaParam[String](
      "accumulo.user",
      "Accumulo user",
      deprecatedKeys = Seq("user"),
      supportsNiFiExpressions = true)

  val PasswordParam =
    new GeoMesaParam[String](
      "accumulo.password",
      "Accumulo password",
      password = true,
      deprecatedKeys = Seq("password"),
      supportsNiFiExpressions = true)

  val KeytabPathParam =
    new GeoMesaParam[String](
      "accumulo.keytab.path",
      "Path to keytab file",
      deprecatedKeys = Seq("keytabPath", "accumulo.keytabPath"),
      supportsNiFiExpressions = true)

  val CatalogParam =
    new GeoMesaParam[String](
      "accumulo.catalog",
      "Accumulo catalog table name",
      optional = false,
      deprecatedKeys = Seq("tableName", "accumulo.tableName"),
      supportsNiFiExpressions = true)

  val RecordThreadsParam =
    new GeoMesaParam[Integer](
      "accumulo.query.record-threads",
      "The number of threads to use for record retrieval",
      default = 10,
      deprecatedKeys = Seq("recordThreads", "accumulo.recordThreads"),
      supportsNiFiExpressions = true)

  val WriteThreadsParam =
    new GeoMesaParam[Integer](
      "accumulo.write.threads",
      "The number of threads to use for writing records",
      default = 10,
      deprecatedKeys = Seq("writeThreads", "accumulo.writeThreads"),
      supportsNiFiExpressions = true)

  val RemoteArrowParam =
    new GeoMesaParam[java.lang.Boolean](
      "accumulo.remote.arrow.enable",
      "Process Arrow encoding in Accumulo tablets servers as a distributed call",
      default = java.lang.Boolean.TRUE,
      systemProperty = Some(SystemPropertyBooleanParam(AccumuloDataStoreFactory.RemoteArrowProperty))
    )

  val RemoteBinParam =
    new GeoMesaParam[java.lang.Boolean](
      "accumulo.remote.bin.enable",
      "Process binary encoding in Accumulo tablets servers as a distributed call",
      default = java.lang.Boolean.TRUE,
      systemProperty = Some(SystemPropertyBooleanParam(AccumuloDataStoreFactory.RemoteBinProperty))
    )

  val RemoteDensityParam =
    new GeoMesaParam[java.lang.Boolean](
      "accumulo.remote.density.enable",
      "Process heatmap encoding in Accumulo tablets servers as a distributed call",
      default = java.lang.Boolean.TRUE,
      systemProperty = Some(SystemPropertyBooleanParam(AccumuloDataStoreFactory.RemoteDensityProperty))
    )

  val RemoteStatsParam =
    new GeoMesaParam[java.lang.Boolean](
      "accumulo.remote.stats.enable",
      "Process statistical calculations in Accumulo tablets servers as a distributed call",
      default = java.lang.Boolean.TRUE,
      systemProperty = Some(SystemPropertyBooleanParam(AccumuloDataStoreFactory.RemoteStatsProperty))
    )
}
