/*
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.locationtech.geomesa.accumulo.data


import com.google.common.collect.ImmutableMap
import org.apache.accumulo.core.client.Connector
import org.geotools.data.DataAccessFactory.Param
import org.geotools.data.Parameter
import org.locationtech.geomesa.index.geotools.GeoMesaDataStoreFactory.GeoMesaDataStoreParams
import org.locationtech.geomesa.security.SecurityParams
import org.locationtech.geomesa.utils.geotools.GeoMesaParam

// keep params in a separate object so we don't require accumulo classes on the build path to access it
object AccumuloDataStoreParams extends GeoMesaDataStoreParams with SecurityParams {

  val ConnectorParam =
    new GeoMesaParam[Connector](
      "accumulo.connector",
      "Accumulo connector",
      deprecatedKeys = Seq("connector"))

  val InstanceIdParam =
    new GeoMesaParam[String](
      "accumulo.instance.id",
      "Accumulo Instance ID",
      optional = false,
      deprecatedKeys = Seq("instanceId", "accumulo.instanceId"),
      supportsNiFiExpressions = true)

  val ZookeepersParam =
    new GeoMesaParam[String](
      "accumulo.zookeepers",
      "Zookeepers",
      optional = false,
      deprecatedKeys = Seq("zookeepers"),
      supportsNiFiExpressions = true)

  val UserParam =
    new GeoMesaParam[String](
      "accumulo.user",
      "Accumulo user",
      optional = false,
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

  // used to handle geoserver password encryption in persisted ds params
  val DeprecatedGeoServerPasswordParam =
    new Param(
      "password",
      classOf[String],
      "",
      false,
      null,
      ImmutableMap.of(Parameter.DEPRECATED, true, Parameter.IS_PASSWORD, true))
}
