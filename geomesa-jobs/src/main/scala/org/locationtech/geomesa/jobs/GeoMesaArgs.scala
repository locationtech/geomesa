/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.jobs

import com.beust.jcommander.{JCommander, Parameter}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams

class GeoMesaArgs(val args: Array[String]) {
  def parse(): Unit = new JCommander(this, args: _*)
}

object GeoMesaArgs {
  final val InputUser = "--geomesa.input.user"
  final val InputPassword = "--geomesa.input.password"
  final val InputInstanceId = "--geomesa.input.instanceId"
  final val InputZookeepers = "--geomesa.input.zookeepers"
  final val InputTableName = "--geomesa.input.tableName"
  final val InputFeatureName = "--geomesa.input.feature"
  final val InputCQL = "--geomesa.input.cql"
  final val OutputUser = "--geomesa.output.user"
  final val OutputPassword = "--geomesa.output.password"
  final val OutputInstanceId = "--geomesa.output.instanceId"
  final val OutputZookeepers = "--geomesa.output.zookeepers"
  final val OutputTableName = "--geomesa.output.tableName"
  final val OutputFeature = "--geomesa.output.feature"
}

trait InputDataStoreArgs {
  @Parameter(names = Array(GeoMesaArgs.InputUser), description = "Accumulo user name", required = true)
  var inUser: String = null

  @Parameter(names = Array(GeoMesaArgs.InputPassword), description = "Accumulo password", required = true)
  var inPassword: String = null

  @Parameter(names = Array(GeoMesaArgs.InputInstanceId), description = "Accumulo instance name", required = true)
  var inInstanceId: String = null

  @Parameter(names = Array(GeoMesaArgs.InputZookeepers), description = "Zookeepers (host[:port], comma separated)", required = true)
  var inZookeepers: String = null

  @Parameter(names = Array(GeoMesaArgs.InputTableName), description = "Accumulo catalog table name", required = true)
  var inTableName: String = null

  def inDataStore: Map[String, String] = Map(
    AccumuloDataStoreParams.userParam.getName       -> inUser,
    AccumuloDataStoreParams.passwordParam.getName   -> inPassword,
    AccumuloDataStoreParams.instanceIdParam.getName -> inInstanceId,
    AccumuloDataStoreParams.zookeepersParam.getName -> inZookeepers,
    AccumuloDataStoreParams.tableNameParam.getName  -> inTableName
  )
}

trait InputFeatureArgs {
  @Parameter(names = Array(GeoMesaArgs.InputFeatureName), description = "Simple feature type name", required = true)
  var inFeature: String = null
}

trait OutputDataStoreArgs {
  @Parameter(names = Array(GeoMesaArgs.OutputUser), description = "Accumulo user name", required = true)
  var outUser: String = null

  @Parameter(names = Array(GeoMesaArgs.OutputPassword), description = "Accumulo password", required = true)
  var outPassword: String = null

  @Parameter(names = Array(GeoMesaArgs.OutputInstanceId), description = "Accumulo instance name", required = true)
  var outInstanceId: String = null

  @Parameter(names = Array(GeoMesaArgs.OutputZookeepers), description = "Zookeepers (host[:port], comma separated)", required = true)
  var outZookeepers: String = null

  @Parameter(names = Array(GeoMesaArgs.OutputTableName), description = "Accumulo catalog table name", required = true)
  var outTableName: String = null

  def outDataStore: Map[String, String] = Map(
    AccumuloDataStoreParams.userParam.getName       -> outUser,
    AccumuloDataStoreParams.passwordParam.getName   -> outPassword,
    AccumuloDataStoreParams.instanceIdParam.getName -> outInstanceId,
    AccumuloDataStoreParams.zookeepersParam.getName -> outZookeepers,
    AccumuloDataStoreParams.tableNameParam.getName  -> outTableName
  )
}

trait OutputFeatureArgs {
  @Parameter(names = Array(GeoMesaArgs.OutputFeature), description = "Simple feature type name", required = true)
  var outFeature: String = null
}

trait OutputFeatureOptionalArgs {
  @Parameter(names = Array(GeoMesaArgs.OutputFeature), description = "Simple feature type name")
  var outFeature: String = null
}

trait InputCqlArgs {
  @Parameter(names = Array(GeoMesaArgs.InputCQL), description = "CQL query filter")
  var inCql: String = null
}