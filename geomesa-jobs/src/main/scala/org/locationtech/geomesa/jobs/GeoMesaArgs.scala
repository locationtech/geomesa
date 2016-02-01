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

trait InputDataStoreArgs {
  @Parameter(names = Array("--geomesa.input.user"), description = "Accumulo user name", required = true)
  var inUser: String = null

  @Parameter(names = Array("--geomesa.input.password"), description = "Accumulo password", required = true)
  var inPassword: String = null

  @Parameter(names = Array("--geomesa.input.instanceId"), description = "Accumulo instance name", required = true)
  var inInstanceId: String = null

  @Parameter(names = Array("--geomesa.input.zookeepers"), description = "Zookeepers (host[:port], comma separated)", required = true)
  var inZookeepers: String = null

  @Parameter(names = Array("--geomesa.input.tableName"), description = "Accumulo catalog table name", required = true)
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
  @Parameter(names = Array("--geomesa.input.feature"), description = "Simple feature type name", required = true)
  var inFeature: String = null
}

trait OutputDataStoreArgs {
  @Parameter(names = Array("--geomesa.output.user"), description = "Accumulo user name", required = true)
  var outUser: String = null

  @Parameter(names = Array("--geomesa.output.password"), description = "Accumulo password", required = true)
  var outPassword: String = null

  @Parameter(names = Array("--geomesa.output.instanceId"), description = "Accumulo instance name", required = true)
  var outInstanceId: String = null

  @Parameter(names = Array("--geomesa.output.zookeepers"), description = "Zookeepers (host[:port], comma separated)", required = true)
  var outZookeepers: String = null

  @Parameter(names = Array("--geomesa.output.tableName"), description = "Accumulo catalog table name", required = true)
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
  @Parameter(names = Array("--geomesa.output.feature"), description = "Simple feature type name", required = true)
  var outFeature: String = null
}

trait OutputFeatureOptionalArgs {
  @Parameter(names = Array("--geomesa.output.feature"), description = "Simple feature type name")
  var outFeature: String = null
}

trait InputCqlArgs {
  @Parameter(names = Array("--geomesa.input.cql"), description = "CQL query filter")
  var inCql: String = null
}