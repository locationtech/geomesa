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

import scala.collection.mutable.ArrayBuffer

abstract class GeoMesaArgs(val args: Array[String]) extends ReverseParsable {
  def parse(): Unit = new JCommander(this, args: _*)
}

trait ReverseParsable {
  def unparse(): Array[String]
}

trait InputDataStoreArgs extends ReverseParsable {

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

  override def unparse(): Array[String] = {
    val buf = ArrayBuffer.empty[String]
    if (inUser != null) {
      buf.append("--geomesa.input.user", inUser)
    }
    if (inPassword != null) {
      buf.append("--geomesa.input.password", inPassword)
    }
    if (inInstanceId != null) {
      buf.append("--geomesa.input.instanceId", inInstanceId)
    }
    if (inZookeepers != null) {
      buf.append("--geomesa.input.zookeepers", inZookeepers)
    }
    if (inTableName != null) {
      buf.append("--geomesa.input.tableName", inTableName)
    }
    buf.toArray
  }
}

trait InputFeatureArgs extends ReverseParsable {

  @Parameter(names = Array("--geomesa.input.feature"), description = "Simple feature type name", required = true)
  var inFeature: String = null

  override def unparse(): Array[String] = {
    if (inFeature != null) {
      Array("--geomesa.input.feature", inFeature)
    } else {
      Array.empty
    }
  }
}

trait OutputDataStoreArgs extends ReverseParsable {

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

  override def unparse(): Array[String] = {
    val buf = ArrayBuffer.empty[String]
    if (outUser != null) {
      buf.append("--geomesa.output.user", outUser)
    }
    if (outPassword != null) {
      buf.append("--geomesa.output.password", outPassword)
    }
    if (outInstanceId != null) {
      buf.append("--geomesa.output.instanceId", outInstanceId)
    }
    if (outZookeepers != null) {
      buf.append("--geomesa.output.zookeepers", outZookeepers)
    }
    if (outTableName != null) {
      buf.append("--geomesa.output.tableName", outTableName)
    }
    buf.toArray
  }
}

trait OutputFeatureArgs extends ReverseParsable {

  @Parameter(names = Array("--geomesa.output.feature"), description = "Simple feature type name", required = true)
  var outFeature: String = null

  override def unparse(): Array[String] = {
    if (outFeature != null) {
      Array("--geomesa.output.feature", outFeature)
    } else {
      Array.empty
    }
  }
}

trait OutputFeatureOptionalArgs extends ReverseParsable {

  @Parameter(names = Array("--geomesa.output.feature"), description = "Simple feature type name")
  var outFeature: String = null

  override def unparse(): Array[String] = {
    if (outFeature != null) {
      Array("--geomesa.output.feature", outFeature)
    } else {
      Array.empty
    }
  }
}

trait InputCqlArgs extends ReverseParsable {

  @Parameter(names = Array("--geomesa.input.cql"), description = "CQL query filter")
  var inCql: String = null

  override def unparse(): Array[String] = {
    if (inCql != null) {
      Array("--geomesa.input.cql", inCql)
    } else {
      Array.empty
    }
  }
}