/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * Portions Crown Copyright (c) 2016-2021 Dstl
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.jobs.accumulo

import com.beust.jcommander.{JCommander, Parameter}
import org.locationtech.geomesa.accumulo.data.AccumuloDataStoreParams

import scala.collection.mutable.ArrayBuffer

abstract class GeoMesaArgs(val args: Array[String]) extends ReverseParsable {
  def parse(): Unit = new JCommander(this, args: _*)
}

object GeoMesaArgs {
  final val InputUser        = "--geomesa.input.user"
  final val InputPassword    = "--geomesa.input.password"
  final val InputKeytabPath  = "--geomesa.input.keytabPath"
  final val InputInstanceId  = "--geomesa.input.instanceId"
  final val InputZookeepers  = "--geomesa.input.zookeepers"
  final val InputTableName   = "--geomesa.input.tableName"
  final val InputFeatureName = "--geomesa.input.feature"
  final val InputCQL         = "--geomesa.input.cql"
  final val OutputUser       = "--geomesa.output.user"
  final val OutputPassword   = "--geomesa.output.password"
  final val OutputInstanceId = "--geomesa.output.instanceId"
  final val OutputZookeepers = "--geomesa.output.zookeepers"
  final val OutputTableName  = "--geomesa.output.tableName"
  final val OutputFeature    = "--geomesa.output.feature"
  final val OutputHdfs       = "--geomesa.output.hdfs"
}

trait ReverseParsable {
  def unparse(): Array[String]
}

trait InputDataStoreArgs extends ReverseParsable {

  @Parameter(names = Array(GeoMesaArgs.InputUser), description = "Accumulo user name", required = true)
  var inUser: String = null

  @Parameter(names = Array(GeoMesaArgs.InputPassword), description = "Accumulo password")
  var inPassword: String = null

  @Parameter(names = Array(GeoMesaArgs.InputKeytabPath), description = "Accumulo Kerberos keytab path")
  var inKeytabPath: String = null

  @Parameter(names = Array(GeoMesaArgs.InputInstanceId), description = "Accumulo instance name", required = true)
  var inInstanceId: String = null

  @Parameter(names = Array(GeoMesaArgs.InputZookeepers), description = "Zookeepers (host[:port], comma separated)", required = true)
  var inZookeepers: String = null

  @Parameter(names = Array(GeoMesaArgs.InputTableName), description = "Accumulo catalog table name", required = true)
  var inTableName: String = null

  def inDataStore: Map[String, String] = Map(
    AccumuloDataStoreParams.UserParam.getName       -> inUser,
    AccumuloDataStoreParams.PasswordParam.getName   -> inPassword,
    AccumuloDataStoreParams.KeytabPathParam.getName -> inKeytabPath,
    AccumuloDataStoreParams.InstanceIdParam.getName -> inInstanceId,
    AccumuloDataStoreParams.ZookeepersParam.getName -> inZookeepers,
    AccumuloDataStoreParams.CatalogParam.getName    -> inTableName
  )

  override def unparse(): Array[String] = {
    val buf = ArrayBuffer.empty[String]
    if (inUser != null) {
      buf.append(GeoMesaArgs.InputUser, inUser)
    }
    if (inPassword != null) {
      buf.append(GeoMesaArgs.InputPassword, inPassword)
    }
    if (inKeytabPath != null) {
      buf.append(GeoMesaArgs.InputKeytabPath, inKeytabPath)
    }
    if (inInstanceId != null) {
      buf.append(GeoMesaArgs.InputInstanceId, inInstanceId)
    }
    if (inZookeepers != null) {
      buf.append(GeoMesaArgs.InputZookeepers, inZookeepers)
    }
    if (inTableName != null) {
      buf.append(GeoMesaArgs.InputTableName, inTableName)
    }
    buf.toArray
  }
}

trait InputFeatureArgs extends ReverseParsable {

  @Parameter(names = Array(GeoMesaArgs.InputFeatureName), description = "Simple feature type name", required = true)
  var inFeature: String = null

  override def unparse(): Array[String] = {
    if (inFeature != null) {
      Array(GeoMesaArgs.InputFeatureName, inFeature)
    } else {
      Array.empty
    }
  }
}

trait OutputDataStoreArgs extends ReverseParsable {

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
    AccumuloDataStoreParams.UserParam.getName       -> outUser,
    AccumuloDataStoreParams.PasswordParam.getName   -> outPassword,
    AccumuloDataStoreParams.InstanceIdParam.getName -> outInstanceId,
    AccumuloDataStoreParams.ZookeepersParam.getName -> outZookeepers,
    AccumuloDataStoreParams.CatalogParam.getName    -> outTableName
  )

  override def unparse(): Array[String] = {
    val buf = ArrayBuffer.empty[String]
    if (outUser != null) {
      buf.append(GeoMesaArgs.OutputUser, outUser)
    }
    if (outPassword != null) {
      buf.append(GeoMesaArgs.OutputPassword, outPassword)
    }
    if (outInstanceId != null) {
      buf.append(GeoMesaArgs.OutputInstanceId, outInstanceId)
    }
    if (outZookeepers != null) {
      buf.append(GeoMesaArgs.OutputZookeepers, outZookeepers)
    }
    if (outTableName != null) {
      buf.append(GeoMesaArgs.OutputTableName, outTableName)
    }
    buf.toArray
  }
}

trait OutputFeatureArgs extends ReverseParsable {

  @Parameter(names = Array(GeoMesaArgs.OutputFeature), description = "Simple feature type name", required = true)
  var outFeature: String = null

  override def unparse(): Array[String] = {
    if (outFeature != null) {
      Array(GeoMesaArgs.OutputFeature, outFeature)
    } else {
      Array.empty
    }
  }
}

trait OutputFeatureOptionalArgs extends ReverseParsable {

  @Parameter(names = Array(GeoMesaArgs.OutputFeature), description = "Simple feature type name")
  var outFeature: String = null

  override def unparse(): Array[String] = {
    if (outFeature != null) {
      Array(GeoMesaArgs.OutputFeature, outFeature)
    } else {
      Array.empty
    }
  }
}

trait InputCqlArgs extends ReverseParsable {

  @Parameter(names = Array(GeoMesaArgs.InputCQL), description = "CQL query filter")
  var inCql: String = null

  override def unparse(): Array[String] = {
    if (inCql != null) {
      Array(GeoMesaArgs.InputCQL, inCql)
    } else {
      Array.empty
    }
  }
}

trait OutputHdfsArgs extends ReverseParsable {

  @Parameter(names = Array(GeoMesaArgs.OutputHdfs), description = "HDFS path", required = true)
  var outHdfs: String = null

  override def unparse(): Array[String] = {
    if (outHdfs != null) {
      Array(GeoMesaArgs.OutputHdfs, outHdfs)
    } else {
      Array.empty
    }
  }
}