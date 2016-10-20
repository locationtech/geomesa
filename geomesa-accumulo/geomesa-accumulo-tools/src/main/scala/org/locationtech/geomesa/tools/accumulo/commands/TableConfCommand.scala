/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.client.TableNotFoundException
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.tools.accumulo.AccumuloRunner.mkSubCommand
import org.locationtech.geomesa.tools.accumulo.GeoMesaConnectionParams
import org.locationtech.geomesa.tools.accumulo.commands.TableConfCommand._
import org.locationtech.geomesa.tools.common.FeatureTypeNameParam
import org.locationtech.geomesa.utils.index.IndexMode

import scala.collection.JavaConversions._

class TableConfCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {
  override val command = "config-table"
  override val params = null
  override def register = {}

  val jcTableConf = mkSubCommand(parent, command, new TableConfParams())
  val tcList      = new ListParams
  val tcUpdate    = new UpdateParams
  val tcDesc      = new DescribeParams

  mkSubCommand(jcTableConf, ListSubCommand, tcList)
  mkSubCommand(jcTableConf, DescribeSubCommand, tcDesc)
  mkSubCommand(jcTableConf, UpdateCommand, tcUpdate)

  def execute() = {
    jcTableConf.getParsedCommand match {
      case ListSubCommand =>
        logger.info(s"Getting configuration parameters for table: ${tcList.tableName}")
        getProperties(tcList).toSeq.sortBy(_.getKey).foreach(println)

      case DescribeSubCommand =>
        logger.info(s"Finding the value for '${tcDesc.param}' on table: ${tcDesc.tableName}")
        println(getProp(tcDesc))

      case UpdateCommand =>
        val param = tcUpdate.param
        val newValue = tcUpdate.newValue
        val tableName = tcUpdate.tableName

        val property = getProp(tcUpdate)
        logger.info(s"'$param' on table '$tableName' currently set to: \n$property")

        if (newValue != property.getValue) {
          logger.info(s"Attempting to update '$param' to '$newValue'...")
          val updatedValue = setValue(tcUpdate)
          logger.info(s"'$param' on table '$tableName' is now set to: \n$updatedValue")
          println(s"Set $param=$updatedValue")
        } else {
          logger.info(s"'$param' already set to '$newValue'. No need to update.")
        }

      case _ =>
        println("Error: no config-table command listed...run as: geomesa config-table <config-table-command>")
        parent.usage(command)
    }
  }

}

object TableConfCommand {
  val ListSubCommand     = "list"
  val DescribeSubCommand = "describe"
  val UpdateCommand      = "update"

  def getProp(p: DescribeParams) = getProperties(p).find(_.getKey == p.param).getOrElse({
    throw new Exception(s"Parameter '${p.param}' not found in table: ${p.tableName}")
  })

  def setValue(p: UpdateParams) =
    try {
      p.ds.connector.tableOperations.setProperty(p.tableName, p.param, p.newValue)
      getProp(p)
    } catch {
      case e: Exception =>
        throw new Exception("Error updating the table property: " + e.getMessage, e)
    }

  def getProperties(p: ListParams) =
    try {
      p.ds.connector.tableOperations.getProperties(p.tableName)
    } catch {
      case tnfe: TableNotFoundException =>
        throw new Exception(s"Error: table ${p.tableName} could not be found: " + tnfe.getMessage, tnfe)
    }
  
  def getTableName(ds: AccumuloDataStore, params: ListParams) =
    AccumuloFeatureIndex.indices(ds.getSchema(params.featureName), IndexMode.Any)
        .find(_.name == params.tableSuffix)
        .map(ds.getTableName(params.featureName, _))
        .getOrElse(throw new Exception(s"Invalid table suffix: ${params.tableSuffix}"))
  
  @Parameters(commandDescription = "Perform table configuration operations")
  class TableConfParams {}

  @Parameters(commandDescription = "List the configuration parameters for a geomesa table")
  class ListParams extends GeoMesaConnectionParams
    with FeatureTypeNameParam {
    @Parameter(names = Array("-t", "--table-suffix"), description = "Table suffix to operate on (attr_idx, st_idx, or records)", required = true)
    var tableSuffix: String = null

    lazy val ds = DataStoreParamsHelper.createDataStore(this)
    lazy val tableName = getTableName(ds, this)
  }

  @Parameters(commandDescription = "Describe a given configuration parameter for a table")
  class DescribeParams extends ListParams {
    @Parameter(names = Array("-P", "--param"), description = "Accumulo table configuration param name (e.g. table.bloom.enabled)", required = true)
    var param: String = null
  }

  @Parameters(commandDescription = "Update a given table configuration parameter")
  class UpdateParams extends DescribeParams {
    @Parameter(names = Array("-n", "--new-value"), description = "New value of the property", required = true)
    var newValue: String = null
  }
}