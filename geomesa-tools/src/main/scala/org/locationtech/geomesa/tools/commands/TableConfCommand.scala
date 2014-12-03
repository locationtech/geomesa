/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.TableNotFoundException
import org.apache.accumulo.core.client.admin.TableOperations
import org.locationtech.geomesa.core.data.{AccumuloDataStore, TableSuffix}
import org.locationtech.geomesa.tools.DataStoreHelper
import org.locationtech.geomesa.tools.Runner.mkSubCommand
import org.locationtech.geomesa.tools.commands.TableConfCommand._

import scala.collection.JavaConversions._

class TableConfCommand(parent: JCommander) extends Command with Logging {

  val jcTableConf    = mkSubCommand(parent, Command, new TableConfParams())
  val tcListParams   = new ListParams
  val tcUpdateParams = new UpdateParams
  val tcDescParams   = new DescribeParams

  mkSubCommand(jcTableConf, ListSubCommand, tcListParams)
  mkSubCommand(jcTableConf, DescribeSubCommand, tcDescParams)
  mkSubCommand(jcTableConf, UpdateCommand, tcUpdateParams)

  def execute() = {
    jcTableConf.getParsedCommand match {
      case ListSubCommand =>
        implicit val ds = new DataStoreHelper(tcListParams).ds
        implicit val tableOps = ds.connector.tableOperations()
        implicit val tableName = getTableName(tcListParams)

        logger.info(s"Gathering the configuration parameters for table: $tableName")
        getProperties().foreach(println)

      case DescribeSubCommand =>
        implicit val ds = new DataStoreHelper(tcDescParams).ds
        implicit val tableOps = ds.connector.tableOperations()
        implicit val tableName = getTableName(tcDescParams)

        logger.info(s"Finding the value for '${tcDescParams.param}' on table: $tableName")
        val prop = getProp(tcDescParams.param)
        if (prop.nonEmpty) {
          println(prop)
        } else {
          throw new Exception(s"Parameter '${tcDescParams.param}' not found in table: $tableName")
        }

      case UpdateCommand =>
        implicit val ds = new DataStoreHelper(tcUpdateParams).ds
        implicit val tableOps = ds.connector.tableOperations()
        implicit val tableName = getTableName(tcUpdateParams)
        val param = tcUpdateParams.param
        val newValue = tcUpdateParams.newValue

        val property = getProp(param).get
        logger.info(s"'$param' on table '$tableName' currently set to: \n$property")

        if (newValue != property.getValue) {
          logger.info(s"Attempting to update '$param' to '$newValue'...")
          setValue(param, newValue)
          println(s"Set $param=$newValue")
        } else {
          logger.info(s"'$param' already set to '$newValue'. No need to update.")
        }

      case _ =>
        println("Error: no tableconf command listed...run as: geomesa tableconf <tableconf-command>")
        parent.usage(Command)
    }
  }

  def getProp(param: String)(implicit tableOps: TableOperations, tableName: String) =
    getProperties().find(_.getKey == param)

  def setValue(param: String, newValue: String)(implicit tableOps: TableOperations, tableName: String) =
    try {
      tableOps.setProperty(tableName, param, newValue)
      val updatedProperty = getProp(param).get
      logger.info(s"'$param' on table '$tableName' is now set to: \n$updatedProperty")
    } catch {
      case e: Exception =>
        throw new Exception("Error updating the table property: " + e.getMessage, e)
    }

  def getProperties()(implicit tableOps: TableOperations, tableName: String) =
    try {
      tableOps.getProperties(tableName)
    } catch {
      case tnfe: TableNotFoundException =>
        throw new Exception(s"Error: table $tableName could not be found: "+tnfe.getMessage, tnfe)
    }

  def getTableName(params: ListParams)(implicit ds: AccumuloDataStore) =
    params.tableSuffix match {
      case TableSuffix.STIdx   => ds.getSpatioTemporalIdxTableName(params.featureName)
      case TableSuffix.AttrIdx => ds.getAttrIdxTableName(params.featureName)
      case TableSuffix.Records => ds.getRecordTableForType(params.featureName)
      case _                   => throw new Exception(s"Invalid table suffix: ${params.tableSuffix}")
    }

}

object TableConfCommand {
  val Command            = "tableconf"
  val ListSubCommand     = "list"
  val DescribeSubCommand = "describe"
  val UpdateCommand      = "update"

  @Parameters(commandDescription = "Perform table configuration operations")
  class TableConfParams {}

  @Parameters(commandDescription = "List the configuration parameters for a geomesa table")
  class ListParams extends FeatureParams {
    @Parameter(names = Array("-t", "--table-suffix"), description = "Table suffix to operate on (attr_idx, st_idx, or records)", required = true)
    var tableSuffix: String = null
  }

  @Parameters(commandDescription = "Describe a given configuration parameter for a table")
  class DescribeParams extends ListParams {
    @Parameter(names = Array("--param"), description = "Accumulo table configuration param name (e.g. table.bloom.enabled)", required = true)
    var param: String = null
  }

  @Parameters(commandDescription = "Update a given table configuration parameter")
  class UpdateParams extends DescribeParams {
    @Parameter(names = Array("-n", "--new-value"), description = "New value of the property", required = true)
    var newValue: String = null
  }
}