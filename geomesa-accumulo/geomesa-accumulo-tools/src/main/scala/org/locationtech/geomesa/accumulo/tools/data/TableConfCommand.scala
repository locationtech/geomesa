/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.data

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import org.apache.accumulo.core.client.TableNotFoundException
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.accumulo.index.AccumuloFeatureIndex
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.tools.{Command, CommandWithSubCommands, RequiredTypeNameParam, Runner}
import org.locationtech.geomesa.utils.index.IndexMode

import scala.collection.JavaConversions._

class TableConfCommand(val runner: Runner, val jc: JCommander) extends CommandWithSubCommands {

  import TableConfCommand._

  override val name = "config-table"
  override val params = new TableConfParams()

  override val subCommands: Seq[Command] =
    Seq(new TableConfListCommand, new TableConfDescribeCommand, new TableConfUpdateCommand)
}

class TableConfListCommand extends AccumuloDataStoreCommand {

  import TableConfCommand._

  override val name = "list"
  override val params = new ListParams

  override def execute(): Unit = {
    Command.user.info(s"Getting configuration parameters for table: ${params.tableSuffix}")
    withDataStore((ds) => getProperties(ds, params).toSeq.sortBy(_.getKey).foreach(p => Command.output.info(p.toString)))
  }
}

class TableConfDescribeCommand extends AccumuloDataStoreCommand {

  import TableConfCommand._

  override val name = "describe"
  override val params = new DescribeParams

  override def execute(): Unit = {
    Command.user.info(s"Finding the value for '${params.param}' on table: ${params.tableSuffix}")
    withDataStore((ds) => Command.output.info(getProp(ds, params).toString))
  }
}

class TableConfUpdateCommand extends AccumuloDataStoreCommand {

  import TableConfCommand._

  override val name = "update"
  override val params = new UpdateParams

  override def execute(): Unit = {
    val param = params.param
    val newValue = params.newValue
    val tableName = params.tableSuffix

    withDataStore { (ds) =>
      val property = getProp(ds, params)
      Command.user.info(s"'$param' on table '$tableName' currently set to: \n$property")

      if (newValue != property.getValue) {
        Command.user.info(s"Attempting to update '$param' to '$newValue'...")
        val updatedValue = setValue(ds, params)
        Command.user.info(s"'$param' on table '$tableName' is now set to: \n$updatedValue")
      } else {
        Command.user.info(s"'$param' already set to '$newValue'. No need to update.")
      }
    }
  }
}

object TableConfCommand {

  def getProp(ds: AccumuloDataStore, params: DescribeParams) = getProperties(ds, params).find(_.getKey == params.param).getOrElse {
    throw new Exception(s"Parameter '${params.param}' not found in table: ${params.tableSuffix}")
  }

  def setValue(ds: AccumuloDataStore, params: UpdateParams) =
    try {
      ds.connector.tableOperations.setProperty(getTableName(ds, params), params.param, params.newValue)
      getProp(ds, params)
    } catch {
      case e: Exception =>
        throw new Exception("Error updating the table property: " + e.getMessage, e)
    }

  def getProperties(ds: AccumuloDataStore, params: ListParams) =
    try {
      ds.connector.tableOperations.getProperties(getTableName(ds, params))
    } catch {
      case tnfe: TableNotFoundException =>
        throw new Exception(s"Error: table ${params.tableSuffix} could not be found: " + tnfe.getMessage, tnfe)
    }
  
  def getTableName(ds: AccumuloDataStore, params: ListParams) =
    AccumuloFeatureIndex.indices(ds.getSchema(params.featureName), IndexMode.Any)
        .find(_.name == params.tableSuffix)
        .map(_.getTableName(params.featureName, ds))
        .getOrElse(throw new Exception(s"Invalid table suffix: ${params.tableSuffix}"))
  
  @Parameters(commandDescription = "Perform table configuration operations")
  class TableConfParams {}

  @Parameters(commandDescription = "List the configuration parameters for a geomesa table")
  class ListParams extends AccumuloDataStoreParams with RequiredTypeNameParam {
    @Parameter(names = Array("-t", "--table-suffix"), description = "Table suffix to operate on (attr_idx, st_idx, or records)", required = true)
    var tableSuffix: String = null
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
