/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
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

import java.util.concurrent.TimeUnit

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.BatchWriterConfig
import org.apache.accumulo.core.data.{Range => ARange}
import org.locationtech.geomesa.raster.AccumuloStoreHelper
import org.locationtech.geomesa.raster.data.GEOMESA_RASTER_BOUNDS_TABLE
import org.locationtech.geomesa.tools.AccumuloProperties
import org.locationtech.geomesa.tools.commands.DeleteRasterCommand._

import scala.collection.JavaConversions._

class DeleteRasterCommand(parent: JCommander) extends Command with Logging with AccumuloProperties {
  val command = "deleteraster"
  val params = new DeleteRasterParams
  parent.addCommand(command, params)

  override def execute(): Unit = {
    val user = params.user
    val pass = params.password
    val table = params.table
    val instance = Option(params.instance).getOrElse(instanceName)
    val zookeepers = Option(params.zookeepers).getOrElse(zookeepersProp)
    val authsParam = Option(params.auths).getOrElse("")
    val useMock = params.useMock
    lazy val accumuloConnector = AccumuloStoreHelper.buildAccumuloConnector(user, pass, instance, zookeepers, useMock)
    lazy val authProv = AccumuloStoreHelper.getAuthorizationsProvider(authsParam.split(","), accumuloConnector)
    lazy val tableOps = accumuloConnector.tableOperations()
    lazy val bwConfig: BatchWriterConfig =
      new BatchWriterConfig().setMaxWriteThreads(3).setTimeout(30L, TimeUnit.SECONDS)
    lazy val auths = authProv.getAuthorizations

    if (params.forceDelete || promptConfirm(table)) {
      //These steps are run individually in case execution
      //is interrupted or tables/rows are delete by other means
      try {
        if (tableOps.exists(table)) {
          logger.info(s"Deleting GeoMesa Raster Table: '$table'")
          tableOps.delete(table)
        }
      }
      try {
        val queryinfo = s"${table}_queries"
        if (tableOps.exists(queryinfo)) {
          logger.info(s"Deleting Query logs table: '$queryinfo'")
          tableOps.delete(queryinfo)
        }
      }
      try {
        if (tableOps.exists(GEOMESA_RASTER_BOUNDS_TABLE)) {
          logger.info(s"Removing Metadata for table: '$table'")
          val deleter = accumuloConnector.createBatchDeleter(GEOMESA_RASTER_BOUNDS_TABLE, auths, 3, bwConfig)
          val deleteRange = new ARange(s"${table}_bounds")
          deleter.setRanges(Seq(deleteRange))
          deleter.delete()
          deleter.close()
        }
      }
    } else {
      logger.info(s"Cancelled deletion of GeoMesa Raster Table: '$table'")
    }
  }
}


object DeleteRasterCommand {
  val Command = "deleteraster"

  def promptConfirm(store: String) =
    if (System.console() != null) {
      print(s"Delete Raster Store Table: '$store'? (yes/no): ")
      System.console().readLine().toLowerCase().trim == "yes"
    } else {
      throw new IllegalStateException("Unable to confirm feature deletion via console..." +
        "Please ensure stdout is not redirected or --force flag is set")
    }

  @Parameters(commandDescription = "Delete a GeoMesa Raster Store")
  class DeleteRasterParams extends RasterParams {
    @Parameter(names = Array("-f", "--force"), description = "Force deletion of feature without prompt", required = false)
    var forceDelete: Boolean = false
  }

}