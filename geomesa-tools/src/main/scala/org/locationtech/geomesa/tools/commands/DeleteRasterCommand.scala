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

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.data.{Range => ARange}
import org.locationtech.geomesa.raster.data.AccumuloRasterStore
import org.locationtech.geomesa.tools.AccumuloProperties
import org.locationtech.geomesa.tools.commands.DeleteRasterCommand._

class DeleteRasterCommand(parent: JCommander) extends Command(parent) with Logging with AccumuloProperties {
  override val command = "deleteraster"
  override val params = new DeleteRasterParams()

  override def execute(): Unit = {
    val user = params.user
    val pass = params.password
    val table = params.table
    val instance = Option(params.instance).getOrElse(instanceName)
    val zookeepers = Option(params.zookeepers).getOrElse(zookeepersProp)
    val authsParam = Option(params.auths).getOrElse("")
    val visibilities = Option(params.visibilities).getOrElse("")
    val useMock = params.useMock

    lazy val RasterStore =
      AccumuloRasterStore(user, pass, instance, zookeepers, table, authsParam, visibilities, useMock)

    if (params.forceDelete || promptConfirm(table)) {
      RasterStore.deleteRasterTable()
    } else {
      logger.info(s"Cancelled deletion of GeoMesa Raster Table: '$table'")
    }
  }
}


object DeleteRasterCommand {
  def promptConfirm(store: String) =
    if (System.console() != null) {
      print(s"Delete Raster Store Table: '$store'? (yes/no): ")
      System.console().readLine().toLowerCase().trim == "yes"
    } else {
      throw new IllegalStateException("Unable to confirm feature deletion via console..." +
        "Please ensure stdout is not redirected or --force flag is set")
    }

  @Parameters(commandDescription = "Delete a GeoMesa Raster Table")
  class DeleteRasterParams extends RasterParams {
    @Parameter(names = Array("-f", "--force"), description = "Force deletion of feature without prompt", required = false)
    var forceDelete: Boolean = false
  }
}