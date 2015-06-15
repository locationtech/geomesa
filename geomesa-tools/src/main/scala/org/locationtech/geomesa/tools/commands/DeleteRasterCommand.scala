/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

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