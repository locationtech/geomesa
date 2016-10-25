/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.accumulo.core.data.{Range => ARange}
import org.locationtech.geomesa.raster.data.AccumuloRasterStore
import org.locationtech.geomesa.tools.accumulo.commands.DeleteRasterCommand._
import org.locationtech.geomesa.tools.accumulo.{AccumuloConnectionParams, AccumuloRasterTableParam}
import org.locationtech.geomesa.tools.common.OptionalForceParam
import org.locationtech.geomesa.tools.common.commands.Command

class DeleteRasterCommand(parent: JCommander) extends Command(parent) with LazyLogging {
  override val command = "delete-raster"
  override val params = new DeleteRasterParams()

  override def execute(): Unit = {
    val user = params.user
    val pass = params.password
    val table = params.table
    val instance = params.instance
    val zookeepers = params.zookeepers
    val authsParam = Option(params.auths).getOrElse("")
    val visibilities = Option(params.visibilities).getOrElse("")
    val useMock = params.useMock

    lazy val RasterStore =
      AccumuloRasterStore(user, pass, instance, zookeepers, table, authsParam, visibilities, useMock)

    if (params.force || promptConfirm(table)) {
      RasterStore.deleteRasterTable()
    } else {
      logger.info(s"Cancelled deletion of GeoMesa Raster Table: '$table'")
    }
    RasterStore.close()
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

  @Parameters(commandDescription = "Delete a GeoMesa Raster table")
  class DeleteRasterParams extends AccumuloConnectionParams
    with AccumuloRasterTableParam
    with OptionalForceParam {}
}