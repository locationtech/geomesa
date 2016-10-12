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
import org.locationtech.geomesa.tools.accumulo.GeoMesaConnectionParams
import org.locationtech.geomesa.tools.accumulo.commands.GetNamesCommand._

class GetNamesCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {
  override val command = "get-names"
  override val params = new ListParameters()

  override def execute() = {
    logger.info("Running List Features on catalog " + params.catalog)
    ds.getTypeNames.foreach(println)
    ds.dispose()
  }

}

object GetNamesCommand {
  @Parameters(commandDescription = "List GeoMesa feature types for a given catalog")
  class ListParameters extends GeoMesaConnectionParams {}
}
