/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.tools.commands.ListCommand._

class ListCommand(parent: JCommander) extends CommandWithCatalog(parent) with Logging {
  override val command = "list"
  override val params = new ListParameters()

  override def execute() = {
    logger.info("Running List Features on catalog "+params.catalog)
    ds.getTypeNames.foreach(println)
  }

}

object ListCommand {
  @Parameters(commandDescription = "List GeoMesa features for a given catalog")
  class ListParameters extends GeoMesaParams {}
}
