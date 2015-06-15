/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.commands

import com.beust.jcommander.{JCommander, Parameters, ParametersDelegate}
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.tools.commands.DeleteCatalogCommand._

class DeleteCatalogCommand (parent: JCommander) extends CommandWithCatalog(parent) with Logging {
  override val command = "deletecatalog"
  override val params = new DeleteCatalogParams

  override def execute() = {
    val msg = s"Delete catalog '$catalog'? (yes/no): "
    if (params.force || PromptConfirm.confirm(msg, List("yes", "y"))) {
      ds.delete()
      println(s"Deleted catalog $catalog")
    } else {
      logger.info(s"Cancelled deletion.")
    }
  }
}

object DeleteCatalogCommand {

  @Parameters(commandDescription = "Delete a GeoMesa catalog completely (and all features in it)")
  class DeleteCatalogParams extends GeoMesaParams {
    @ParametersDelegate
    val forceParams = new ForceParams
    def force = forceParams.force
  }

}
