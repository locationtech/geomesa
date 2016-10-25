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
import org.locationtech.geomesa.tools.accumulo.commands.DeleteCatalogCommand._
import org.locationtech.geomesa.tools.common.{OptionalForceParam, Prompt}

class DeleteCatalogCommand (parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {
  override val command = "delete-catalog"
  override val params = new DeleteCatalogParams

  override def execute() = {
    val msg = s"Delete catalog '$catalog'? (yes/no): "
    if (params.force || Prompt.confirm(msg, List("yes", "y"))) {
      ds.delete()
      println(s"Deleted catalog $catalog")
    } else {
      logger.info(s"Cancelled deletion.")
    }
    ds.dispose()
  }
}

object DeleteCatalogCommand {
  @Parameters(commandDescription = "Delete a GeoMesa catalog completely (and all features in it)")
  class DeleteCatalogParams extends GeoMesaConnectionParams
    with OptionalForceParam {}
}
