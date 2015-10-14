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
import GetSftCommand._
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

class GetSftCommand(parent: JCommander) extends CommandWithCatalog(parent) with Logging {
  override val command = "getsft"
  override val params = new GetSftParameters

  override def execute() = {
    logger.info(s"Getting SFT for feature ${params.featureName} from catalog ${catalog}")
    try {
      println(SimpleFeatureTypes.encodeType(ds.getSchema(params.featureName)))
    } catch {
    case npe: NullPointerException =>
      logger.error(s"Error: feature '${params.featureName}' not found. Check arguments...", npe)
    case e: Exception =>
      logger.error(s"Error describing feature '${params.featureName}': " + e.getMessage, e)
    }
  }

}

object GetSftCommand {
  @Parameters(commandDescription = "Get the SimpleFeatureType of a feature")
  class GetSftParameters extends FeatureParams {}
}

