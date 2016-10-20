/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.tools.accumulo.GeoMesaConnectionParams
import org.locationtech.geomesa.tools.accumulo.commands.GetSftConfigCommand._
import org.locationtech.geomesa.tools.common.FeatureTypeNameParam
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

class GetSftConfigCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {
  override val command = "get-sft-config"
  override val params = new GetSftParameters

  override def execute() = {
    logger.info(s"Getting SFT for feature ${params.featureName} from catalog $catalog")
    try {
      params.format.toLowerCase match {
        case "typesafe" =>
          println(SimpleFeatureTypes.toConfigString(ds.getSchema(params.featureName), !params.excludeUserData, params.concise))
        case "spec" =>
          println(SimpleFeatureTypes.encodeType(ds.getSchema(params.featureName), !params.excludeUserData))
        case _ =>
          logger.error(s"Unknown config format: ${params.format}")
      }
    } catch {
      case npe: NullPointerException =>
        logger.error(s"Error: feature '${params.featureName}' not found. Check arguments...", npe)
      case e: Exception =>
        logger.error(s"Error describing feature '${params.featureName}':", e)
    } finally {
      ds.dispose()
    }
  }

}

object GetSftConfigCommand {
  @Parameters(commandDescription = "Get the SimpleFeatureType of a feature")
  class GetSftParameters extends GeoMesaConnectionParams with FeatureTypeNameParam {
    @Parameter(names = Array("--concise"), description = "Render in concise format", required = false)
    var concise: Boolean = false

    @Parameter(names = Array("--format"), description = "Formats for sft (comma separated string, allowed values are typesafe, spec)", required = false)
    var format: String = "typesafe"

    @Parameter(names = Array("--exclude-user-data"), description = "Exclude user data", required = false)
    var excludeUserData: Boolean = false
  }
}

