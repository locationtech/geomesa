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
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.tools.commands.ExplainCommand.ExplainParameters

class ExplainCommand(parent: JCommander) extends CommandWithCatalog(parent) with Logging {
  override val command = "explain"
  override val params = new ExplainParameters()

  override def execute() =
    try {
      val q = new Query(params.featureName, ECQL.toFilter(params.cqlFilter))
      ds.explainQuery(q)
    } catch {
      case e: Exception =>
        logger.error(s"Error: Could not explain the query (${params.cqlFilter}): ${e.getMessage}", e)
    }

}

object ExplainCommand {
  @Parameters(commandDescription = "Explain how a GeoMesa query will be executed")
  class ExplainParameters extends RequiredCqlFilterParameters {}
}
