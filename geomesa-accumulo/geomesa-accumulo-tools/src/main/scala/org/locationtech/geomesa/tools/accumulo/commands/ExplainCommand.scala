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
import org.geotools.data.Query
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.index.utils.ExplainPrintln
import org.locationtech.geomesa.tools.accumulo.GeoMesaConnectionParams
import org.locationtech.geomesa.tools.accumulo.Utils.setOverrideAttributes
import org.locationtech.geomesa.tools.accumulo.commands.ExplainCommand.ExplainParameters
import org.locationtech.geomesa.tools.common.{CQLFilterParam, FeatureTypeNameParam, OptionalAttributesParam}

class ExplainCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {
  override val command = "explain"
  override val params = new ExplainParameters()

  override def execute() =
    try {
      val q = new Query(params.featureName, ECQL.toFilter(params.cqlFilter))
      setOverrideAttributes(q, Option(params.attributes))
      ds.getQueryPlan(q, explainer = new ExplainPrintln)
    } catch {
      case e: Exception =>
        logger.error(s"Error: Could not explain the query (${params.cqlFilter}): ${e.getMessage}", e)
    } finally {
      ds.dispose()
    }

}

object ExplainCommand {
  @Parameters(commandDescription = "Explain how a GeoMesa query will be executed")
  class ExplainParameters extends GeoMesaConnectionParams
    with FeatureTypeNameParam
    with CQLFilterParam
    with OptionalAttributesParam {}
}
