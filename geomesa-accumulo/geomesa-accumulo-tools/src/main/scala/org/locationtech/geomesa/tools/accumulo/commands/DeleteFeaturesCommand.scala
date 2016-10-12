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
import org.geotools.data.Transaction
import org.geotools.filter.text.ecql.ECQL
import org.locationtech.geomesa.tools.accumulo.GeoMesaConnectionParams
import org.locationtech.geomesa.tools.accumulo.commands.DeleteFeaturesCommand.DeleteFeaturesParameters
import org.locationtech.geomesa.tools.common.{FeatureTypeNameParam, OptionalCQLFilterParam}
import org.opengis.filter.Filter

class DeleteFeaturesCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {
  override val command = "delete-features"
  override val params = new DeleteFeaturesParameters

  override def execute() = {
    val sftName = params.featureName
    val filter = Option(params.cqlFilter).map(ECQL.toFilter).getOrElse(Filter.INCLUDE)

    logger.info(s"Deleting features from $sftName with filter $filter. This may take a few moments...")
    ds.getFeatureSource(sftName).removeFeatures(filter)
    logger.info("Features deleted")
  }
}

object DeleteFeaturesCommand {
  @Parameters(commandDescription = "Delete features from a table in GeoMesa. " +
    "Does not delete any tables or schema information.")
  class DeleteFeaturesParameters extends GeoMesaConnectionParams
    with FeatureTypeNameParam
    with OptionalCQLFilterParam {}
}