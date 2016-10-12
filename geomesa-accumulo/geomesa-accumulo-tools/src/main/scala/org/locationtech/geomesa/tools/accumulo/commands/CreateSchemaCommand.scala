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
import org.locationtech.geomesa.accumulo.index.Constants
import org.locationtech.geomesa.tools.accumulo.commands.CreateSchemaCommand.CreateParameters
import org.locationtech.geomesa.tools.accumulo.{GeoMesaConnectionParams, OptionalAccumuloSharedTablesParam}
import org.locationtech.geomesa.tools.common.{CLArgResolver, FeatureTypeNameParam, FeatureTypeSpecParam, OptionalDTGParam}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

class CreateSchemaCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {
  override val command = "create-schema"
  override val params = new CreateParameters()

  override def execute() = {
    val sft = CLArgResolver.getSft(params.spec, params.featureName)
    val featureName = params.featureName
    val dtField = Option(params.dtgField)
    val sharedTable = Option(params.useSharedTables)

    val sftString = SimpleFeatureTypes.encodeType(sft)
    logger.info(s"Creating '$featureName' on catalog table '$catalog' with spec " +
      s"'$sftString'. Just a few moments...")

    if (ds.getSchema(featureName) == null) {

      logger.info("Creating GeoMesa tables...")
      if (dtField.orNull != null) {
        sft.setDtgField(dtField.getOrElse(Constants.SF_PROPERTY_START_TIME))
      }

      sharedTable.foreach(sft.setTableSharing)

      ds.createSchema(sft)

      if (ds.getSchema(featureName) != null) {
        logger.info(s"Feature '$featureName' on catalog table '$catalog' with spec " +
          s"'$sftString' successfully created.")
        println(s"Created feature $featureName")
      } else {
        logger.error(s"There was an error creating feature '$featureName' on catalog table " +
          s"'$catalog' with spec '$sftString'. Please check that all arguments are correct " +
          "in the previous command.")
      }
    } else {
      logger.error(s"A feature named '$featureName' already exists in the data store with " +
        s"catalog table '$catalog'.")
    }
    ds.dispose()
  }
}

object CreateSchemaCommand {
  @Parameters(commandDescription = "Create a GeoMesa feature type")
  class CreateParameters extends GeoMesaConnectionParams
    with FeatureTypeSpecParam
    with FeatureTypeNameParam
    with OptionalDTGParam
    with OptionalAccumuloSharedTablesParam {}
}