/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.kafka.commands

import com.beust.jcommander.{JCommander, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.kafka.KafkaDataStoreHelper
import org.locationtech.geomesa.tools.common.{CLArgResolver, FeatureTypeNameParam, FeatureTypeSpecParam}
import org.locationtech.geomesa.tools.kafka.ProducerKDSConnectionParams
import org.locationtech.geomesa.tools.kafka.commands.CreateSchemaCommand.CreateParameters
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

class CreateSchemaCommand(parent: JCommander) extends CommandWithKDS(parent) with LazyLogging {
  override val command = "create-schema"
  override val params = new CreateParameters()

  override def execute() = {
    val sft = CLArgResolver.getSft(params.spec, params.featureName)
    val featureName = params.featureName

    val sftString = SimpleFeatureTypes.encodeType(sft)
    logger.info(s"Creating '$featureName' using a KafkaDataStore with spec " +
      s"'$sftString'. Just a few moments...")

    val streamingSFT = KafkaDataStoreHelper.createStreamingSFT(sft, zkPath)

    try {
      logger.info("Creating GeoMesa Kafka schema...")
      ds.createSchema(streamingSFT)

      if (ds.getSchema(featureName) != null) {
        logger.info(s"Feature '$featureName' with spec " +
          s"'$sftString' successfully created in Kafka with zkPath '$zkPath'.")
        println(s"Created feature $featureName")
      } else {
        logger.error(s"There was an error creating feature '$featureName' with spec " +
          s"'$sftString'. Please check that all arguments are correct " +
          "in the previous command.")
      }
    } catch {
      // if the schema already exists at the specified zkPath
      // error message will be s"Type $typeName already exists at $zkPath."
      case e: IllegalArgumentException => {
        logger.error(e.getMessage)
      }
    }
  }
}

object CreateSchemaCommand {
  @Parameters(commandDescription = "Create a feature definition in GeoMesa")
  class CreateParameters extends ProducerKDSConnectionParams
    with FeatureTypeSpecParam
    with FeatureTypeNameParam {}
}