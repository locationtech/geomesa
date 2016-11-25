/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka.tools.data

import com.beust.jcommander.Parameters
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.kafka.KafkaDataStoreHelper
import org.locationtech.geomesa.kafka.tools.{KafkaDataStoreCommand, ProducerKDSConnectionParams}
import org.locationtech.geomesa.tools.utils.CLArgResolver
import org.locationtech.geomesa.tools.{RequiredFeatureSpecParam, RequiredTypeNameParam}
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

class KafkaCreateSchemaCommand extends KafkaDataStoreCommand with LazyLogging {

  override val name = "create-schema"
  override val params = new KafkaCreateSchemaParams()

  override def execute() = {
    val sft = CLArgResolver.getSft(params.spec, params.featureName)
    val featureName = params.featureName

    val sftString = SimpleFeatureTypes.encodeType(sft)
    logger.info(s"Creating '$featureName' using a KafkaDataStore with spec " +
      s"'$sftString'. Just a few moments...")

    val streamingSFT = KafkaDataStoreHelper.createStreamingSFT(sft, params.zkPath)

    try {
      withDataStore { (ds) =>
        logger.info("Creating GeoMesa Kafka schema...")
        ds.createSchema(streamingSFT)

        if (ds.getSchema(featureName) != null) {
          logger.info(s"Feature '$featureName' with spec " +
            s"'$sftString' successfully created in Kafka with zkPath '${params.zkPath}'.")
          println(s"Created feature $featureName")
        } else {
          logger.error(s"There was an error creating feature '$featureName' with spec " +
            s"'$sftString'. Please check that all arguments are correct " +
            "in the previous command.")
        }
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

@Parameters(commandDescription = "Create a feature definition in GeoMesa")
class KafkaCreateSchemaParams extends ProducerKDSConnectionParams with RequiredFeatureSpecParam with RequiredTypeNameParam
