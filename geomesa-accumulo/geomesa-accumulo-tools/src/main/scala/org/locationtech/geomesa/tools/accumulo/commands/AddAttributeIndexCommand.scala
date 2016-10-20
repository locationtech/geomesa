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
import org.apache.hadoop.util.ToolRunner
import org.locationtech.geomesa.jobs.index.{AttributeIndexArgs, AttributeIndexJob}
import org.locationtech.geomesa.tools.accumulo.GeoMesaConnectionParams
import org.locationtech.geomesa.tools.accumulo.commands.AddAttributeIndexCommand.AddIndexParameters
import org.locationtech.geomesa.tools.common.commands.Command
import org.locationtech.geomesa.tools.common.{AttributesParam, FeatureTypeNameParam}

class AddAttributeIndexCommand(parent: JCommander) extends Command(parent) with CommandWithAccumuloDataStore with LazyLogging {
  override val command = "add-attribute-index"
  override val params = new AddIndexParameters

  override def execute() = {

    try {
      val args = new AttributeIndexArgs(Array.empty)
      args.inZookeepers = params.zookeepers
      args.inInstanceId = params.instance
      args.inUser       = params.user
      args.inPassword   = params.password
      args.inTableName  = params.catalog
      args.inFeature    = params.featureName
      args.coverage     = params.coverage
      args.attributes.addAll(params.attributes)

      logger.info(s"Running map reduce index job for attributes: ${params.attributes} with coverage: ${params.coverage}...")

      val result = ToolRunner.run(new AttributeIndexJob(), args.unparse())

      if (result == 0) {
        logger.info("Add attribute index command finished successfully.")
      } else {
        logger.error("Error encountered running attribute index command. Check hadoop's job history logs for more information.")
      }

    } catch {
      case e: Exception =>
        logger.error(s"Exception encountered running attribute index command. " +
          s"Check hadoop's job history logs for more information if necessary: " + e.getMessage, e)
    }
  }
}

object AddAttributeIndexCommand {
  @Parameters(commandDescription = "Run a Hadoop map reduce job to add an index for attributes")
  class AddIndexParameters extends GeoMesaConnectionParams
    with FeatureTypeNameParam with AttributesParam {

    @Parameter(names = Array("--coverage"),
      description = "Type of index (join or full)", required = true)
    var coverage: String = null
  }
}
