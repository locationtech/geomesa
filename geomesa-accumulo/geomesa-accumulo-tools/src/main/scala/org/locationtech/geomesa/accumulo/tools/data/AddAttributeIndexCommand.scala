/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.tools.data

import com.beust.jcommander.{Parameter, Parameters}
import org.locationtech.geomesa.accumulo.tools.data.AddAttributeIndexCommand.AddAttributeIndexParams
import org.locationtech.geomesa.accumulo.tools.{AccumuloDataStoreCommand, AccumuloDataStoreParams}
import org.locationtech.geomesa.jobs.accumulo.index.{AttributeIndexArgs, AttributeIndexJob}
import org.locationtech.geomesa.tools.{Command, RequiredAttributesParam, RequiredTypeNameParam}

class AddAttributeIndexCommand extends AccumuloDataStoreCommand {

  override val name = "add-attribute-index"
  override val params = new AddAttributeIndexParams

  override def execute(): Unit = {
    // We instantiate the class at runtime to avoid classpath dependencies from commands that are not being used.
    new AddAttributeIndexCommandExecutor(params).run()
  }
}

object AddAttributeIndexCommand {
  @Parameters(commandDescription = "Run a Hadoop map reduce job to add an index for attributes")
  class AddAttributeIndexParams extends AccumuloDataStoreParams with RequiredTypeNameParam with RequiredAttributesParam {
    @Parameter(names = Array("--coverage"),
      description = "Type of index (join or full)", required = true)
    var coverage: String = null
  }
}

class AddAttributeIndexCommandExecutor(params: AddAttributeIndexParams) extends Runnable {
  override def run(): Unit = {
    // Imported here to avoid classpath issues when generating the autocomplete script
    import org.apache.hadoop.util.ToolRunner

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

      Command.user.info(s"Running map reduce index job for attributes: ${params.attributes} with coverage: ${params.coverage}...")

      val result = ToolRunner.run(new AttributeIndexJob(), args.unparse())

      if (result == 0) {
        Command.user.info("Add attribute index command finished successfully.")
      } else {
        Command.user.error("Error encountered running attribute index command. Check hadoop's job history logs for more information.")
      }

    } catch {
      case e: Exception =>
        Command.user.error(s"Exception encountered running attribute index command. " +
          s"Check hadoop's job history logs for more information if necessary: " + e.getMessage, e)
    }
  }
}
