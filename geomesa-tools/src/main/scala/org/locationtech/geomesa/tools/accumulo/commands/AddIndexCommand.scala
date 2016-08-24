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
import org.locationtech.geomesa.jobs.index.AttributeIndexJob
import org.locationtech.geomesa.tools.accumulo.GeoMesaConnectionParams
import org.locationtech.geomesa.tools.accumulo.commands.AddIndexCommand.AddIndexParameters
import org.locationtech.geomesa.tools.common.{AttributesParam, FeatureTypeNameParam}

import scala.util.control.NonFatal

class AddIndexCommand(parent: JCommander) extends CommandWithCatalog(parent) with LazyLogging {
  override val command = "addindex"
  override val params = new AddIndexParameters

  override def execute() = {
    try {
      val attributeIndexJobParams = Map(
        "--geomesa.input.user" -> params.user,
        "--geomesa.input.password" -> params.password,
        "--geomesa.input.instanceId" -> params.instance,
        "--geomesa.input.zookeepers" -> params.zookeepers,
        "--geomesa.input.tableName" -> params.catalog,
        "--geomesa.input.feature" -> params.featureName,
        "--geomesa.index.attributes" -> params.attributes,
        "--geomesa.index.coverage" -> params.coverage
      ).filter(_._2 != null).flatMap(e => List(e._1, e._2)).toArray

      logger.info(s"Running map reduce index job for attributes: ${params.attributes} with coverage: ${params.coverage}...")

      val result = ToolRunner.run(new AttributeIndexJob(), attributeIndexJobParams)

      if (result == 0) {
        logger.info("Add attribute index command finished successfully.")
      } else {
        logger.error("Error encountered running attribute index command. Check hadoop's job history logs for more information.")
      }

    } catch {
      case npe: NullPointerException =>
        logger.error(s"Error: feature '${params.featureName}' not found. Check arguments...", npe)
      case e: Exception =>
        logger.error(s"Error describing feature '${params.featureName}': " + e.getMessage, e)
      case NonFatal(e) =>
        logger.warn(s"Non fatal error encountered describing feature '${params.featureName}': ", e)
    } finally {
      ds.dispose()
    }
  }
}

object AddIndexCommand {
  @Parameters(commandDescription = "Run a Hadoop map reduce job to add an index for attributes")
  class AddIndexParameters extends GeoMesaConnectionParams
    with FeatureTypeNameParam with AttributesParam {

    @Parameter(names = Array("-co", "--coverage"),
      description = "Type of index (join or full)", required = true)
    var coverage: String = null
  }
}
