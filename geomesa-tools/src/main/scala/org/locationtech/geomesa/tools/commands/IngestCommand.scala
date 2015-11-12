/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.commands

import java.util

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.config.{ConfigFactory, Config}
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.convert.SimpleFeatureConverters
import org.locationtech.geomesa.tools.DataStoreHelper
import org.locationtech.geomesa.tools.Utils.Formats._
import org.locationtech.geomesa.tools.Utils.{Configurator, Speculator}
import org.locationtech.geomesa.tools.commands.IngestCommand._
import org.locationtech.geomesa.tools.ingest.DelimitedIngest
import org.locationtech.geomesa.utils.geotools.GeneralShapefileIngest

import scala.collection.JavaConversions._

class IngestCommand(parent: JCommander) extends Command(parent) with Logging {
  override val command = "ingest"
  override val params = new IngestParameters()

  override def execute(): Unit = {
    val fmt = Option(params.format).getOrElse(getFileExtension(params.files(0)))
    fmt match {
      case CSV | TSV => new DelimitedIngest(params).run() //TODO converter may define fmt...better figure that out
      case SHP       =>
        val ds = new DataStoreHelper(params).getOrCreateDs()
        GeneralShapefileIngest.shpToDataStore(params.files(0), ds, params.featureName)
      case _         =>
        logger.error("Error: Unrecognized file format...define converter config or specify --format shp")
    }
  }

}

object IngestCommand {
  @Parameters(commandDescription = "Ingest a file of various formats into GeoMesa")
  class IngestParameters extends CreateFeatureParams {
    @Parameter(names = Array("--convert"), description = "GeoMesa Convert config or file")
    var convertSpec: String = null

    @Parameter(names = Array("-is", "--index-schema"), description = "GeoMesa index schema format string")
    var indexSchema: String = null

    @Parameter(names = Array("-fmt", "--format"), description = "indicate non-converter ingest (shp)")
    var format: String = null

    @Parameter(description = "<file>...", required = true)
    var files: java.util.List[String] = new util.ArrayList[String]()
  }
}
