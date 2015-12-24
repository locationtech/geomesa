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
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.tools.DataStoreHelper
import org.locationtech.geomesa.tools.Utils.Formats._
import org.locationtech.geomesa.tools.commands.IngestCommand._
import org.locationtech.geomesa.tools.ingest.DelimitedIngest
import org.locationtech.geomesa.utils.geotools.GeneralShapefileIngest

import scala.collection.JavaConversions._

class IngestCommand(parent: JCommander) extends Command(parent) with LazyLogging {
  override val command = "ingest"
  override val params = new IngestParameters()

  override def execute(): Unit = {
    val fmt = Option(params.format).getOrElse(getFileExtension(params.files(0)))
    if (fmt == SHP) {
      val ds = new DataStoreHelper(params).getDataStore()
      GeneralShapefileIngest.shpToDataStore(params.files(0), ds, params.featureName)
    } else {
      new DelimitedIngest(params).run()
    }
  }
}

object IngestCommand {
  @Parameters(commandDescription = "Ingest/convert various file formats into GeoMesa")
  class IngestParameters extends OptionalFeatureParams {
    @Parameter(names = Array("-s", "--spec"), description = "SimpleFeatureType specification as a GeoTools spec, SFT config, or name of an available type")
    var spec: String = null

    @Parameter(names = Array("-C", "--converter"), description = "GeoMesa converter specification as a config string or name of an available converter")
    var config: String = null

    @Parameter(names = Array("-F", "--format"), description = "indicate non-converter ingest (shp)")
    var format: String = null

    @Parameter(description = "<file>...", required = true)
    var files: java.util.List[String] = new util.ArrayList[String]()
  }
}
