/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import java.io.Closeable

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.apache.commons.io.IOUtils
import org.locationtech.geomesa.tools.accumulo._
import org.locationtech.geomesa.tools.accumulo.commands.BinaryExportCommand.BinaryExportParameters
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType

import scala.collection.JavaConversions._

class BinaryExportCommand(parent: JCommander) extends CommandWithCatalog(parent)
  with ExportCommandTools[BinaryExportParameters]
  with LazyLogging {

  override val command = "export-bin"
  override val params = new BinaryExportParameters

  override def execute() = {
    val start = System.currentTimeMillis()
    val sft = ds.getSchema(params.featureName)
    sft.getDtgField.foreach(BinFileExport.DEFAULT_TIME = _)
    val optAtt = Seq(BinFileExport.getAttributeList(params))
    val features = getFeatureCollection(Option(seqAsJavaList(optAtt)), ds, params)
    val exporter: FeatureExporter = BinFileExport(createOutputStream(false, params), params)
    try {
      exporter.write(features)
      logger.info(s"Feature export complete to ${Option(params.file).map(_.getPath).getOrElse("standard out")}")
    } finally {
      IOUtils.closeQuietly(exporter)
      ds.dispose()
    }
    logger.info(s"Feature export complete to ${Option(params.file).map(_.getPath).getOrElse("standard out")} " +
      s"in ${System.currentTimeMillis() - start}ms")
  }
}

object BinaryExportCommand {
  @Parameters(commandDescription = "Export features from a GeoMesa data store in a binary format.")
  class BinaryExportParameters extends BaseExportCommands {
    @Parameter(names = Array("--id-attribute"), description = "name of the id attribute to export")
    var idAttribute: String = null

    @Parameter(names = Array("--lat-attribute"), description = "name of the latitude attribute to export")
    var latAttribute: String = null

    @Parameter(names = Array("--lon-attribute"), description = "name of the longitude attribute to export")
    var lonAttribute: String = null

    @Parameter(names = Array("--dt-attribute"), description = "name of the date attribute to export", required = true)
    var dateAttribute: String = null

    @Parameter(names = Array("--label-attribute"), description = "name of the attribute to use as a bin file label")
    var labelAttribute: String = null
  }
}
