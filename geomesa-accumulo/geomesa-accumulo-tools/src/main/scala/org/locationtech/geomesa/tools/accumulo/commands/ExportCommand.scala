/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools.accumulo.commands

import java.util.Locale
import java.util.zip.Deflater

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import com.typesafe.scalalogging.LazyLogging
import org.locationtech.geomesa.tools.accumulo._
import org.locationtech.geomesa.tools.accumulo.commands.ExportCommand.ExportParameters
import org.locationtech.geomesa.tools.common._
import org.locationtech.geomesa.tools.common.commands.ExportCommandTools
import org.locationtech.geomesa.utils.file.FileUtils.Formats
import org.locationtech.geomesa.utils.file.FileUtils.Formats._

import scala.collection.JavaConversions._

class ExportCommand(parent: JCommander) extends CommandWithCatalog(parent)
  with ExportCommandTools
  with LazyLogging {

  override val command = "export"
  override val params = new ExportParameters

  override def execute() = {
    val start = System.currentTimeMillis()
    lazy val sft = ds.getSchema(params.featureName)
    val fmt = Formats.withName(params.outputFormat.toLowerCase(Locale.US))
    val features = fmt match {
      case SHP =>
        val schemaString: Seq[String] =
          if (params.attributes == null) {
            Seq(ShapefileExport.modifySchema(sft))
          } else {
            Seq(ShapefileExport.replaceGeomInAttributesString(params.attributes, sft))
          }
        getFeatureCollection(Some(seqAsJavaList(schemaString)), ds, params)
      case _ =>
        getFeatureCollection(None, ds, params)
    }

    lazy val avroCompression = Option(params.gzip).map(_.toInt).getOrElse(Deflater.DEFAULT_COMPRESSION)
    val exporter: FeatureExporter = fmt match {
      case CSV | TSV      => new DelimitedExport(getWriter(params), fmt, !params.noHeader)
      case SHP            => new ShapefileExport(checkShpFile(params))
      case GeoJson | JSON => new GeoJsonExport(getWriter(params))
      case GML            => new GmlExport(createOutputStream(false, params))
      case AVRO           => new AvroExport(createOutputStream(true, params), features.getSchema, avroCompression)
      case BIN            => throw new UnsupportedOperationException(s"This operation has been deprecated. " +
                              "Use the command 'export-bin' instead.")
      case NULL           => NullExport
      // shouldn't happen unless someone adds a new format and doesn't implement it here
      case _              => throw new UnsupportedOperationException(s"Format $fmt can't be exported")
    }
    try {
      exporter.write(features)
    } finally {
      exporter.flush()
      exporter.close()
      ds.dispose()
    }
    logger.info(s"Feature export complete to ${Option(params.file).map(_.getPath).getOrElse("standard out")} " +
        s"in ${System.currentTimeMillis() - start}ms")
  }
}

object ExportCommand {
  @Parameters(commandDescription = "Export features from a GeoMesa data store")
  class ExportParameters extends BaseExportCommands
    with GeoMesaConnectionParams {}
}
