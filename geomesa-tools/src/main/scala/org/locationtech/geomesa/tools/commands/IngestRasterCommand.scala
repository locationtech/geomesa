/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.commands

import java.io.File

import com.beust.jcommander.{IParameterValidator, JCommander, Parameter, Parameters}
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams
import org.locationtech.geomesa.tools.Utils.Formats._
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.commands.IngestRasterCommand.IngestRasterParameters
import org.locationtech.geomesa.tools.ingest.LocalRasterIngest

import scala.util.{Failure, Success}

class IngestRasterCommand(parent: JCommander) extends Command(parent) with AccumuloProperties {
  override val command: String = "ingestraster"
  override val params = new IngestRasterParameters()

  override def execute() {
    val fmt = Option(params.format).getOrElse(getFileExtension(params.file))
    fmt match {
      case TIFF | DTED =>
        val localIngester =
          new LocalRasterIngest(getRasterIngestParams + (IngestRasterParams.FILE_PATH  -> Some(params.file)))
        localIngester.runIngestTask match {
          case Success(info) => logger.info("Local ingestion is done.")
          case Failure(e) => throw new RuntimeException(e)
        }
      case _         =>
        logger.error("Error: File format not supported for file " + params.file + ". Supported formats " +
          "are geotiff and DTED")
    }
  }

  def getFormat(fileOrDir: File): String = {
    val file =
      if (fileOrDir.isDirectory) fileOrDir.listFiles.head
      else fileOrDir
    getFileExtension(file.getName)
  }

  def getRasterIngestParams(): Map[String, Option[String]] = {
    Map(
      IngestRasterParams.ZOOKEEPERS        -> Some(Option(params.zookeepers).getOrElse(zookeepersProp)),
      IngestRasterParams.ACCUMULO_INSTANCE -> Some(Option(params.instance).getOrElse(instanceName)),
      IngestRasterParams.ACCUMULO_USER     -> Some(params.user),
      IngestRasterParams.ACCUMULO_PASSWORD -> Some(getPassword(params.password)),
      IngestRasterParams.AUTHORIZATIONS    -> Option(params.auths),
      IngestRasterParams.VISIBILITIES      -> Option(params.visibilities),
      IngestRasterParams.ACCUMULO_MOCK     -> Some(params.useMock.toString),
      IngestRasterParams.TABLE             -> Some(params.table),
      IngestRasterParams.FORMAT            -> Some(Option(params.format).getOrElse(getFormat(new File(params.file)))),
      IngestRasterParams.TIME              -> Option(params.timeStamp),
      IngestRasterParams.WRITE_MEMORY      -> Option(params.writeMemory),
      IngestRasterParams.WRITE_THREADS     -> Option(params.writeThreads).map(_.toString),
      IngestRasterParams.QUERY_THREADS     -> Option(params.queryThreads).map(_.toString),
      IngestRasterParams.PARLEVEL          -> Some(params.parLevel.toString)
    )
  }
}

class PathValidator extends IParameterValidator {
  def validate(name: String, value: String) {
    if (value == null) handleError(value, "is null")
    val fileOrDir = new File(value)
    if (!fileOrDir.exists) handleError(value, "doesn't exist")
    if (fileOrDir.isDirectory && fileOrDir.listFiles.size == 0) handleError(value, "is empty")
  }

  def handleError(fileOrDir: String, reason: String) {
    throw new Exception(s"No file is specified to ingest: ${fileOrDir} ${reason}.")
  }
}

object IngestRasterCommand {
  @Parameters(commandDescription = "Ingest a raster file or raster files in a directory into GeoMesa")
  class IngestRasterParameters extends CreateRasterParams {
    @Parameter(names = Array("-fmt", "--format"), description = "Format of incoming raster data " +
      "(geotiff | DTED) to override file extension recognition")
    var format: String = null

    @Parameter(names = Array("-f", "--file"), description = "Single raster file or directory of " +
      "raster files to be ingested", validateWith = classOf[PathValidator], required = true)
    var file: String = null

    @Parameter(names = Array("-tm", "--timestamp"), description = "Ingestion time (default to current time)")
    var timeStamp: String = null

    @Parameter(names = Array("-par", "--parallel-level"), description = "Maximum number of local " +
      "threads for ingesting multiple raster files (default to 1)")
    var parLevel: Integer = 1

  }
}
