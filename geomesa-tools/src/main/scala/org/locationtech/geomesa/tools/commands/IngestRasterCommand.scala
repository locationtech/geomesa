/*
 * Copyright 2014 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.locationtech.geomesa.tools.commands

import java.io.{File, Serializable}
import java.util.{Map => JMap}

import com.beust.jcommander.{JCommander, Parameter, Parameters}
import org.locationtech.geomesa.raster.data.AccumuloCoverageStore
import org.locationtech.geomesa.raster.ingest.SimpleRasterIngest
import org.locationtech.geomesa.raster.util.RasterUtils.IngestRasterParams
import org.locationtech.geomesa.tools.Utils.Formats._
import org.locationtech.geomesa.tools._
import org.locationtech.geomesa.tools.commands.IngestRasterCommand.{Command, IngestRasterParameters}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

class IngestRasterCommand(parent: JCommander) extends Command with AccumuloProperties {

  val params = new IngestRasterParameters()
  parent.addCommand(Command, params)

  override def execute() {
    val fmt = Option(params.format).getOrElse(getFileExtension(params.file))
    fmt match {
      case TIFF | DTED =>
        ingest
      case _         =>
        logger.error("Error: File format not supported for file " + params.file + ". Supported formats" +
          "are tif, tiff, dt0, dt1 and dt2")
    }
  }

  def ingest() {
    val cs = createCoverageStore(params)

    val args: Map[String, Option[String]] = Map(
      IngestRasterParams.FILE_PATH -> Some(params.file),
      IngestRasterParams.FORMAT -> Some(Option(params.format).getOrElse(getFileExtension(params.file))),
      IngestRasterParams.RASTER_NAME -> Some(params.rasterName),
      IngestRasterParams.TIME -> Option(params.timeStamp)
    )

    val ingester = new SimpleRasterIngest(args, cs)
    ingester.runIngestTask() match {
      case Success(info) => logger.info("Ingestion is done.")
      case Failure(e) => throw new RuntimeException(e)
    }
  }

  def createCoverageStore(config: IngestRasterParameters): AccumuloCoverageStore = {
    if (config.rasterName == null || config.rasterName.isEmpty) {
      logger.error("No raster name specified for raster feature ingest." +
        " Please check that all arguments are correct in the previous command. ")
      sys.exit()
    }

    val password = getPassword(params.password)
    val csConfig: JMap[String, Serializable] = getAccumuloCoverageStoreConf(config, password)

    AccumuloCoverageStore(csConfig)
  }

  def getAccumuloCoverageStoreConf(config: IngestRasterParameters, password: String): JMap[String, Serializable] =
    mapAsJavaMap(Map(
      "instanceId" -> Option(config.instance).getOrElse(instanceName),
      "zookeepers" -> Option(config.zookeepers).getOrElse(zookeepersProp),
      "user" -> config.user,
      "password" -> password,
      "tableName" -> config.table,
      "geoserverConfig" -> Option(config.geoserverConf),
      "auths" -> Option(config.auths),
      "visibilities" -> Option(config.visibilities),
      "maxShard" -> Option(config.numShards),
      "writeMemory" -> Option(config.writeMemory),
      "writeThreads" -> Option(config.writeThreads),
      "queryThreads" -> Option(config.queryThreads)
    ).collect {
      case (key, Some(value)) => (key, value);
      case (key, value: String) => (key, value)
    }).asInstanceOf[java.util.Map[String, Serializable]]
}

object IngestRasterCommand {
  val Command = "ingestRaster"

  @Parameters(commandDescription = "Ingest a raster file of various formats into GeoMesa")
  class IngestRasterParameters extends CreateRasterParams {
    @Parameter(names = Array("-fmt", "--format"), description = "Format of incoming raster data (tif | tiff | dt0 | " +
      "dt1 | dt2) to override file extension recognition")
    var format: String = null

    @Parameter(names = Array("-f", "--file"), description = "Raster file to be ingested", required = true)
    var file: String = null

    @Parameter(names = Array("-tm", "--timestamp"), description = "Raster file to be ingested")
    var timeStamp: String = null
  }
}
