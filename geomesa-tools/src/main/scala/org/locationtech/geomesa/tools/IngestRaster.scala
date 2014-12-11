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

package org.locationtech.geomesa.tools

import java.io.Serializable
import java.util.{Map => JMap}
import com.typesafe.scalalogging.slf4j.Logging
import org.locationtech.geomesa.raster.data.AccumuloCoverageStore
import org.locationtech.geomesa.raster.ingest._
import scala.collection.JavaConversions._
import scala.util.{Failure, Success}

class IngestRaster() extends Logging with AccumuloProperties {
   def createCoverageStore(config: IngestRasterArguments, password: String): AccumuloCoverageStore = {
    if (config.rasterName.isEmpty) {
      logger.error("No raster name specified for raster feature ingest." +
        " Please check that all arguments are correct in the previous command. ")
      sys.exit()
    }

    val csConfig: JMap[String, Serializable] = getAccumuloCoverageStoreConf(config, password)

    AccumuloCoverageStore(csConfig)
  }

  def getAccumuloCoverageStoreConf(config: IngestRasterArguments, password: String): JMap[String, Serializable] =
    mapAsJavaMap(Map(
      "instanceId" -> config.instanceName.getOrElse(instanceName),
      "zookeepers" -> config.zookeepers.getOrElse(zookeepers),
      "user" -> config.username,
      "password" -> password,
      "tableName" -> config.table,
      "geoserverConfig" -> config.geoserverReg,
      "auths" -> config.auths,
      "visibilities" -> config.visibilities,
      "maxShard" -> config.maxShards,
      "writeMemory" -> config.writeMemory,
      "writeThreads" -> config.writeThreads,
      "queryThreads" -> config.queryThreads
    ).collect {
      case (key, Some(value)) => (key, value);
      case (key, value: String) => (key, value)
    }).asInstanceOf[java.util.Map[String, Serializable]]

  def defineIngestJob(config: IngestRasterArguments, password: String) = {
    IngestRaster.getFileExtension(config.file).toUpperCase match {
      case "TIFF" | "DTED" =>
        ingest(config, password)
      case _ =>
        logger.error(s"Error: file format not supported. Supported formats include: TIF, TIFF and DT{0,1,2}. " +
          s"No coverage ingested.")
    }
  }

  def ingest(config: IngestRasterArguments, password: String): Unit = {
    val cs = createCoverageStore(config, password)

    val args: Map[String, Option[String]] = Map(
      IngestRasterParams.FILE_PATH -> Some(config.file),
      IngestRasterParams.FILE_TYPE -> Some(IngestRaster.getFileExtension(config.file)),
      IngestRasterParams.RASTER_NAME -> Some(config.rasterName),
      IngestRasterParams.TIME -> config.timeStr
    )

    val ingester = new SimpleRasterIngest(args, cs)
    ingester.runIngestTask() match {
      case Success(info) => logger.info("Ingestion is done.")
      case Failure(e) => throw new RuntimeException(e)
    }
  }
}

object IngestRaster extends App with Logging with GetPassword {
  val parser = new scopt.OptionParser[IngestRasterArguments]("geomesa-tools ingestRaster") {
    implicit val optionStringRead: scopt.Read[Option[String]] = scopt.Read.reads(Option[String])
    head("GeoMesa Tools IngestRaster", "1.0")
    opt[String]('u', "username") action { (x, c) =>
      c.copy(username = x) } text "Accumulo username" required()
    opt[Option[String]]('p', "password") action { (x, c) =>
      c.copy(password = x) } text "Accumulo password, This can also be provided after entering a command" optional()
    opt[Option[String]]("instance-name").action { (s, c) =>
      c.copy(instanceName = s) } text "Accumulo instance name" optional()
    opt[Option[String]]('z', "zookeepers").action { (s, c) =>
      c.copy(zookeepers = s) } text "Accumulo Zookeepers string" optional()
    opt[String]('t', "table").action { (s, c) =>
      c.copy(table = s) } text "the name of the Accumulo table to use -- or create" required()
    opt[Option[String]]('a', "auths") action { (s, c) =>
      c.copy(auths = s) } text "Accumulo auths (optional)" optional()
    opt[Option[String]]('v', "visibilities") action { (s, c) =>
      c.copy(visibilities = s) } text "Accumulo visibilities (optional)" optional()
    opt[Int]("shards") action { (i, c) =>
      c.copy(maxShards = Option(i)) } text "Accumulo number of shards to use (optional)" optional()
    opt[Long]("writeMemory") action { (l, c) =>
      c.copy(writeMemory = Option(l)) } text "Memory allocation to ask for (optional)" optional()
    opt[Int]("writeThreads") action { (i, c) =>
      c.copy(writeThreads = Option(i)) } text "Threads for writing raster data (optional)" optional()
    opt[Int]("queryThreads") action { (i, c) =>
      c.copy(writeThreads = Option(i)) } text "Threads for querying raster data (optional)" optional()
    opt[String]("timeStr") action { (s, c) =>
      c.copy(timeStr = Option(s)) } text "Ingestion time string (optional)" optional()
    opt[String]('r', "raster-name").action { (s, c) =>
      c.copy(rasterName = s) } text "the name of the feature" required()
    opt[String]("file").action { (s, c) =>
      c.copy(file = s) } text "the file to be ingested" required()
    opt[Option[String]]('g', "geoserverReg") action { (s, c) =>
      c.copy(geoserverReg = s) } text "Geoserver registration info (optional)" optional()
    help("help").text("show help command")
    success
  }

  try {
    parser.parse(args, IngestRasterArguments()).map { config =>
      val pw = password(config.password)
      val ingestRaster = new IngestRaster()
      ingestRaster.defineIngestJob(config, pw)
    } getOrElse {
      logger.error("Error: command not recognized.")
    }
  }
  catch {
    case npe: NullPointerException => logger.error("Missing options and or unknown arguments on ingestRaster." +
                                                   "\n\t See 'geomesa ingestRaster --help'", npe)
  }

  def getFileExtension(file: String) = file.toLowerCase match {
    case geotiff if (file.endsWith("tif") || file.endsWith("tiff"))                     => "TIFF"
    case dted if (file.endsWith("dt0") || file.endsWith("dt1") || file.endsWith("dt2")) => "DTED"
    case _                                                                              => "NOTSUPPORTED"
  }
}
