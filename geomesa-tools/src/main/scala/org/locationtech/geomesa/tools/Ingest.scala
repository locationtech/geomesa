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

import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.hadoop.fs.Path

import scala.util.Try
import scala.xml.XML

class IngestTask() extends Logging {
  val accumuloConf = XML.loadFile(s"${System.getenv("ACCUMULO_HOME")}/conf/accumulo-site.xml")
  val zookeepers = (accumuloConf \\ "property")
    .filter(x => (x \ "name")
    .text == "instance.zookeeper.host")
    .map(y => (y \ "value").text)
    .head
  val instanceDfsDir = Try((accumuloConf \\ "property")
    .filter(x => (x \ "name")
    .text == "instance.dfs.dir")
    .map(y => (y \ "value").text)
    .head)
    .getOrElse("/accumulo")
  val instanceIdDir = new Path(instanceDfsDir, "instance_id")
  val instanceName = new ZooKeeperInstance(UUID.fromString(ZooKeeperInstance.getInstanceIDFromHdfs(instanceIdDir)), zookeepers).getInstanceName

  def getAccumuloDataStoreConf(config: IngestArguments, password: String) = collection.mutable.Map (
    "instanceId"        ->  instanceName,
    "zookeepers"        ->  zookeepers,
    "user"              ->  config.username,
    "password"          ->  password,
    "auths"             ->  config.auths.getOrElse(""),
    "visibilities"      ->  config.visibilities.getOrElse(""),
    "tableName"         ->  config.catalog
  )

  def defineIngestJob(config: IngestArguments, password: String): Boolean = {
    //ensure that geomesa classes are loaded so that the subsequent
    val dsConfig = getAccumuloDataStoreConf(config, password)
    // add the maxShards variable if present prior to connecting to data store, make sure both are not set!
    if (config.maxShards.isDefined && config.indexSchemaFormat.isEmpty) {
      dsConfig.updated("maxShard", config.maxShards.get)
    }
    if (config.maxShards.isEmpty && config.indexSchemaFormat.isDefined) {
      dsConfig.updated("indexSchemaFormat", config.indexSchemaFormat.get)
    }
    config.format.toUpperCase match {
      case "CSV" | "TSV" =>
        config.method.toLowerCase match {
          case "local" =>
            logger.info("Ingest has started, please wait.")
            val ingest = new SVIngest(config, dsConfig.toMap)
            ingest.runIngest()
            true
          case _ =>
            logger.error("Error, no such ingest method for CSV or TSV found, no data ingested")
            false
        }

      case _ =>
        logger.error(s"Error, format: \'${config.format}\' not supported. Supported formats include: CSV, TSV")
        false
    }
  }
}

object Ingest extends App with Logging {
  val parser = new scopt.OptionParser[IngestArguments]("geomesa-tools ingest") {
    head("GeoMesa Tools Ingest", "1.0")
    note("A single format flag must be set, eg: either --csv or --tsv")
    opt[Unit]("csv") action { (_, c) =>
      c.copy(format = "CSV") } text "partially optional csv format flag" optional()
    opt[Unit]("tsv") action { (_, c) =>
      c.copy(format = "TSV") } text "partially optional tsv format flag" optional()
    opt[String]('u', "username") action { (x, c) =>
      c.copy(username = x) } text "Accumulo username" required()
    opt[String]('p', "password") action { (x, c) =>
      c.copy(password = x) } text "Accumulo password, This can also be provided after entering a command" optional()
    opt[String]('a', "auths") action { (s, c) =>
      c.copy(auths = Option(s)) } text "Accumulo auths (optional)" optional()
    opt[String]('v', "visibilities") action { (s, c) =>
      c.copy(visibilities = Option(s)) } text "Accumulo visibilities (optional)" optional()
    opt[String]('i', "indexSchemaFormat") action { (s, c) =>
      c.copy(indexSchemaFormat = Option(s)) } text "Accumulo index schema format (optional)" optional()
    opt[Int]("shards") action { (i, c) =>
      c.copy(maxShards = Option(i)) } text "Accumulo Max number of shards to use (optional)" optional()
    opt[String]('c', "catalog").action { (s, c) =>
      c.copy(catalog = s) } text "the name of the Accumulo table to use -- or create" required()
    opt[String]('f', "feature-name").action { (s, c) =>
      c.copy(featureName = s) } text "the name of the feature" required()
    opt[String]('s', "sftspec").action { (s, c) =>
      c.copy(spec = s) } text "the sft specification of the file," +
      " must match number of columns and order of ingest file if csv or tsv formatted" required()
    opt[String]("datetime").action { (s, c) =>
      c.copy(dtField = Option(s)) } text "the name of the datetime field in the sft" required()
    opt[String]("dtformat").action { (s, c) =>
      c.copy(dtFormat = s) } text "the format of the datetime field" required()
    opt[String]("idfields").action { (s, c) =>
      c.copy(idFields = Option(s)) } text "the set of attributes of each feature used" +
      " to encode the feature name" optional()
    opt[Unit]("hash") abbr "h" action { (_, c) =>
      c.copy(doHash = true) } text "flag to md5 hash to indentity of each feature" optional()
    opt[String]("lon").action { (s, c) =>
      c.copy(lonAttribute = Option(s)) } text "the name of the longitude field in the sft if ingesting point data" optional()
    opt[String]("lat").action { (s, c) =>
      c.copy(latAttribute = Option(s)) } text "the name of the latitude field in the sft if ingesting point data" optional()
    opt[Unit]("skip-header").action { (b, c) =>
      c.copy(skipHeader = true) } text "flag for skipping first line in file" optional()
    opt[String]("file").action { (s, c) =>
      c.copy(file = s) } text "the file to be ingested" required()
    help("help").text("show help command")
    checkConfig{ c =>
      if (c.format.isEmpty) {
        failure("No format set for ingest")
      } else if (c.maxShards.isDefined && c.indexSchemaFormat.isDefined) {
        failure("Error: the options for setting the max shards and the index schema format cannot both be set")
      } else {
        success
      } }
  }

  //
  try {
    parser.parse(args, IngestArguments()).map { config =>
      val password = config.password match {
        case pw: String => pw
        case _ =>
          val standardIn = System.console()
          print("Password> ")
          standardIn.readPassword().mkString
      }

      val ingest = new IngestTask()
      ingest.defineIngestJob(config, password) match {
        case true => logger.info(s"Successful ingest of file: \'${config.file}\'")
        case false => logger.error(s"Error: could not successfully ingest file: \'${config.file}\'")
      }

    } getOrElse {
      logger.error("Error: command not recognized.")
    }
  }
  catch {
    case npe: NullPointerException => logger.error("Missing options and or unknown arguments on ingest." +
                                                   "\n\t See 'geomesa ingest --help'")
  }

}

/*  ScoptArguments is a case Class used by scopt, args are stored in it and default values can be set in Config also.*/
case class IngestArguments(username: String = null, password: String = null, spec: String = null,
                          idFields: Option[String] = None, dtField: Option[String] = None, dtFormat: String = null,
                          method: String = "local", file: String = null, featureName: String = null,
                          format: String = null, catalog: String = null, lonAttribute: Option[String] = None,
                          latAttribute: Option[String] = None, skipHeader: Boolean = false, doHash: Boolean = false,
                          auths: Option[String] = None, visibilities: Option[String] = None,
                          indexSchemaFormat: Option[String] = None, maxShards: Option[Int] = None )