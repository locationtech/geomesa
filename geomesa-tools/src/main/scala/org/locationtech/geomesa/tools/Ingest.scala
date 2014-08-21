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

import java.net.URLEncoder
import com.twitter.scalding.Tool
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.util.ToolRunner
import org.locationtech.geomesa.jobs.JobUtils

class Ingest() extends Logging with AccumuloProperties {

  def getAccumuloDataStoreConf(config: IngestArguments, password: String) = Map (
    "instanceId"        ->  instanceName,
    "zookeepers"        ->  zookeepers,
    "user"              ->  config.username,
    "password"          ->  password,
    "auths"             ->  config.auths.orNull,
    "visibilities"      ->  config.visibilities.orNull,
    "maxShard"          ->  Some(config.maxShards),
    "indexSchemaFormat" ->  config.indexSchemaFormat.orNull,
    "tableName"         ->  config.catalog
  )

  def defineIngestJob(config: IngestArguments, password: String) = {
    config.format.get.toUpperCase match {
      case "CSV" | "TSV" =>
        config.method.toLowerCase match {
          case "local" =>
            logger.info("Ingest has started, please wait.")
            runIngestJob(config, "--local", password)
          case "mr" =>
            logger.info("Map-reduced Ingest has started, please wait.")
            runIngestJob(config, "--hdfs", password)
          case _ =>
            logger.error("Error, no such ingest method for CSV or TSV found, no data ingested")
        }
      case "SHP" =>
        val dsConfig = getAccumuloDataStoreConf(config, password)
        ShpIngest.doIngest(config, dsConfig)
      case _ =>
        logger.error(s"Error: file format not supported." +
          s" Supported formats include: CSV, TSV, and SHP. No data ingested.")

    }
  }

  def runIngestJob(config: IngestArguments, fileSystem: String, password: String): Unit = {
    //val libJars = JobUtils.getJarsFromClasspath(classOf[SVIngest]).mkString(",")
    val libJars = JobUtils.defaultLibJars.mkString(",")
    val jobConf = new JobConf
    // jobConf.setJobName(s"Ingest Job by user: $config.username")
    // not sure about this part
    jobConf.setSpeculativeExecution(false)
    ToolRunner.run(
      jobConf, new Tool,
      Array(
        "-libjars", libJars,
        classOf[SVIngest].getCanonicalName,
        fileSystem,
        "--idFields",           config.idFields.orNull,
        "--file",               config.file.toString,
        "--sftspec",            URLEncoder.encode(config.spec, "UTF-8"),
        "--dtField",            config.dtField.orNull,
        "--lonAttribute",       config.lonAttribute.orNull,
        "--latAttribute",       config.latAttribute.orNull,
        "--skipHeader",         config.skipHeader.toString,
        "--doHash",             config.doHash.toString,
        "--format",             config.format.orNull,
        "--catalog",            config.catalog,
        "--instanceId",         instanceName,
        "--featureName",        config.featureName.orNull,
        "--zookeepers",         zookeepers,
        "--user",               config.username,
        "--password",           password,
        "--auths",              config.auths.orNull,
        "--visibilities",       config.visibilities.orNull,
        "--indexSchemaFmt",     config.indexSchemaFormat.orNull,
        "--shards",             config.maxShards.orNull.toString )
      )
  }

//  def buildLibJars: String = {
//    val accumuloJars = classOf[String].getClassLoader.asInstanceOf[URLClassLoader]
//      .getURLs
//      .filter { _.toString.contains("accumulo") }
//      .map(u => Utils.classPathUrlToAbsolutePath(u.getFile))
//
//    val geomesaJars = classOf[IndexSchema].getClassLoader match {
//      case cl: VFSClassLoader =>
//        cl.getFileObjects.map(u => Utils.classPathUrlToAbsolutePath(u.getURL.getFile))
//
//      case cl: URLClassLoader =>
//        cl.getURLs.filter { _.toString.contains("geomesa") }.map { u => Utils.classPathUrlToAbsolutePath(u.getFile) }
//    }
//
//    val ret = (accumuloJars ++ geomesaJars).mkString(",")
//    logger.info(ret)
//    ret
//  }

}


object Ingest extends App with Logging with GetPassword {
  val parser = new scopt.OptionParser[IngestArguments]("geomesa-tools ingest") {
    head("GeoMesa Tools Ingest", "1.0")
    opt[String]('u', "username") action { (x, c) =>
      c.copy(username = x) } text "Accumulo username" required()
    opt[String]('p', "password") action { (x, c) =>
      c.copy(password = x) } text "Accumulo password, This can also be provided after entering a command" optional()
    opt[String]('c', "catalog").action { (s, c) =>
      c.copy(catalog = s) } text "the name of the Accumulo table to use -- or create" required()
    opt[String]('a', "auths") action { (s, c) =>
      c.copy(auths = Option(s)) } text "Accumulo auths (optional)" optional()
    opt[String]('v', "visibilities") action { (s, c) =>
      c.copy(visibilities = Option(s)) } text "Accumulo visibilities (optional)" optional()
    opt[String]('i', "indexSchemaFormat") action { (s, c) =>
      c.copy(indexSchemaFormat = Option(s)) } text "Accumulo index schema format (optional)" optional()
    opt[Int]("shards") action { (i, c) =>
      c.copy(maxShards = Option(i)) } text "Accumulo number of shards to use (optional)" optional()
    opt[String]('f', "feature-name").action { (s, c) =>
      c.copy(featureName = Option(s)) } text "the name of the feature" required()
    opt[String]('s', "sftspec").action { (s, c) =>
      c.copy(spec = s) } text "the sft specification of the file," +
      " must match number of columns and order of ingest file if csv or tsv formatted." +
      " If ingesting lat/lon column data an additional field for the point geometry must be added, ie: *geom:Point ." optional()
    opt[String]("datetime").action { (s, c) =>
      c.copy(dtField = Option(s)) } text "the name of the datetime field in the sft" optional()
    opt[String]("dtformat").action { (s, c) =>
      c.copy(dtFormat = s) } text "the format of the datetime field" optional()
    opt[String]("idfields").action { (s, c) =>
      c.copy(idFields = Option(s)) } text "the set of attributes of each feature used" +
      " to encode the feature name" optional()
    opt[Unit]('h', "hash")action { (_, c) =>
      c.copy(doHash = true) } text "flag to md5 hash to identity of each feature" optional()
    opt[String]("lon").action { (s, c) =>
      c.copy(lonAttribute = Option(s)) } text "the name of the longitude field in the sft if ingesting point data" optional()
    opt[String]("lat").action { (s, c) =>
      c.copy(latAttribute = Option(s)) } text "the name of the latitude field in the sft if ingesting point data" optional()
    opt[Unit]("skip-header").action { (b, c) =>
      c.copy(skipHeader = true) } text "flag for skipping first line in file" optional()
    opt[String]("file").action { (s, c) =>
      c.copy(file = s, format = Option(getFileExtension(s)), method = getFileSystemMethod(s)) } text "the file to be ingested" required()
    help("help").text("show help command")
    checkConfig { c =>
      if (c.maxShards.isDefined && c.indexSchemaFormat.isDefined) {
        failure("Error: the options for setting the max shards and the indexSchemaFormat cannot both be set.")
      } else {
        success
      }
    }
  }

  try {
    parser.parse(args, IngestArguments()).map { config =>
      val pw = password(config.password)
      val ingest = new Ingest()
      ingest.defineIngestJob(config, pw)
    } getOrElse {
      logger.error("Error: command not recognized.")
    }
  }
  catch {
    case npe: NullPointerException => logger.error("Missing options and or unknown arguments on ingest." +
                                                   "\n\t See 'geomesa ingest --help'")
  }

  def getFileExtension(file: String) = file.toLowerCase match {
    case csv if file.endsWith("csv") => "CSV"
    case tsv if file.endsWith("tsv") => "TSV"
    case shp if file.endsWith("shp") => "SHP"
    case _                           => "NOTSUPPORTED"
  }

  def getFileSystemMethod(path: String): String = path.toLowerCase.startsWith("hdfs") match {
    case true => "mr"
    case _    => "local"
  }

}

