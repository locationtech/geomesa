/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/
package org.locationtech.geomesa.tools.ingest

import java.io.File
import java.net.URLEncoder
import java.nio.charset.StandardCharsets

import com.twitter.scalding.{Args, Hdfs, Local, Mode}
import com.typesafe.config.ConfigRenderOptions
import org.apache.accumulo.core.client.Connector
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.tools.Utils.Modes._
import org.locationtech.geomesa.tools.Utils._
import org.locationtech.geomesa.tools.commands.IngestCommand.IngestParameters
import org.locationtech.geomesa.tools.ingest.DelimitedIngest._
import org.locationtech.geomesa.tools.{AccumuloProperties, ConverterConfigParser, FeatureCreator, SftArgParser}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes

import scala.collection.JavaConversions._

class DelimitedIngest(params: IngestParameters) extends AccumuloProperties {

  val sft = SftArgParser.getSft(params.spec, params.featureName, params.config)
  val converterConfig = ConverterConfigParser.getConfig(params.config)

  def run(): Unit = {
    // create schema for the feature prior to Ingest job
    FeatureCreator.createFeature(params, sft)

    val conf = new Configuration()
    JobUtils.setLibJars(conf, libJars = ingestLibJars, searchPath = ingestJarSearchPath)

    // setup ingest
    val mode =
      if (getJobMode(params.files(0)) == Modes.Hdfs) {
        logger.info("Running ingest job in HDFS Mode")
        Hdfs(strict = true, conf)
      } else {
        logger.info("Running ingest job in Local Mode")
        Local(strictSources = true)
      }

    validateFileArgs(mode, params)

    val arguments = Mode.putMode(mode, getScaldingArgs())
    val job = new ScaldingConverterIngestJob(arguments)
    val flow = job.buildFlow

    //block until job is completed.
    flow.complete()
    job.printStatInfo()
  }

  def validateFileArgs(mode: Mode, params: IngestParameters) =
    mode match {
      case Local(_) =>
        if (params.files.size > 1) {
          throw new IllegalArgumentException("Cannot ingest multiple files in Local mode..." +
            "please provide only a single file argument")
        }
      case _ =>
    }

  def ingestLibJars = {
    val is = getClass.getClassLoader.getResourceAsStream("org/locationtech/geomesa/tools/ingest-libjars.list")
    try {
      IOUtils.readLines(is)
    } catch {
      case e: Exception => throw new Exception("Error reading ingest libjars: "+e.getMessage, e)
    } finally {
      IOUtils.closeQuietly(is)
    }
  }

  def ingestJarSearchPath: Iterator[() => Seq[File]] =
    Iterator(() => ClassPathUtils.getJarsFromEnvironment("GEOMESA_HOME"),
      () => ClassPathUtils.getJarsFromEnvironment("ACCUMULO_HOME"),
      () => ClassPathUtils.getJarsFromClasspath(classOf[ScaldingConverterIngestJob]),
      () => ClassPathUtils.getJarsFromClasspath(classOf[AccumuloDataStore]),
      () => ClassPathUtils.getJarsFromClasspath(classOf[Connector]))

  def getScaldingArgs(): Args = {
    val singleArgs = List(classOf[ScaldingConverterIngestJob].getCanonicalName, getModeFlag(params.files(0)))

    val sftString = SimpleFeatureTypes.encodeType(sft)
    val requiredKvArgs: Map[String, List[String]] = Map(
      IngestParams.FILE_PATH         -> encodeFileList(params.files.toList),
      IngestParams.SFT_SPEC          -> URLEncoder.encode(sftString, StandardCharsets.UTF_8.displayName),
      IngestParams.CATALOG_TABLE     -> params.catalog,
      IngestParams.ZOOKEEPERS        -> Option(params.zookeepers).getOrElse(zookeepersProp),
      IngestParams.ACCUMULO_INSTANCE -> Option(params.instance).getOrElse(instanceName),
      IngestParams.ACCUMULO_USER     -> params.user,
      IngestParams.ACCUMULO_PASSWORD -> getPassword(params.password),
      IngestParams.ACCUMULO_MOCK     -> params.useMock.toString,
      IngestParams.FEATURE_NAME      -> sft.getTypeName,
      IngestParams.IS_TEST_INGEST    -> false.toString,
      IngestParams.CONVERTER_CONFIG  -> URLEncoder.encode(
                                          converterConfig.root().render(ConfigRenderOptions.concise()),
                                          StandardCharsets.UTF_8.displayName)
    ).mapValues(List(_))

    val optionalKvArgs: Map[String, List[String]] = List(
      Option(params.auths)        .map(IngestParams.AUTHORIZATIONS   -> List(_)),
      Option(params.visibilities) .map(IngestParams.VISIBILITIES     -> List(_)),
      Option(params.indexSchema)  .map(IngestParams.INDEX_SCHEMA_FMT -> List(_))).flatten.toMap

    val kvArgs = (requiredKvArgs ++ optionalKvArgs).flatMap { case (k,v) => List(s"--$k") ++ v }
    Args(singleArgs ++ kvArgs)
  }
}

object DelimitedIngest {
  def encodeFileList(files: List[String]) =
    files.map { s => Hex.encodeHexString(s.getBytes(StandardCharsets.UTF_8)) }.mkString(" ")

  def decodeFileList(encoded: String) =
    encoded.split(" ").map { s => new String(Hex.decodeHex(s.toCharArray)) }
}