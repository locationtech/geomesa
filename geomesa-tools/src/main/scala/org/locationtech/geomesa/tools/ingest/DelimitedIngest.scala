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
import org.apache.accumulo.core.client.Connector
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.locationtech.geomesa.accumulo.data.AccumuloDataStore
import org.locationtech.geomesa.jobs.JobUtils
import org.locationtech.geomesa.tools.Utils.Formats._
import org.locationtech.geomesa.tools.Utils.Modes._
import org.locationtech.geomesa.tools.Utils.{IngestParams, Modes}
import org.locationtech.geomesa.tools.commands.IngestCommand.IngestParameters
import org.locationtech.geomesa.tools.ingest.DelimitedIngest._
import org.locationtech.geomesa.tools.{AccumuloProperties, FeatureCreator}
import org.locationtech.geomesa.utils.classpath.ClassPathUtils

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

class DelimitedIngest(params: IngestParameters) extends AccumuloProperties {

  def run(): Unit = {
    // create schema for the feature prior to Ingest job
    FeatureCreator.createFeature(params)

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
    val job = new ScaldingDelimitedIngestJob(arguments)
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
      () => ClassPathUtils.getJarsFromClasspath(classOf[ScaldingDelimitedIngestJob]),
      () => ClassPathUtils.getJarsFromClasspath(classOf[AccumuloDataStore]),
      () => ClassPathUtils.getJarsFromClasspath(classOf[Connector]))

  def getScaldingArgs(): Args = {
    val singleArgs = List(classOf[ScaldingDelimitedIngestJob].getCanonicalName, getModeFlag(params.files(0)))

    val requiredKvArgs: Map[String, List[String]] = Map(
      IngestParams.FILE_PATH         -> encodeFileList(params.files.toList),
      IngestParams.SFT_SPEC          -> URLEncoder.encode(params.spec, "UTF-8"),
      IngestParams.CATALOG_TABLE     -> params.catalog,
      IngestParams.ZOOKEEPERS        -> Option(params.zookeepers).getOrElse(zookeepersProp),
      IngestParams.ACCUMULO_INSTANCE -> Option(params.instance).getOrElse(instanceName),
      IngestParams.ACCUMULO_USER     -> params.user,
      IngestParams.ACCUMULO_PASSWORD -> getPassword(params.password),
      IngestParams.DO_HASH           -> params.hash.toString,
      IngestParams.FORMAT            -> Option(params.format).getOrElse(getFileExtension(params.files(0))),
      IngestParams.FEATURE_NAME      -> params.featureName,
      IngestParams.IS_TEST_INGEST    -> false.toString
    ).mapValues(List(_))

    val optionalKvArgs: Map[String, List[String]] = List(
      Option(params.columns)      .map(      IngestParams.COLS            -> List(_)),
      Option(params.dtFormat)     .map(      IngestParams.DT_FORMAT       -> List(_)),
      Option(params.idFields)     .map(      IngestParams.ID_FIELDS       -> List(_)),
      Option(params.dtgField)     .map(      IngestParams.DT_FIELD        -> List(_)),
      Option(params.skipHeader)   .map(sh => IngestParams.SKIP_HEADER     -> List(sh.toString)),
      Option(params.lon)          .map(      IngestParams.LON_ATTRIBUTE   -> List(_)),
      Option(params.lat)          .map(      IngestParams.LAT_ATTRIBUTE   -> List(_)),
      Option(params.auths)        .map(      IngestParams.AUTHORIZATIONS  -> List(_)),
      Option(params.visibilities) .map(      IngestParams.VISIBILITIES    -> List(_)),
      Option(params.indexSchema)  .map(      IngestParams.INDEX_SCHEMA_FMT-> List(_)),
      Option(params.listDelimiter).map(      IngestParams.LIST_DELIMITER  -> List(_)),
      Option(params.mapDelimiters).map(      IngestParams.MAP_DELIMITERS  -> _.asScala.toList)).flatten.toMap

    if (!optionalKvArgs.contains(IngestParams.DT_FIELD)) {
      // assume user has no date field to use and that there is no column of data signifying it.
      logger.warn("Warning: no date-time field specified. Assuming that data contains no date column. \n" +
        s"GeoMesa is defaulting to the system time for ingested features.")
    }

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