/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.tools

import java.io.{FileInputStream, BufferedReader, File, InputStreamReader}
import java.util.UUID

import com.beust.jcommander.ParameterException
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.accumulo.server.client.HdfsZooInstance
import org.apache.commons.compress.compressors.bzip2.BZip2Utils
import org.apache.commons.compress.compressors.gzip.GzipUtils
import org.apache.commons.compress.compressors.xz.XZUtils
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.locationtech.geomesa.tools.Utils.Speculator
import org.locationtech.geomesa.tools.commands.CreateFeatureParams
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.{Failure, Success, Try}
import scala.xml.XML

object Utils {

  object IngestParams {
    val ACCUMULO_INSTANCE   = "geomesa.tools.ingest.instance"
    val ZOOKEEPERS          = "geomesa.tools.ingest.zookeepers"
    val ACCUMULO_MOCK       = "geomesa.tools.ingest.use-mock"
    val ACCUMULO_USER       = "geomesa.tools.ingest.user"
    val ACCUMULO_PASSWORD   = "geomesa.tools.ingest.password"
    val AUTHORIZATIONS      = "geomesa.tools.ingest.authorizations"
    val VISIBILITIES        = "geomesa.tools.ingest.visibilities"
    val SHARDS              = "geomesa.tools.ingest.shards"
    val INDEX_SCHEMA_FMT    = "geomesa.tools.ingest.index-schema-format"
    val FILE_PATH           = "geomesa.tools.ingest.path"
    val FEATURE_NAME        = "geomesa.tools.feature.name"
    val CATALOG_TABLE       = "geomesa.tools.feature.tables.catalog"
    val SFT_SPEC            = "geomesa.tools.feature.sft-spec"
    val IS_TEST_INGEST      = "geomesa.tools.ingest.is-test-ingest"
    val CONVERTER_CONFIG    = "geomesa.tools.ingest.converter-config"
  }

  object Formats {
    val CSV     = "csv"
    val TSV     = "tsv"
    val TIFF    = "geotiff"
    val DTED    = "DTED"
    val SHP     = "shp"
    val JSON    = "json"
    val GeoJson = "geojson"
    val GML     = "gml"
    val BIN     = "bin"

    def getFileExtension(name: String) = {
      val fileExtension = name match {
        case _ if GzipUtils.isCompressedFilename(name)  => GzipUtils.getUncompressedFilename(name)
        case _ if BZip2Utils.isCompressedFilename(name) => BZip2Utils.getUncompressedFilename(name)
        case _ if XZUtils.isCompressedFilename(name)    => XZUtils.getUncompressedFilename(name)
        case _ => name
      }

      fileExtension match {
        case _ if fileExtension.toLowerCase.endsWith(CSV)      => CSV
        case _ if fileExtension.toLowerCase.endsWith(TSV)      => TSV
        case _ if fileExtension.toLowerCase.endsWith("tif") ||
                  fileExtension.toLowerCase.endsWith("tiff")   => TIFF
        case _ if fileExtension.toLowerCase.endsWith("dt0") ||
                  fileExtension.toLowerCase.endsWith("dt1") ||
                  fileExtension.toLowerCase.endsWith("dt2")    => DTED
        case _ if fileExtension.toLowerCase.endsWith(SHP)      => SHP
        case _ if fileExtension.toLowerCase.endsWith(JSON)     => JSON
        case _ if fileExtension.toLowerCase.endsWith(GML)      => GML
        case _ if fileExtension.toLowerCase.endsWith(BIN)      => BIN
        case _                                                 => "unknown"
      }
    }

    val All = List(CSV, TSV, SHP, JSON, GeoJson, GML, BIN)
  }

  object Modes {
    val Local = "local"
    val Hdfs = "hdfs"

    def getJobMode(filename: String) = if (filename.toLowerCase.trim.startsWith("hdfs://")) Hdfs else Local
    def getModeFlag(filename: String) = "--" + getJobMode(filename)
  }

  //Recursively delete a local directory and its children
  def deleteLocalDirectory(pathStr: String) {
    val path = new File(pathStr)
    if (path.exists) {
      val files = path.listFiles
      files.foreach { _ match {
        case p if p.isDirectory => deleteLocalDirectory(p.getAbsolutePath)
        case f => f.delete
      }}
      path.delete
    }
  }

  //Recursively delete a HDFS directory and its children
  def deleteHdfsDirectory(pathStr: String) {
    val fs = FileSystem.get(new Configuration)
    val path = new Path(pathStr)
    fs.delete(path, true)
  }

  object Configurator extends Logging {
    type ConfigParser = String => Option[Config]

    def getConfig(configArg: String): Config =
      Seq(parseString, parseFile)
       .view.map(_(configArg))
       .find(_.nonEmpty)
       .getOrElse(throw new IllegalArgumentException(s"Unable to parse Converter config from argument $configArg"))
       .get

    private[Configurator] val parseString: ConfigParser = (configArg: String) =>
      Try(ConfigFactory.parseString(configArg)) match {
        case Success(config) => Some(config)
        case Failure(ex) =>
          logger.debug(s"Unable to parse config from string $configArg")
          None
      }

    private[Configurator] val parseFile: ConfigParser = (configArg: String) =>
      Try(ConfigFactory.parseFile(new File(configArg))) match {
        case Success(config) => Some(config)
        case Failure(ex) =>
          logger.debug(s"Unable to parse config from file $configArg")
          None
      }
  }

  object Speculator extends Logging {
    type SpecParser = () => Option[SimpleFeatureType]

    def getSft(specArg: String, featureName: String = null, convertArg: String = null): SimpleFeatureType = {
      val configParsers =
        Seq(Option(specArg), Option(convertArg))
          .flatten
          .flatMap(s => List(readFile(s),Some(s)).flatten)
          .map(s => getConfParser(s))

      (configParsers ++ Seq(getSpecParser(specArg, Option(featureName))))
        .view.map(_())
        .find(_.nonEmpty)
        .getOrElse(throw new ParameterException("Unable to parse Simple Feature type from sft config or string"))
        .get
    }

    private[Speculator] def getSpecParser (specArg: String, nameOpt: Option[String]) : SpecParser = () => {
      nameOpt.map[Option[SimpleFeatureType]] { featureName =>
        Try { SimpleFeatureTypes.createType(featureName, specArg) }
        match {
          case Success(sft) => Some(sft)
          case Failure(ex)  =>
            logger.debug(s"Unable to parse sft spec from string $specArg with error ex.getMessage")
            None
        }
      }.getOrElse(Option.empty)
    }

    private[Speculator] def getConfParser(str: String): SpecParser = () =>
      Try { SimpleFeatureTypes.createType(ConfigFactory.parseString(str)) }
      match {
        case Success(sft) => Some(sft)
        case Failure(ex)  =>
          logger.debug(s"Unable to parse sft conf from string $str with error ex.getMessage")
          Option.empty
      }

    def readFile(s: String): Option[String] = {
      val f = new File(s)
      if (f.exists && f.canRead) {
        val reader = new BufferedReader(new InputStreamReader(new FileInputStream(f)))
        val contents = try {
          IOUtils.toString(reader)
        } finally {
          reader.close()
        }
        Some(contents)
      } else None
    }

  }
}
/* get password trait */
trait GetPassword {
  def getPassword(pass: String) = Option(pass).getOrElse({
    if (System.console() != null) {
      System.err.print("Password (mask enabled)> ")
      System.console().readPassword().mkString
    } else {
      System.err.print("Password (mask disabled when redirecting output)> ")
      val reader = new BufferedReader(new InputStreamReader(System.in))
      reader.readLine()
    }
  })
}

/**
 * Loads accumulo properties for instance and zookeepers from the accumulo installation found via
 * the system path in ACCUMULO_HOME in the case that command line parameters are not provided
 */
trait AccumuloProperties extends GetPassword with Logging {
  lazy val accumuloConf = XML.loadFile(s"${System.getenv("ACCUMULO_HOME")}/conf/accumulo-site.xml")

  lazy val zookeepersProp =
    (accumuloConf \\ "property")
    .filter { x => (x \ "name").text == "instance.zookeeper.host" }
    .map { y => (y \ "value").text }
    .head

  lazy val instanceDfsDir =
    Try(
      (accumuloConf \\ "property")
      .filter { x => (x \ "name").text == "instance.dfs.dir" }
      .map { y => (y \ "value").text }
      .head)
    .getOrElse("/accumulo")

  def instanceIdStr = HdfsZooInstance.getInstance().getInstanceID

  def instanceName = HdfsZooInstance.getInstance().getInstanceName
}
