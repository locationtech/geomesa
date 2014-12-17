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

import java.io.{BufferedReader, InputStreamReader}
import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.hadoop.fs.Path

import scala.util.{Failure, Success, Try}
import scala.xml.XML

object Utils {

  object IngestParams {
    val ACCUMULO_INSTANCE   = "geomesa.tools.ingest.instance"
    val ZOOKEEPERS          = "geomesa.tools.ingest.zookeepers"
    val ACCUMULO_MOCK       = "geomesa.tools.ingest.useMock"
    val ACCUMULO_USER       = "geomesa.tools.ingest.user"
    val ACCUMULO_PASSWORD   = "geomesa.tools.ingest.password"
    val AUTHORIZATIONS      = "geomesa.tools.ingest.authorizations"
    val VISIBILITIES        = "geomesa.tools.ingest.visibilities"
    val SHARDS              = "geomesa.tools.ingest.shards"
    val INDEX_SCHEMA_FMT    = "geomesa.tools.ingest.indexSchemaFormat"
    val SKIP_HEADER         = "geomesa.tools.ingest.skipHeader"
    val DO_HASH             = "geomesa.tools.ingest.doHash"
    val DT_FORMAT           = "geomesa.tools.ingest.dtFormat"
    val ID_FIELDS           = "geomesa.tools.ingest.idFields"
    val DT_FIELD            = "geomesa.tools.ingest.dtField"
    val FILE_PATH           = "geomesa.tools.ingest.path"
    val FORMAT              = "geomesa.tools.ingest.delimiter"
    val LON_ATTRIBUTE       = "geomesa.tools.ingest.lonAttribute"
    val LAT_ATTRIBUTE       = "geomesa.tools.ingest.latAttribute"
    val FEATURE_NAME        = "geomesa.tools.feature.name"
    val CATALOG_TABLE       = "geomesa.tools.feature.tables.catalog"
    val SFT_SPEC            = "geomesa.tools.feature.sftspec"
    val COLS                = "geomesa.tools.ingest.cols"
    val IS_TEST_INGEST      = "geomesa.tools.ingest.runIngest"
  }

  object Formats {
    val CSV     = "csv"
    val TSV     = "tsv"
    val TIFF    = "tiff"
    val DTED    = "dted"
    val SHP     = "shp"
    val JSON    = "json"
    val GeoJson = "geojson"
    val GML = "gml"

    def getFileExtension(name: String) =
      name.toLowerCase match {
        case _ if name.endsWith(CSV)  => CSV
        case _ if name.endsWith("tif") ||
                  name.endsWith("tiff") => TIFF
        case _ if name.endsWith("dt0") ||
                  name.endsWith("dt1") ||
                  name.endsWith("dt2")=> DTED
        case _ if name.endsWith(TSV)  => TSV
        case _ if name.endsWith(SHP)  => SHP
        case _ if name.endsWith(JSON) => JSON
        case _ if name.endsWith(GML)  => GML
        case _                        => "unknown"
      }

    val All = List(CSV, TSV, SHP, JSON, GeoJson, GML)
  }

  object Modes {
    val Local = "local"
    val Hdfs = "hdfs"

    def getMode(filename: String) = if (filename.toLowerCase.trim.startsWith("hdfs://")) Hdfs else Local

    def getModeFlag(filename: String) = "--" + getMode(filename)
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

  lazy val instanceIdStr =
    Try(ZooKeeperInstance.getInstanceIDFromHdfs(new Path(instanceDfsDir, "instance_id"))) match {
      case Success(value) => value
      case Failure(ex) =>
        throw new Exception("Error retrieving /accumulo/instance_id from HDFS. To resolve this, double check that " +
          "the HADOOP_CONF_DIR environment variable is set. If that does not work, specify your Accumulo Instance " +
          "Name as an argument with the --instance-name flag.", ex)
    }

  lazy val instanceName = new ZooKeeperInstance(UUID.fromString(instanceIdStr), zookeepersProp).getInstanceName
}
