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

/**
 *  FeatureArguments, ExportArguments, and IngestArguments are case classes used by scopt for command
 *  line parsing. Arguments are stored in their respective case class and then used in individual
 *  classes where commands get executed.
 */

object Utils {

  object IngestParams {
    val ACCUMULO_INSTANCE   = "geomesa-tools.ingest.instance"
    val ZOOKEEPERS          = "geomesa-tools.ingest.zookeepers"
    val ACCUMULO_MOCK       = "geomesa-tools.ingest.useMock"
    val ACCUMULO_USER       = "geomesa-tools.ingest.user"
    val ACCUMULO_PASSWORD   = "geomesa-tools.ingest.password"
    val AUTHORIZATIONS      = "geomesa-tools.ingest.authorizations"
    val VISIBILITIES        = "geomesa-tools.ingest.visibilities"
    val SHARDS              = "geomesa-tools.ingest.shards"
    val INDEX_SCHEMA_FMT    = "geomesa-tools.ingest.indexSchemaFormat"
    val SKIP_HEADER         = "geomesa-tools.ingest.skipHeader"
    val DO_HASH             = "geomesa-tools.ingest.doHash"
    val DT_FORMAT           = "geomesa-tools.ingest.dtFormat"
    val ID_FIELDS           = "geomesa-tools.ingest.idFields"
    val DT_FIELD            = "geomesa-tools.ingest.dtField"
    val FILE_PATH           = "geomesa-tools.ingest.path"
    val FORMAT              = "geomesa-tools.ingest.delimiter"
    val LON_ATTRIBUTE       = "geomesa-tools.ingest.lonAttribute"
    val LAT_ATTRIBUTE       = "geoemsa-tools.ingest.latAttribute"
    val FEATURE_NAME        = "geomesa-tools.feature.name"
    val CATALOG_TABLE       = "geomesa-tools.feature.tables.catalog"
    val SFT_SPEC            = "geomesa-tools.feature.sftspec"
    val COLS                = "geomesa-tools.ingest.cols"
    val IS_TEST_INGEST      = "geomesa-tools.ingest.runIngest"

  }

}

case class FeatureArguments(username: String = null,
                            password: Option[String] = None,
                            mode: String = null,
                            spec: String = null,
                            dtField: Option[String] = None,
                            method: String = "local",
                            featureName: String = null,
                            catalog: String = null,
                            query: String = null,
                            param: String = null,
                            newValue: String = null,
                            suffix: String = null,
                            instanceName: Option[String] = None,
                            zookeepers: Option[String] = None,
                            visibilities: Option[String] = None,
                            auths: Option[String] = None,
                            toStdOut: Boolean = false,
                            forceDelete: Boolean = false,
                            maxShards: Option[Int] = None,
                            sharedTable: Option[Boolean] = Some(true))


case class ExportArguments(username: String = null,
                           password: Option[String] = None,
                           mode: String = null,
                           featureName: String = null,
                           format: String = null,
                           toStdOut: Boolean = false,
                           catalog: String = null,
                           maxFeatures: Option[Int] = None,
                           attributes: Option[String] = None,
                           lonAttribute: Option[String] = None,
                           latAttribute: Option[String] = None,
                           query: Option[String] = None,
                           instanceName: Option[String] = None,
                           zookeepers: Option[String] = None,
                           visibilities: Option[String] = None,
                           auths: Option[String] = None,
                           idFields: Option[String] = None,
                           dtField: Option[String] = None)


case class IngestArguments(username: String = null,
                           password: Option[String] = None,
                           instanceName: Option[String] = None,
                           zookeepers: Option[String] = None,
                           catalog: String = null,
                           auths: Option[String] = None,
                           visibilities: Option[String] = None,
                           indexSchemaFmt: Option[String] = None,
                           spec: String = null,
                           cols: Option[String] = None,
                           idFields: Option[String] = None,
                           dtField: Option[String] = None,
                           dtFormat: Option[String] = None,
                           file: String = null,
                           featureName: String = null,
                           lonAttribute: Option[String] = None,
                           latAttribute: Option[String] = None,
                           doHash: Boolean = false,
                           maxShards: Option[Int] = None,
                           dryRun: Boolean = false,
                           sharedTable: Option[Boolean] = Some(true))


/* get password trait */
trait GetPassword {
  def password(s: Option[String]) = s.getOrElse({
    val standardIn = System.console()
    print("Password> ")
    standardIn.readPassword().mkString
  })
}

/* Accumulo properties trait */
trait AccumuloProperties extends Logging {
  lazy val accumuloConf = XML.loadFile(s"${System.getenv("ACCUMULO_HOME")}/conf/accumulo-site.xml")
  lazy val zookeepers = (accumuloConf \\ "property")
    .filter(x => (x \ "name")
    .text == "instance.zookeeper.host")
    .map(y => (y \ "value").text)
    .head
  lazy val instanceDfsDir = Try((accumuloConf \\ "property")
    .filter(x => (x \ "name")
    .text == "instance.dfs.dir")
    .map(y => (y \ "value").text)
    .head)
    .getOrElse("/accumulo")
  lazy val instanceIdStr = Try(ZooKeeperInstance.getInstanceIDFromHdfs(new Path(instanceDfsDir, "instance_id"))).getOrElse({
    logger.error(
      "Error retrieving /accumulo/instance_id from HDFS. To resolve this, double check that the \n" +
      "HADOOP_CONF_DIR environment variable is set. If that does not work, specify your \n" +
      "Accumulo Instance Name as an argument with the --instance-name flag.")
    sys.exit()
  })
  lazy val instanceName = new ZooKeeperInstance(UUID.fromString(instanceIdStr), zookeepers).getInstanceName
}

