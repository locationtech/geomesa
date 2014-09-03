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
import org.apache.accumulo.core.client.ZooKeeperInstance
import org.apache.hadoop.fs.Path
import scala.util.Try
import scala.xml.XML

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
    val RUN_INGEST          = "geomesa-tools.ingest.runIngest"

  }

}


/*  ScoptArguments is a case Class used by scopt, args are stored in it and default values can be set in Config also.*/
case class ScoptArguments(username: String = null,
                          password: String = null,
                          mode: String = null,
                          spec: String = null,
                          idFields: Option[String] = None,
                          dtField: Option[String] = None,
                          dtFormat: String = null,
                          method: String = "local",
                          featureName: String = null,
                          format: String = null,
                          toStdOut:Boolean = false,
                          catalog: String = null,
                          maxFeatures: Int = -1,
                          filterString: String = null,
                          attributes: String = null,
                          lonAttribute: Option[String] = None,
                          latAttribute: Option[String] = None,
                          query: String = null,
                          param: String = null,
                          newValue: String = null,
                          suffix: String = null)

/*  ScoptArguments is a case Class used by scopt, args are stored in it and default values can be set in Config also.*/
case class IngestArguments(username: String = null,
                           password: String = null,
                           catalog: String = null,
                           auths: Option[String] = None,
                           visibilities: Option[String] = None,
                           indexSchemaFmt: Option[String] = None,
                           spec: String = null,
                           idFields: Option[String] = None,
                           dtField: Option[String] = None,
                           dtFormat: Option[String] = None,
                           method: String = "local",
                           file: String = null,
                           featureName: Option[String] = None,
                           format: Option[String] = None,
                           lonAttribute: Option[String] = None,
                           latAttribute: Option[String] = None,
                           skipHeader: Boolean = false,
                           doHash: Boolean = false,
                           maxShards: Option[Int] = None )

/* get password trait */
trait GetPassword {
  def password(s: String) = s match {
    case pw: String => pw
    case _ =>
      val standardIn = System.console()
      print("Password> ")
      standardIn.readPassword().mkString
  }
}

/* Accumulo properties trait */
trait AccumuloProperties {
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
  val instanceIdStr = ZooKeeperInstance.getInstanceIDFromHdfs(instanceIdDir)
  val instanceName = new ZooKeeperInstance(UUID.fromString(instanceIdStr), zookeepers).getInstanceName
}


