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

/*  ScoptArguments is a case Class used by scopt, args are stored in it and default values can be set in Config also.*/
case class ScoptArguments(username: String = null, password: String = null,
                          mode: String = null, spec: String = null, idFields: Option[String] = None,
                          dtField: Option[String] = None, dtFormat: String = null, method: String = "local",
                          file: String = null, featureName: String = null, format: String = null,
                          catalog: String = null, maxFeatures: Int = -1, defaultDate: String = null,
                          filterString: String = null, attributes: String = null,
                          lonAttribute: Option[String] = None, latAttribute: Option[String] = None,
                          query: String = null, skipHeader: Boolean = false)

/*  ScoptArguments is a case Class used by scopt, args are stored in it and default values can be set in Config also.*/
case class IngestArguments(username: String = null, password: String = null, spec: String = null,
                           idFields: Option[String] = None, dtField: Option[String] = None, dtFormat: String = null,
                           method: String = "local", file: String = null, featureName: String = null,
                           format: String = null, catalog: String = null, lonAttribute: Option[String] = None,
                           latAttribute: Option[String] = None, skipHeader: Boolean = false, doHash: Boolean = false,
                           auths: Option[String] = None, visibilities: Option[String] = None,
                           indexSchemaFormat: Option[String] = None, maxShards: Option[Int] = None )

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


