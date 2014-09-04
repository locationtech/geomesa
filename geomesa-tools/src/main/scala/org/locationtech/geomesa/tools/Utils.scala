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
                          forceDelete: Boolean = false)

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

/*  ScoptArguments is a case Class used by scopt, args are stored in it and default values can be set in Config also.*/
case class IngestArguments(username: String = null,
                           password: Option[String] = None,
                           catalog: String = null,
                           auths: Option[String] = None,
                           visibilities: Option[String] = None,
                           indexSchemaFormat: Option[String] = None,
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
                           maxShards: Option[Int] = None,
                           instanceName: Option[String] = None,
                           zookeepers: Option[String] = None)

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

