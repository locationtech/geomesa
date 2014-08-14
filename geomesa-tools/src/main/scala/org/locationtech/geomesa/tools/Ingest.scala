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

class Ingest() extends Logging {
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

  def getAccumuloDataStoreConf(config: ScoptArguments, password: String) = Map (
    "instanceId"   ->  instanceName,
    "zookeepers"   ->  zookeepers,
    "user"         ->  config.username,
    "password"     ->  password,
    "auths"        ->  sys.env.getOrElse("GEOMESA_AUTHS", ""),
    "visibilities" ->  sys.env.getOrElse("GEOMESA_VISIBILITIES", ""),
    "tableName"    ->  config.catalog
  )

  def defineIngestJob(config: ScoptArguments, password: String): Boolean = {
    //ensure that geomesa classes are loaded so that the subsequent
    val dsConfig = getAccumuloDataStoreConf(config, password)
    config.format.toUpperCase match {
      case "CSV" | "TSV" =>
        config.method.toLowerCase match {
          case "local" =>
            logger.info("Ingest has started, please wait.")
            val ingest = new SVIngest(config, dsConfig)
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