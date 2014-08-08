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
package geomesa.tools

import com.typesafe.scalalogging.slf4j.Logging

class Ingest() extends Logging {

  def getAccumuloDataStoreConf(config: ScoptArguments) = Map (
    "instanceId"   ->  sys.env.getOrElse("GEOMESA_INSTANCEID", "instanceId"),
    "zookeepers"   ->  sys.env.getOrElse("GEOMESA_ZOOKEEPERS", "zoo1:2181,zoo2:2181,zoo3:2181"),
    "user"         ->  sys.env.getOrElse("GEOMESA_USER", "admin"),
    "password"     ->  sys.env.getOrElse("GEOMESA_PASSWORD", "admin"),
    "auths"        ->  sys.env.getOrElse("GEOMESA_AUTHS", ""),
    "visibilities" ->  sys.env.getOrElse("GEOMESA_VISIBILITIES", ""),
    "tableName"    ->  config.table
  )

  def defineIngestJob(config: ScoptArguments): Boolean = {
    val dsConfig = getAccumuloDataStoreConf(config)
    config.format.toUpperCase match {
      case "CSV" | "TSV" =>
        config.method.toLowerCase match {
          case "mapreduce" =>
            true
          case "local" =>
            new SVIngest(config, dsConfig)
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