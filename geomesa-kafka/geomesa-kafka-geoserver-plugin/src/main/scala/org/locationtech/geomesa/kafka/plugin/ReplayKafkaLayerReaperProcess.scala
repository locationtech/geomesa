/*
 * Copyright 2015 Commonwealth Computer Research, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.locationtech.geomesa.kafka.plugin

import com.typesafe.scalalogging.slf4j.Logging
import org.geoserver.catalog.{Catalog, DataStoreInfo, LayerInfo}
import org.geotools.process.factory.{DescribeProcess, DescribeResult}
import org.joda.time.{Duration, Instant}

import scala.collection.JavaConversions._
import scala.util.Try

@DescribeProcess(
  title = "GeoMesa Replay Kafka Layer Reaper",
  description = "Removes Kafka Replay Layers from GeoServer",
  version = "1.0.0"
)
class ReplayKafkaLayerReaperProcess(val catalog: Catalog, val hours: Int)
  extends GeomesaKafkaProcess with Logging with Runnable {
  import org.locationtech.geomesa.kafka.plugin.ReplayKafkaDataStoreProcess._

  implicit def longToInstant(l: Long): Instant = new Instant(l)
  @DescribeResult(name = "result", description = "If removal was successful, true.")
  def execute(): Boolean = {
    Try {
      val currentTime = new Instant(System.currentTimeMillis())
      val ageLimit = currentTime.minus(Duration.standardHours(hours))

      // Get DataStoreInfo Schema pairs for old Schemas
      val oldOnly = for {
        dsi <- catalog.getDataStores
        schema <- getReplaySchemaNames(dsi)
        age <- getVolatileAge(dsi, schema)
        if age.isBefore(ageLimit)
      } yield (dsi, schema)

      // Remove old schemas from DataStores, return flattened list of them
      val removedSchemas = oldOnly.flatMap{ case (dsi, oldSchema) =>
        val ds = dsi.getDataStore(null)
        val oldSchemas = ds.getNames.filter(_.getLocalPart.contains(oldSchema)).toList
        oldSchemas.foreach(ds.removeSchema)
        oldSchemas.map(_.getLocalPart)
      }.toList

      //Todo: remove layers before calling remove schema
      // Remove Layers associated with removed schemas
      val oldReplayLayers = for {
        layer <- catalog.getLayers
        schema <- removedSchemas
        if isOldReplayLayer(layer, schema)
      } yield layer

      oldReplayLayers.foreach(catalog.remove)
    }.isSuccess
  }

  private def getReplaySchemaNames(dsi: DataStoreInfo): List[String] = {
    dsi.getMetadata.keysIterator.flatMap(getSftFromKey).toList
  }

  private def isOldReplayLayer(l: LayerInfo, s: String): Boolean = l.getMetadata.containsValue(s)

  override def run(): Unit = {
    logger.info("Running Replay Kafka Cleaner")
    println("Did the thing")
    execute()
  }
}
