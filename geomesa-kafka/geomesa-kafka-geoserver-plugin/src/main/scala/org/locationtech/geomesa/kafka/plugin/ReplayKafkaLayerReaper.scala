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
import org.geoserver.catalog.{DataStoreInfo, WorkspaceInfo, LayerInfo, Catalog}
import org.geotools.process.factory.{DescribeResult, DescribeProcess}

import scala.collection.JavaConversions._
import scala.util.Try

@DescribeProcess(
  title = "GeoMesa Replay Kafka Layer Reaper",
  description = "Removes Kafka Replay Layers from GeoServer",
  version = "1.0.0"
)
class ReplayKafkaLayerReaper(val catalog: Catalog) extends GeomesaKafkaProcess with Logging {
  import org.locationtech.geomesa.kafka.plugin.ReplayKafkaDataStoreProcess._
  @DescribeResult(name = "result", description = "If removal was successful, true.")
  def execute(): Boolean = {
    Try {
      val replayWorkspaces = catalog.getWorkspaces.toList.flatMap(isReplayWorkspace)
      // remove volatile schemas from replay workspaces
      val replayDataStores = replayWorkspaces.flatMap(x => catalog.getDataStoresByWorkspace(x).toList)
      replayDataStores.foreach(removeSchema)
      // remove volatile layers
      val replayLayers = catalog.getLayers.toList.flatMap(isReplayKafkaLayer)
      replayLayers.foreach(catalog.remove)
    }.isSuccess
  }

  private def isReplayWorkspace(w: WorkspaceInfo) = {
    if (w.getMetadata.containsKey(volatileHint)) Some(w) else None
  }

  private def removeSchema(dsi: DataStoreInfo) = {
    val ds = dsi.getDataStore(null)
    ds.getNames.filter(_.getLocalPart.toLowerCase.contains("volatile")).foreach(ds.removeSchema)
  }

  private def isReplayKafkaLayer(l: LayerInfo) = {
    if (l.getMetadata.containsKey(volatileHint)) Some(l) else None // No idea if this will work
  }

}
