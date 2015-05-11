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

import java.util.UUID

import com.typesafe.scalalogging.slf4j.Logging
import org.geoserver.catalog._
import org.geotools.data.simple.SimpleFeatureCollection
import org.geotools.data.store.ContentDataStore
import org.geotools.feature.simple.SimpleFeatureTypeBuilder
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.opengis.feature.simple.SimpleFeatureType

import scala.util.Try

@DescribeProcess(
  title = "GeoMesa Build Replay From KafkaDataStore",
  description = "Builds a replay layer from a defined window of time on a KafkaDataStore",
  version = "1.0.0"
)
class ReplayKafkaDataStoreProcess(val catalog: Catalog) extends GeomesaKafkaProcess with Logging {
  import org.locationtech.geomesa.kafka.plugin.ReplayKafkaDataStoreProcess._
  @DescribeResult(name = "result", description = "Name of the Layer created for the Kafka Window")
  def execute(
              @DescribeParameter(name = "features", description = "Source GeoServer Feature Collection, used for SFT.")
              features: SimpleFeatureCollection,
              @DescribeParameter(name = "source workspace", description = "Workspace of Source Store.")
              sourceWorkspace: String,
              @DescribeParameter(name = "store", description = "Name of Source Store")
              sourceStore: String,
              @DescribeParameter(name = "target workspace", description = "Target workspace, created if missing.")
              targetWorkspace: String,
              @DescribeParameter(name = "startTime", description = "Start Time of the replay window")
              startTime: Integer, //Todo: change to something that can be used for Joda constructor
              @DescribeParameter(name = "endTime", description = "End Time of the replay window")
              endTime: Integer,   //Todo: change to something that can be used for Joda constructor
              @DescribeParameter(name = "readBehind", description = "The amount of time to pre-read")
              readBehind: java.lang.Long
              ): String = {

    val sourceWorkSpaceInfo: WorkspaceInfo = getWorkSpace(sourceWorkspace)
    val volatileWorkSpaceInfo: WorkspaceInfo = getVolatileWorkSpace(targetWorkspace)

    val catalogBuilder = new CatalogBuilder(catalog)
    catalogBuilder.setWorkspace(volatileWorkSpaceInfo)

    // Is there a way of making the store a parameter?
    val sourceStoreInfo: DataStoreInfo = Option(catalog.getDataStoreByName(sourceWorkSpaceInfo.getName, sourceStore)).getOrElse {
      throw new ProcessException(s"Unable to find store $sourceStore in workspace $targetWorkspace")
    }

    //create volatile SFT, todo: will need to use replay config here
    val volatileSFT = createVolatileSFT(features, sourceStoreInfo, volatileWorkSpaceInfo.getName)

    // set the catalogBuilder to our store
    catalogBuilder.setStore(sourceStoreInfo)

    // add well known volatile keyword
    val volatileTypeInfo = catalogBuilder.buildFeatureType(volatileSFT.getName)
    volatileTypeInfo.getKeywords.add(volatileKW) // Todo: this may be redundant, I don't check it

    // do some setup
    catalogBuilder.setupBounds(volatileTypeInfo)

    // build the layer and mark as volatile
    val volatileLayerInfo = catalogBuilder.buildLayer(volatileTypeInfo)
    volatileLayerInfo.getMetadata.put(volatileHint, volatileHint)

    // Add new layer with hints to geoserver
    catalog.add(volatileTypeInfo)
    catalog.add(volatileLayerInfo)

    s"created workspace: ${volatileWorkSpaceInfo.getName} and layer: ${volatileLayerInfo.getName}"
  }

  private def createVolatileSFT(features: SimpleFeatureCollection,
                                storeInfo: DataStoreInfo,
                                targetWorkspace: String): SimpleFeatureType = {
    // Use features just to grab the parent SFT.
    // TODO: wire in bits for replay config
    val extantSFT: SimpleFeatureType = features.getSchema
    val extantSftName = extantSFT.getTypeName
    // create new sft, some of this will be replaced by pending utility method
    val targetSftName = extantSftName+s"_volatile_${UUID.randomUUID()}" // need to create unique sft name, not sure what is best
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.init(extantSFT)
    sftBuilder.setName(targetSftName)
    val destinationSFT = sftBuilder.buildFeatureType()
    val ds = storeInfo.getDataStore(null).asInstanceOf[ContentDataStore]
    ds.createSchema(destinationSFT)
    //verify by retrieving the stored sft
    val storedSFT = ds.getSchema(destinationSFT.getName)
    // check if layer exists
    if (checkForLayer(targetWorkspace, storedSFT.getTypeName))
      throw new ProcessException(s"Target layer already exists for SFT: ${storedSFT.getTypeName}")
    storedSFT
  }

  private def getVolatileWorkSpace(workspace: String): WorkspaceInfo = Option(catalog.getWorkspaceByName(workspace)) match {
    case Some(wsi) => wsi
    case _         =>
      logger.info(s"Could not find workspace $workspace Attempting to create.")
      val attempt = Try {
        val ws = catalog.getFactory.createWorkspace()
        val ns = catalog.getFactory.createNamespace()
        ws.setName(workspace)
        ws.getMetadata.put(volatileHint, volatileHint) // volatile hint needed for cleanup
        ns.setPrefix(ws.getName)
        ns.setURI("http://www.geomesa.org")
        catalog.add(ws)
        catalog.add(ns)
        ws
      } orElse Try {
        logger.warn(s"Could not make workspace $workspace Attempting to use default workspace.")
        catalog.getDefaultWorkspace
      }
      attempt.getOrElse {
        throw new ProcessException(s"Unable to use default workspace.")
      }
  }
  
  private def getWorkSpace(ws: String): WorkspaceInfo = Option(catalog.getWorkspaceByName(ws)) match {
    case Some(wsi) => wsi
    case _         => catalog.getDefaultWorkspace
  }

  private def checkForLayer(workspace: String, sftTypeName: String): Boolean = {
    val layerName = s"$workspace:$sftTypeName"
    val layer = catalog.getLayerByName(layerName)
    if (layer == null) false else true
  }

}

object ReplayKafkaDataStoreProcess {
  val volatileHint: String = "kafka.geomesa.volatile"
  val volatileKW: Keyword = new Keyword(volatileHint) //Todo: make this accessible to other things
}