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
class ReplayKafkaDataStoreProcess(val catalog: Catalog) extends GeomesaKafkaProcess with Logging{
  @DescribeResult(name = "result", description = "Name of the Layer created for the Kafka Window")
  def execute(
              @DescribeParameter(name = "features",  description = "Source GeoServer Feature Collection")
              features: SimpleFeatureCollection, //Todo: check for is kafka
              @DescribeParameter(name = "workspace", description = "Target workspace, created if missing.")
              workspace: String,
              @DescribeParameter(name = "store",     description = "Target store")
              store: String,      //Todo: this might be removable...
              @DescribeParameter(name = "startTime", description = "Start Time of the replay window")
              startTime: Integer, //Todo: change to something that can be used for Joda constructor
              @DescribeParameter(name = "endTime",   description = "End Time of the replay window")
              endTime: Integer,   //Todo: change to something that can be used for Joda constructor
              @DescribeParameter(name = "readBehind", description = "The amount of time to pre-read")
              readBehind: java.lang.Long
              ): String = {

    val workspaceInfo: WorkspaceInfo = getWorkspace(workspace)

    val catalogBuilder = new CatalogBuilder(catalog)
    catalogBuilder.setWorkspace(workspaceInfo)

    //TODO: Make sure we create the new store, parse replay config things!
    val newStore = catalog.getFactory.createDataStore()
    newStore.setWorkspace(workspaceInfo)
    newStore.setName(store) // possibly the store parameter given above, or can be removed
    newStore.setDescription("Volatile Kafka Replay Layer")
    newStore.setType("Kafka Replay Data Store") // TODO: this is naive...
    newStore.setEnabled(true)
    catalog.add(newStore) // TODO: should I be using catalog.add or catalogBuilder.attach?

    // grab out the Store we just added, this may be unnecessary
    val storeInfo: DataStoreInfo = Option(catalog.getDataStoreByName(workspaceInfo.getName, store)).getOrElse {
      throw new ProcessException(s"Unable to find store $store in workspace $workspace")
    }
    //create volatile SFT, todo: will need to use replay config here
    val volatileSFT = createVolatileSFT(features, storeInfo, workspaceInfo.getName)

    // set the catalogBuilder to our store
    catalogBuilder.setStore(storeInfo)

    // add well known volatile keyword
    val volatileTypeInfo = catalogBuilder.buildFeatureType(volatileSFT.getName)
    val volatileKW: Keyword = new Keyword("kafka.geomesa.volatile") //Todo: make this accessible to other things
    volatileTypeInfo.getKeywords.add(volatileKW)

    // do some setup
    catalogBuilder.setupBounds(volatileTypeInfo)

    // build the layer and add it to geoserver
    val volatileLayerInfo = catalogBuilder.buildLayer(volatileTypeInfo)
    catalog.add(volatileTypeInfo)
    catalog.add(volatileLayerInfo)

    s"created workspace: ${workspaceInfo.getName} and layer: ${volatileLayerInfo.getName}"
  }

  private def createVolatileSFT(features: SimpleFeatureCollection,
                                storeInfo: DataStoreInfo,
                                workspace: String): SimpleFeatureType = {
    // TODO: wire in bits for replay config
    // get source layer SFT
    val extantSFT: SimpleFeatureType = features.getSchema
    val extantSftName = extantSFT.getTypeName
    // create new sft
    val targetSftName = extantSftName+s"_volatile_Something" // todo: figure out new sft name
    val sftBuilder = new SimpleFeatureTypeBuilder()
    sftBuilder.init(extantSFT)
    sftBuilder.setName(targetSftName)
    val destinationSFT = sftBuilder.buildFeatureType()
    val ds = storeInfo.getDataStore(null).asInstanceOf[ContentDataStore]
    ds.createSchema(destinationSFT)
    //verify by retrieving the stored sft
    val storedSFT = ds.getSchema(destinationSFT.getName)
    // check if layer exists
    if (checkForLayer(workspace, storedSFT.getTypeName))
      throw new ProcessException(s"Target layer already exists for SFT: ${storedSFT.getTypeName}")
    storedSFT
  }

  private def getWorkspace(workspace: String): WorkspaceInfo = Option(catalog.getWorkspaceByName(workspace)) match {
    case Some(wsi) => wsi
    case _         =>
      logger.info(s"Could not find workspace $workspace Attempting to create.")
      val attempt = Try {
        val ws = catalog.getFactory.createWorkspace()
        val ns = catalog.getFactory.createNamespace()
        ws.setName(workspace)
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

  private def checkForLayer(workspace: String, sftTypeName: String): Boolean = {
    val layerName = s"$workspace:$sftTypeName"
    val layer = catalog.getLayerByName(layerName)
    if (layer == null) false else true
  }

}
