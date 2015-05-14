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

import scala.util.matching.Regex

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
              @DescribeParameter(name = "startTime", description = "POSIX Start Time of the replay window.")
              startTime: java.lang.Long,
              @DescribeParameter(name = "endTime", description = "POSIX End Time of the replay window.")
              endTime: java.lang.Long,
              @DescribeParameter(name = "readBehind", description = "The amount of time to pre-read in milliseconds.")
              readBehind: java.lang.Long
              ): String = {

    val sourceWorkSpaceInfo: WorkspaceInfo = getWorkSpace(sourceWorkspace)

    val sourceStoreInfo: DataStoreInfo = Option(catalog.getDataStoreByName(sourceWorkSpaceInfo.getName, sourceStore)).getOrElse {
      throw new ProcessException(s"Unable to find store $sourceStore in source workspace $sourceWorkspace")
    }
    // set the catalogBuilder to our store
    val catalogBuilder = new CatalogBuilder(catalog)
    catalogBuilder.setWorkspace(sourceWorkSpaceInfo)
    catalogBuilder.setStore(sourceStoreInfo)

    // create volatile SFT, todo: will need to use replay config here
    val volatileSFT = createVolatileSFT(features, sourceStoreInfo)
    // update store to save new metadata
    catalog.save(sourceStoreInfo)

    // check for existing layer
    if (checkForLayer(sourceWorkSpaceInfo.getName, volatileSFT.getTypeName))
      throw new ProcessException(s"Target layer already exists for SFT: ${volatileSFT.getTypeName}")

    // add a well known volatile keyword
    val volatileTypeInfo = catalogBuilder.buildFeatureType(volatileSFT.getName)

    // do some setup
    catalogBuilder.setupBounds(volatileTypeInfo)

    // build the layer and mark as volatile
    val volatileLayerInfo = catalogBuilder.buildLayer(volatileTypeInfo)
    // add the name of the volatile SFT associated with this new layer
    volatileLayerInfo.getMetadata.put(volatileHint, volatileHint)
    volatileLayerInfo.getMetadata.put(volatileLayerSftHint, volatileSFT.getTypeName)
    // Add new layer with hints to GeoServer
    catalog.add(volatileTypeInfo)
    catalog.add(volatileLayerInfo)

    s"created layer: ${volatileLayerInfo.getName}"
  }

  private def createVolatileSFT(features: SimpleFeatureCollection,
                                storeInfo: DataStoreInfo): SimpleFeatureType = {
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
    injectAge(storeInfo, destinationSFT)
    //verify by retrieving the stored sft
    val storedSFT = ds.getSchema(destinationSFT.getName)
    storedSFT
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
  val volatileLayerSftHint: String = "kafka.geomesa.volatile.layer.sft"
  val volatileSftAgeHint: String   = "kafka.geomesa.volatile.age.of.sft "
  val volatileSftPattern: Regex = s"($volatileSftAgeHint)(.*)".r
  val volatileHint: String = "kafka.geomesa.volatile"
  val volatileKW: Keyword = new Keyword(volatileHint)

  private def makeAgeKey(sfts: String): String = volatileSftAgeHint + sfts
  private def makeAgeKey(sft: SimpleFeatureType): String = makeAgeKey(sft.getTypeName)

  def injectAge(dsi: DataStoreInfo, sft: SimpleFeatureType): Unit = {
    dsi.getMetadata.put(makeAgeKey(sft), System.currentTimeMillis())
  }

  def getSftFromKey(key: String): Option[String] = key match {
    case volatileSftPattern(hint, sftname) => Some(sftname)
    case _                                 => None
  }

  def getVolatileAge(dsi: DataStoreInfo, sft: SimpleFeatureType): Option[Long] = {
    getVolatileAge(dsi, sft.getTypeName)
  }

  def getVolatileAge(dsi: DataStoreInfo, sfts: String): Option[Long] = {
    val meta = dsi.getMetadata
    val ageKey = makeAgeKey(sfts)
    meta.containsKey(ageKey) match {
      case true => Some(meta.get(ageKey, classOf[Long]))
      case _    => None
    }
  }
}