/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.kafka.plugin

import java.lang.{Long => JLong}

import com.typesafe.scalalogging.slf4j.Logging
import org.geoserver.catalog._
import org.geotools.data.DataStore
import org.geotools.process.ProcessException
import org.geotools.process.factory.{DescribeParameter, DescribeProcess, DescribeResult}
import org.joda.time.{Duration, Instant}
import org.locationtech.geomesa.kafka.{KafkaDataStoreHelper, ReplayConfig}
import org.opengis.feature.`type`.Name
import org.opengis.feature.simple.SimpleFeatureType

import scala.language.implicitConversions

@DescribeProcess(
  title = "GeoMesa Build Replay From KafkaDataStore",
  description = "Builds a replay layer from a defined window of time on a KafkaDataStore",
  version = "1.0.0"
)
class ReplayKafkaDataStoreProcess(val catalog: Catalog) extends GeomesaKafkaProcess with Logging {

  import org.locationtech.geomesa.kafka.plugin.VolatileLayer._

  @DescribeResult(name = "result", description = "Name of the Layer created for the Kafka Window")
  def execute(
              @DescribeParameter(name = "workspace", description = "Workspace containing the live layer.  The replay layer will be added here.")
              workspace: String,
              @DescribeParameter(name = "layer", description = "Source live layer.")
              layer: String,
              @DescribeParameter(name = "startTime", description = "Start Time of the replay window (UTC).")
              startTime: java.util.Date,
              @DescribeParameter(name = "endTime", description = "End Time of the replay window (UTC).")
              endTime: java.util.Date,
              @DescribeParameter(name = "readBehind", description = "The amount of time to pre-read in milliseconds.")
              readBehind: java.lang.Long
              ): String = {

    val workspaceInfo = getWorkspaceInfo(workspace)
    val layerInfo = getLayerInfo(workspaceInfo, layer)

    val replayConfig = new ReplayConfig(
      new Instant(startTime.getTime), new Instant(endTime.getTime), Duration.millis(readBehind))

    execute(workspaceInfo, layerInfo, replayConfig)
  }

  def execute(workspaceInfo: WorkspaceInfo, layerInfo: LayerInfo, replayConfig: ReplayConfig): String = {

    val featureInfo = getFeatureInfo(layerInfo)
    val storeInfo = featureInfo.getStore

    val sftName = featureInfo.getQualifiedNativeName

    // create replay SFT
    val replaySFT = createReplaySFT(storeInfo, sftName, replayConfig)

    // set the catalogBuilder to our store
    val catalogBuilder = new CatalogBuilder(catalog)
    catalogBuilder.setWorkspace(workspaceInfo)
    catalogBuilder.setStore(storeInfo)
    
    // build the feature type info
    val replayFeatureInfo = catalogBuilder.buildFeatureType(replaySFT.getName)
    catalogBuilder.setupBounds(replayFeatureInfo)

    // build the layer info and mark as volatile
    val replayLayerInfo = catalogBuilder.buildLayer(replayFeatureInfo)
    injectMetadata(replayLayerInfo, replaySFT)

    // add new layer with hints to GeoServer
    catalog.add(replayFeatureInfo)
    catalog.add(replayLayerInfo)

    s"created layer: ${replayLayerInfo.getName}"
  }

  private def getWorkspaceInfo(workspaceName: String): WorkspaceInfo =
    Option(catalog.getWorkspaceByName(workspaceName)).getOrElse {
      throw new ProcessException(s"Unable to find workspace $workspaceName.")
    }

  private def getLayerInfo(wi: WorkspaceInfo, layerName: String): LayerInfo = {
    val workspaceName = wi.getName
    Option(catalog.getLayerByName(s"$workspaceName:$layerName")).getOrElse {
      throw new ProcessException(s"Unable to find layer $layerName in workspace $workspaceName.")
    }
  }

  private def getFeatureInfo(li: LayerInfo): FeatureTypeInfo =
    GeoServerUtils.getFeatureTypeInfo(li).getOrElse {
      throw new ProcessException(s"Unable to get feature info from layer ${li.getName}.")
    }

  private def createReplaySFT(storeInfo: DataStoreInfo,
                              liveSftName: Name,
                              config: ReplayConfig): SimpleFeatureType = {

    val ds = GeoServerUtils.getDataStore(storeInfo).getOrElse(throw new ProcessException(
      s"Store '${storeInfo.getName}' is not a DataStore."))

    val liveSFT = ds.getSchema(liveSftName)
    val replaySFT = KafkaDataStoreHelper.createReplaySFT(liveSFT, config)

    // check for existing layer
    if (checkForLayer(storeInfo.getWorkspace.getName, replaySFT.getTypeName)) {
      throw new ProcessException(s"Target layer already exists for SFT: ${replaySFT.getTypeName}")
    }

    ds.createSchema(replaySFT)

    //verify by retrieving the stored sft
    ds.getSchema(replaySFT.getName)
  }

  private def checkForLayer(workspace: String, sftTypeName: String): Boolean = {
    val layerName = s"$workspace:$sftTypeName"
    catalog.getLayerByName(layerName) != null
  }

}

/** General purpose utils, not specific to replay layers.
  * 
  */
object GeoServerUtils {

  def getFeatureTypeInfo(li: LayerInfo): Option[FeatureTypeInfo] = li.getResource match {
    case fti: FeatureTypeInfo => Some(fti)
    case _ => None
  }

  def getSimpleFeatureType(fti: FeatureTypeInfo): Option[SimpleFeatureType] = fti.getFeatureType match {
    case sft: SimpleFeatureType => Some(sft)
    case _ => None
  }

  def getDataStore(dsi: DataStoreInfo): Option[DataStore] = dsi.getDataStore(null) match {
    case ds: DataStore => Some(ds)
    case _ => None
  }
}

object VolatileLayer {
  val volatileLayerSftHint: String = "geomesa.volatile.layer.sft"
  val volatileLayerAgeHint: String = "geomesa.volatile.layer.age"

  def injectMetadata(info: LayerInfo, sft: SimpleFeatureType): Unit = {
    info.getMetadata.put(volatileLayerSftHint, sft.getTypeName)
    info.getMetadata.put(volatileLayerAgeHint, System.currentTimeMillis().asInstanceOf[JLong])
  }

  def getSftName(info: LayerInfo): Option[String] =
    Option(info.getMetadata.get(volatileLayerSftHint, classOf[String]))

  def getVolatileAge(info: LayerInfo): Option[Instant] =
    Option(new Instant(info.getMetadata.get(volatileLayerAgeHint, classOf[JLong])))
}