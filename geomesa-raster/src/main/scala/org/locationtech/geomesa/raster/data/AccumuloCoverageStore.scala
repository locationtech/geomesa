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


package org.locationtech.geomesa.raster.data

import java.io.Serializable
import java.util.{Map => JMap}

import com.typesafe.scalalogging.slf4j.Logging
import org.apache.accumulo.core.client.Connector
import org.apache.accumulo.core.security.Authorizations
import org.geotools.factory.Hints
import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory._
import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._
import org.locationtech.geomesa.raster.AccumuloStoreHelper
import org.locationtech.geomesa.raster.ingest.GeoserverClientService
import org.locationtech.geomesa.raster.util.RasterUtils._
import org.locationtech.geomesa.utils.stats.Timings

trait CoverageStore {
  def getAuths: Authorizations
  def getVisibility: String
  def getConnector: Connector
  def getTable: String
  def saveRaster(raster: Raster): Unit
  def registerToGeoServer(raster: Raster): Unit
  def getRasters(rasterQuery: RasterQuery)(implicit timing: Timings): Iterator[Raster]
}

/**
 *
 *  This class handles operations on a coverage, including cutting coverage into chunks
 *  and resembling chunks to a coverage, saving/retrieving coverage to/from data source,
 *  and registering coverage to Geoserver.
 *
 * @param rasterStore Raster store instance
 * @param geoserverClientService Optional GeoServer client instance
 */
class AccumuloCoverageStore(val rasterStore: AccumuloRasterStore,
                            val geoserverClientService: Option[GeoserverClientService] = None)
  extends CoverageStore with Logging {

  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)
  // Ensure the Table and Bounds Table both exist
  rasterStore.createTableStructure()

  def getAuths = rasterStore.getAuths

  def getVisibility = rasterStore.getVisibility

  def getConnector = rasterStore.getConnector

  def getTable = rasterStore.getTable

  def getRasters(rasterQuery: RasterQuery)(implicit timing: Timings): Iterator[Raster] = rasterStore.getRasters(rasterQuery)

  def saveRaster(raster: Raster) = rasterStore.putRaster(raster)

  def getQueryRecords(numRecords: Int): Iterator[String]  = rasterStore.getQueryRecords(numRecords)

  def registerToGeoServer(raster: Raster) {
    geoserverClientService.foreach { geoserverClientService => {
      registerToGeoServer(raster, geoserverClientService)
      logger.debug(s"Register raster ${raster.id} to geoserver at ${geoserverClientService.geoserverUrl}")
    }}
  }

  private def registerToGeoServer(raster: Raster, geoserverClientService: GeoserverClientService) {
    geoserverClientService.registerRasterStyles()
    geoserverClientService.registerRaster(raster.id, raster.id, "Raster data", None)
  }
}

object AccumuloCoverageStore extends Logging {
  def apply(username: String,
            password: String,
            instanceId: String,
            zookeepers: String,
            tableName: String,
            auths: String,
            writeVisibilities: String): AccumuloCoverageStore = {

    val rs = AccumuloRasterStore(username, password, instanceId, zookeepers,
                         tableName, auths, writeVisibilities)

    new AccumuloCoverageStore(rs, None)
  }

  def apply(config: JMap[String, Serializable]): AccumuloCoverageStore = {
    val userName = userParam.lookUp(config).asInstanceOf[String]
    val password = passwordParam.lookUp(config).asInstanceOf[String]
    val instanceId = instanceIdParam.lookUp(config).asInstanceOf[String]
    val zookeepers = zookeepersParam.lookUp(config).asInstanceOf[String]
    val authorizations = AccumuloStoreHelper.getAuthorization(config)
    val visibility = AccumuloStoreHelper.getVisibility(config)
    val tableName = tableNameParam.lookUp(config).asInstanceOf[String]
    val useMock = java.lang.Boolean.valueOf(mockParam.lookUp(config).asInstanceOf[String])
    val shardsConfig = shardsParam.lookupOpt(config)
    val writeMemoryConfig = writeMemoryParam.lookupOpt(config)
    val writeThreadsConfig = writeThreadsParam.lookupOpt(config)
    val queryThreadsConfig = queryThreadsParam.lookupOpt(config)

    val rasterStore = AccumuloRasterStore(userName, password, instanceId, zookeepers,
                                  tableName, visibility, authorizations, useMock,
                                  shardsConfig, writeMemoryConfig, writeThreadsConfig,
                                  queryThreadsConfig)

    val dsConnectConfig: Map[String, String] = Map(
      IngestRasterParams.ACCUMULO_INSTANCE -> instanceId,
      IngestRasterParams.ZOOKEEPERS        -> zookeepers,
      IngestRasterParams.ACCUMULO_USER     -> userName,
      IngestRasterParams.ACCUMULO_PASSWORD -> password,
      IngestRasterParams.TABLE             -> tableName,
      IngestRasterParams.AUTHORIZATIONS    -> authorizations,
      IngestRasterParams.VISIBILITIES      -> visibility
    )

    val GeoServerConfig = geoserverParam.lookUp(config).asInstanceOf[Option[String]]
    val GeoServerClientService: Option[GeoserverClientService] = GeoServerConfig match {
      case Some(gsconfig) =>
        val gsConnectConfig: Map[String, String] =
          gsconfig.split(",").map(_.split("=") match {
            case Array(s1, s2) => (s1, s2)
            case _ =>
              logger.error("Failed to instantiate Geoserver client service: wrong parameters.")
              sys.exit()
          }).toMap
        Some(new GeoserverClientService(dsConnectConfig ++ gsConnectConfig))
      case _              => None
    }

    new AccumuloCoverageStore(rasterStore, GeoServerClientService)
  }

}

