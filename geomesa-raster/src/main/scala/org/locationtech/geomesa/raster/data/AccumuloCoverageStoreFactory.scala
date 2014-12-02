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
import org.locationtech.geomesa.core.stats.StatWriter
import org.locationtech.geomesa.raster.ingest.{IngestRasterParams, GeoserverClientService}

import scala.util.Try

class AccumuloCoverageStoreFactory extends Logging {

  import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory._
  import org.locationtech.geomesa.core.data.AccumuloDataStoreFactory.params._
  import org.locationtech.geomesa.raster.AccumuloStoreHelper

  def createCoverageStore(params: JMap[String, Serializable]): AccumuloCoverageStore = {
    val visibility = AccumuloStoreHelper.getVisibility(params)
    val tableName = tableNameParam.lookUp(params).asInstanceOf[String]
    val useMock = java.lang.Boolean.valueOf(mockParam.lookUp(params).asInstanceOf[String])
    val connector =
      if (params.containsKey(connParam.key)) connParam.lookUp(params).asInstanceOf[Connector]
      else AccumuloStoreHelper.buildAccumuloConnector(params, useMock)
    val authorizationsProvider = AccumuloStoreHelper.getAuthorizationsProvider(params, connector)
    val collectStats = !useMock && Try(statsParam.lookUp(params).asInstanceOf[java.lang.Boolean] == true).getOrElse(false)

    val shardsConfig = shardsParam.lookupOpt(params)
    val writeMemoryConfig = writeMemoryParam.lookupOpt(params)
    val writeThreadsConfig = writeThreadsParam.lookupOpt(params)
    val queryThreadsConfig = queryThreadsParam.lookupOpt(params)

    val rasterOps =
    if (collectStats) {
      new AccumuloBackedRasterOperations(connector,
                                         tableName,
                                         authorizationsProvider,
                                         visibility,
                                         shardsConfig,
                                         writeMemoryConfig,
                                         writeThreadsConfig,
                                         queryThreadsConfig) with StatWriter
    } else {
      new AccumuloBackedRasterOperations(connector,
                                         tableName,
                                         authorizationsProvider,
                                         visibility,
                                         shardsConfig,
                                         writeMemoryConfig,
                                         writeThreadsConfig,
                                         queryThreadsConfig)
    }

    val dsConnectConfig: Map[String, String] = Map(
      IngestRasterParams.ACCUMULO_INSTANCE -> instanceIdParam.lookUp(params).asInstanceOf[String],
      IngestRasterParams.ZOOKEEPERS -> zookeepersParam.lookUp(params).asInstanceOf[String],
      IngestRasterParams.ACCUMULO_USER -> userParam.lookUp(params).asInstanceOf[String],
      IngestRasterParams.ACCUMULO_PASSWORD -> passwordParam.lookUp(params).asInstanceOf[String],
      IngestRasterParams.TABLE -> tableName,
      IngestRasterParams.AUTHORIZATIONS -> authorizationsProvider.getAuthorizations.toString
    )

    val geoserverClientServiceO: Option[GeoserverClientService] = geoserverParam.lookupOpt(params) match {
      case geoserverConfig if geoserverConfig.isInstanceOf[String] =>
        val gsConnectConfig: Map[String, String] =
          geoserverConfig.asInstanceOf[String].split(",").map(_.split("=") match {
            case Array(s1, s2) => (s1, s2)
            case _ => logger.error("Failed in registering raster to Geoserver: wrong parameters.")
              sys.exit()
          }).toMap
        Some(new GeoserverClientService(dsConnectConfig ++ gsConnectConfig))
      case _ => None
    }

    new AccumuloCoverageStore(new RasterStore(rasterOps), geoserverClientServiceO)
  }
}

