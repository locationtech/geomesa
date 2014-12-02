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

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.factory.Hints
import org.locationtech.geomesa.raster.feature.Raster
import org.locationtech.geomesa.raster.ingest.GeoserverClientService

trait CoverageStore {
  def getAuths(): String
  def getVisibility(): String
  def saveRaster(raster: Raster): Unit
  def registerToGeoserver(raster: Raster): Unit
}

/**
 *
 *  This class handles operations on a coverage, including cutting coverage into chunks
 *  and resembling chunks to a coverage, saving/retrieving coverage to/from data source,
 *  and registering coverage to Geoserver.
 *
 * @param rasterStore Raster store instance
 * @param geoserverClientServiceO Optional Geoserver client instance
 */
class AccumuloCoverageStore(val rasterStore: RasterStore,
                            val geoserverClientServiceO: Option[GeoserverClientService] = None)
  extends CoverageStore with Logging {

  Hints.putSystemDefault(Hints.FORCE_LONGITUDE_FIRST_AXIS_ORDER, true)

  rasterStore.ensureTableExists()

  def getAuths() = rasterStore.getAuths

  def getVisibility() = rasterStore.getVisibility

  def saveRaster(raster: Raster) = {
    rasterStore.putRaster(raster)
    registerToGeoserver(raster)
  }

  def registerToGeoserver(raster: Raster) {
    geoserverClientServiceO.foreach { geoserverClientService => {
      registerToGeoserver(raster, geoserverClientService)
      logger.debug(s"Register raster ${raster.getID} to geoserver at ${geoserverClientService.geoserverUrl}")
    }}
  }

  private def registerToGeoserver(raster: Raster, geoserverClientService: GeoserverClientService) {
    geoserverClientService.registerRasterStyles()
    geoserverClientService.registerRaster(raster.getID,
                                          raster.getID,
                                          "Raster data",
                                          raster.getMbgh.hash,
                                          raster.getMbgh.prec,
                                          None)
  }
}

object AccumuloCoverageStore {
}

