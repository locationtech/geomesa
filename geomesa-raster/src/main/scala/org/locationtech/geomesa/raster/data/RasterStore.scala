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

import org.geotools.coverage.grid.GridEnvelope2D
import org.locationtech.geomesa.raster.AccumuloStoreHelper
import org.locationtech.geomesa.utils.geohash.BoundingBox

/**
 * This class defines basic operations on Raster, including saving/retrieving
 * Raster objects to/from data source.
 *
 * @param rasterOps
 */
class RasterStore(val rasterOps: RasterOperations) {

  def ensureBoundsTableExists() = rasterOps.ensureBoundsTableExists()

  def createTableStructure() = rasterOps.createTableStructure()

  def getAuths = rasterOps.getAuths()

  def getVisibility = rasterOps.getVisibility()

  def getConnector = rasterOps.getConnector()

  def getTable = rasterOps.getTable

  def getRasters(rasterQuery: RasterQuery): Iterator[Raster] = rasterOps.getRasters(rasterQuery)

  def putRaster(raster: Raster) = rasterOps.putRaster(raster)

  def getBounds(): BoundingBox = rasterOps.getBounds()

  def getAvailableResolutions(): Seq[Double] = rasterOps.getAvailableResolutions()

  def getGridRange(): GridEnvelope2D = rasterOps.getGridRange()
}

object RasterStore {
  def apply(username: String,
            password: String,
            instanceId: String,
            zookeepers: String,
            tableName: String,
            auths: String,
            writeVisibilities: String,
            useMock: Boolean = false): RasterStore = {

    val conn = AccumuloStoreHelper.buildAccumuloConnector(username, password, instanceId, zookeepers, useMock)

    val authorizationsProvider = AccumuloStoreHelper.getAuthorizationsProvider(auths.split(","), conn)

    val rasterOps = new AccumuloBackedRasterOperations(conn, tableName, authorizationsProvider, writeVisibilities)
    // this will actually create the Accumulo Table
    rasterOps.createTableStructure()
    // TODO: WCS: Configure the shards/writeMemory/writeThreads/queryThreadsParams
    // GEOMESA-568
    new RasterStore(rasterOps)
  }
}
