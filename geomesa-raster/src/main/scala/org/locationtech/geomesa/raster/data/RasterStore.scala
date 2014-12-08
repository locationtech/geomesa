package org.locationtech.geomesa.raster.data

import java.awt.image.RenderedImage

import org.locationtech.geomesa.raster.AccumuloStoreHelper
import org.locationtech.geomesa.raster.feature.Raster

/**
 * This class defines basic operations on Raster, including saving/retrieving
 * Raster objects to/from data source.
 *
 * @param rasterOps
 */
class RasterStore(val rasterOps: RasterOperations) {

  def ensureTableExists() = rasterOps.ensureTableExists()

  def getAuths = rasterOps.getAuths()

  def getVisibility = rasterOps.getVisibility()

  def getConnector = rasterOps.getConnector()

  def getTable = rasterOps.getTable

  def getRasters(rasterQuery: RasterQuery): Iterator[Raster] = rasterOps.getRasters(rasterQuery)

  def putRaster(raster: Raster) = rasterOps.putRaster(raster)
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
    rasterOps.ensureTableExists()
    // TODO: WCS: Configure the shards/writeMemory/writeThreads/queryThreadsParams
    // GEOMESA-568
    new RasterStore(rasterOps)
  }
}
