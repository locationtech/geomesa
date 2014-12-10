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

  def ensureTableExists() = rasterOps.ensureTableExists

  def getAuths() = rasterOps.getAuths

  def getVisibility() = rasterOps.getVisibility

  // TODO: WCS: This needs to fleshed out to perform any general query planning, etc.
  // anything general should be performed here,
  // while anything AccumuloSpecific should be done in AccumuloBackedRasterOperations
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
    // NB: JNH: Skipping the shards/writeMemory/writeThreads/queryThreadsParams
    new RasterStore(rasterOps)
  }
}