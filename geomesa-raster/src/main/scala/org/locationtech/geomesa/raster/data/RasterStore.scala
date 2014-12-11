package org.locationtech.geomesa.raster.data

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
