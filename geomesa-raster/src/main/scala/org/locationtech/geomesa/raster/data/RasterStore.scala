package org.locationtech.geomesa.raster.data

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
