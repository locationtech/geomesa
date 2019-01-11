/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.wcs

import com.typesafe.scalalogging.LazyLogging
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader
import org.geotools.coverage.grid.{GridCoverage2D, GridEnvelope2D}
import org.geotools.factory.Hints
import org.geotools.geometry.GeneralEnvelope
import org.geotools.util.Utilities
import org.locationtech.geomesa.raster.data.{AccumuloRasterStore, GeoMesaCoverageQueryParams}
import org.locationtech.geomesa.raster.util.RasterUtils
import org.opengis.parameter.GeneralParameterValue

class GeoMesaCoverageReader(val url: String, hints: Hints) extends AbstractGridCoverage2DReader() with LazyLogging {

  logger.debug(s"Creating coverage reader for url '${url.replaceAll(":.*@", ":*@")}'")
  val parsed = AccumuloUrl(url)
  logger.debug(s"extracted $parsed")

  val ars = AccumuloRasterStore(parsed.user, parsed.password, parsed.instanceId, parsed.zookeepers,
    parsed.table, parsed.auths.getOrElse(""), parsed.visibilities.getOrElse(""), parsed.collectStats)

  // update super class state
  coverageName = parsed.table
  crs = org.locationtech.geomesa.utils.geotools.CRS_EPSG_4326
  originalEnvelope = getBounds
  originalEnvelope.setCoordinateReferenceSystem(crs)
  originalGridRange = getGridRange
  coverageFactory = CoverageFactoryFinder.getGridCoverageFactory(hints)

  /**
   * Default implementation does not allow a non-default coverage name
   */
  override protected def checkName(coverageName: String) = {
    Utilities.ensureNonNull("coverageName", coverageName)
    true
  }

  override def getOriginalEnvelope = this.originalEnvelope

  override def getCoordinateReferenceSystem = this.crs

  override def getCoordinateReferenceSystem(coverageName: String) = this.getCoordinateReferenceSystem

  override def getFormat = new GeoMesaCoverageFormat

  override def getOriginalGridRange = this.originalGridRange

  override def getOriginalGridRange(coverageName: String) = this.originalGridRange

  def read(parameters: Array[GeneralParameterValue]): GridCoverage2D = {
    logger.debug(s"READ: $parameters")
    val params = new GeoMesaCoverageQueryParams(parameters)
    val rq = params.toRasterQuery
    if (params.width.toInt == 5 && params.height.toInt == 5){
      //TODO: https://geomesa.atlassian.net/browse/GEOMESA-868, https://geomesa.atlassian.net/browse/GEOMESA-869
      logger.warn("Suspected GeoServer registration layer, returning a default image for now until mosaicing fixed")
      coverageFactory.create(coverageName, RasterUtils.defaultBufferedImage, params.envelope)
    } else {
      logger.debug(s"read: width: ${params.width.toInt} height: ${params.height.toInt}" +
          s" resX: ${params.resX} resY: ${params.resY} env: ${params.envelope}")
      val mosaic = ars.getMosaicedRaster(rq, params)
      if (mosaic == null) {
        null
      } else {
        val coverage = coverageFactory.create(coverageName, mosaic, params.envelope)
        mosaic.flush()
        coverage
      }
    }
  }

  def getBounds: GeneralEnvelope = {
    val bbox = ars.getBounds
    new GeneralEnvelope(Array(bbox.minLon, bbox.minLat), Array(bbox.maxLon, bbox.maxLat))
  }

  def getGridRange: GridEnvelope2D = ars.getGridRange

  override def dispose(): Unit = {
    ars.close()
    super.dispose()
  }
}