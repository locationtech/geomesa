/***********************************************************************
* Copyright (c) 2013-2015 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0 which
* accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.plugin.wcs

import com.typesafe.scalalogging.slf4j.Logging
import org.geotools.coverage.CoverageFactoryFinder
import org.geotools.coverage.grid.io.AbstractGridCoverage2DReader
import org.geotools.coverage.grid.{GridCoverage2D, GridEnvelope2D}
import org.geotools.factory.Hints
import org.geotools.geometry.GeneralEnvelope
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.geotools.util.Utilities
import org.joda.time.format.ISODateTimeFormat
import org.joda.time.{DateTime, DateTimeZone}
import org.locationtech.geomesa.raster.data.{AccumuloRasterStore, GeoMesaCoverageQueryParams}
import org.locationtech.geomesa.raster.util.RasterUtils
import org.opengis.parameter.GeneralParameterValue

import scala.util.Try

object GeoMesaCoverageReader {
  val GeoServerDateFormat = ISODateTimeFormat.dateTime()
  val DefaultDateString = GeoServerDateFormat.print(new DateTime(DateTimeZone.forID("UTC")))
  val FORMAT = """accumulo://(.*):(.*)@(.*)/(.*)#zookeepers=([^#]*)(?:#auths=)?([^#]*)(?:#visibilities=)?([^#]*)(?:#collectStats=)?([^#]*)$""".r
}

import org.locationtech.geomesa.plugin.wcs.GeoMesaCoverageReader._

class GeoMesaCoverageReader(val url: String, hints: Hints) extends AbstractGridCoverage2DReader() with Logging {
  logger.debug(s"""creating coverage reader for url "${url.replaceAll(":.*@", ":********@").replaceAll("#auths=.*","#auths=********")}"""")
  val FORMAT(user, password, instanceId, table, zookeepers, auths, visibilities, collectStats) = url
  logger.debug(s"extracted user $user, password ********, instance id $instanceId, table $table, zookeepers $zookeepers, auths ********")
  logger.info(s"collectstats: $collectStats")
  coverageName = table

  val dostats = if (collectStats == "null") false else true
  val ars = AccumuloRasterStore(user, password, instanceId, zookeepers, table, auths, visibilities, collectStats = dostats)

  crs = Try(CRS.decode("EPSG:4326")).getOrElse(DefaultGeographicCRS.WGS84)
  originalEnvelope = getBounds
  originalEnvelope.setCoordinateReferenceSystem(crs)
  originalGridRange = getGridRange
  coverageFactory = CoverageFactoryFinder.getGridCoverageFactory(hints)

  /**
   * Default implementation does not allow a non-default coverage name
   * @param coverageName
   * @return
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
      logger.warn("In GeoMesaCoverageReader: suspected GeoServer Registration Layer, returning a default image for now until mosaicing fixed")
      coverageFactory.create(coverageName, RasterUtils.defaultBufferedImage, params.envelope)
    } else {
      logger.info(s"In rastersToCoverage: width: ${params.width.toInt} height: ${params.height.toInt} resX: ${params.resX} resY: ${params.resY} env: ${params.envelope}")
      val mosaic = ars.getMosaicedRaster(rq, params)
      if (mosaic == null) null
      else {
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
}