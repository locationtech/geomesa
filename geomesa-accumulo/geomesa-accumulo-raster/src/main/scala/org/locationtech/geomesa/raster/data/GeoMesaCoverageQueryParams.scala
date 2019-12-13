/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.raster.data

import org.geotools.coverage.grid.GridGeometry2D
import org.geotools.coverage.grid.io.AbstractGridFormat
import org.geotools.parameter.Parameter
import org.locationtech.geomesa.raster.util.RasterUtils
import org.locationtech.geomesa.utils.geohash.{BoundingBox, Bounds}
import org.opengis.parameter.GeneralParameterValue

/**
 * Takes the Array[GeneralParameterValue] from the read() function of GeoMesaCoverageReader and pulls
 * out the gridGeometry, envelope, height and width, resolution, and bounding box from the query
 * parameters. These are then used to query Accumulo and retrieve out the correct raster information.
 * @param parameters the Array of GeneralParameterValues from the GeoMesaCoverageReader read() function.
 */
class GeoMesaCoverageQueryParams(parameters: Array[GeneralParameterValue]) {
  val paramsMap = parameters.map { gpv => (gpv.getDescriptor.getName.getCode, gpv) }.toMap
  val gridGeometry = paramsMap(AbstractGridFormat.READ_GRIDGEOMETRY2D.getName.toString)
                     .asInstanceOf[Parameter[GridGeometry2D]].getValue
  val envelope = gridGeometry.getEnvelope
  val dim = gridGeometry.getGridRange2D.getBounds
  val rasterParams = RasterUtils.sharedRasterParams(gridGeometry, envelope)
  val width = rasterParams.width
  val height = rasterParams.height
  val resX = rasterParams.resX
  val resY = rasterParams.resY
  val suggestedQueryResolution = rasterParams.suggestedQueryResolution
  val min = Array(correctedMinLongitude, correctedMinLatitude)
  val max = Array(correctedMaxLongitude, correctedMaxLatitude)
  val bbox = BoundingBox(Bounds(min(0), max(0)), Bounds(min(1), max(1)))

  def toRasterQuery: RasterQuery = RasterQuery(bbox, suggestedQueryResolution)

  def correctedMaxLongitude: Double = Math.max(Math.min(envelope.getMaximum(0), 180), -180.0)

  def correctedMinLongitude: Double = Math.min(Math.max(envelope.getMinimum(0), -180), 180.0)

  def correctedMaxLatitude: Double = Math.max(Math.min(envelope.getMaximum(1), 90), -90.0)

  def correctedMinLatitude: Double = Math.min(Math.max(envelope.getMinimum(1), -90), 90.0)

}