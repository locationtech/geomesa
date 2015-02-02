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

  def toRasterQuery: RasterQuery = RasterQuery(bbox, suggestedQueryResolution, None, None)

  def correctedMaxLongitude(): Double = Math.max(Math.min(envelope.getMaximum(0), 180) - .00000001, -179.99999999)

  def correctedMinLongitude(): Double = Math.min(Math.max(envelope.getMinimum(0), -180) + .00000001, 179.99999999)

  def correctedMaxLatitude(): Double = Math.max(Math.min(envelope.getMaximum(1), 90) - .00000001, -89.99999999)

  def correctedMinLatitude(): Double = Math.min(Math.max(envelope.getMinimum(1), -90) + .00000001, 89.99999999)

}