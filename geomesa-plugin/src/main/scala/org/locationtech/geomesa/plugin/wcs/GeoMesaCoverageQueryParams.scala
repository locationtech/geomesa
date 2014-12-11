package org.locationtech.geomesa.plugin.wcs

import org.geotools.coverage.grid.GridGeometry2D
import org.geotools.coverage.grid.io.{AbstractGridFormat, OverviewPolicy}
import org.geotools.parameter.Parameter
import org.locationtech.geomesa.utils.geohash.{BoundingBox, Bounds}
import org.opengis.parameter.GeneralParameterValue

/**
 * Takes the Array[GeneralParameterValue] from the read() function of GeoMesaCoverageReader and pulls
 * out the gridGeometry, envelope, height and width, resolution, and bounding box from the query
 * parameters. These are then used to query Accumulo and retrieve out the correct raster information.
 * @param parameters the Array of GeneralParameterValues from the GeoMesaCoverageReader read() function.
 */
class GeoMesaCoverageQueryParams(parameters: Array[GeneralParameterValue]) {
  val paramsMap = parameters.map(gpv => (gpv.getDescriptor.getName.getCode, gpv)).toMap
  val gridGeometry: GridGeometry2D = paramsMap(AbstractGridFormat.READ_GRIDGEOMETRY2D.getName.toString).asInstanceOf[Parameter[GridGeometry2D]].getValue
  val overviewPolicy = OverviewPolicy.IGNORE
  val envelope = gridGeometry.getEnvelope
  val dim = gridGeometry.getGridRange2D.getBounds
  val width = gridGeometry.getGridRange2D.getWidth
  val height = gridGeometry.getGridRange2D.getHeight
  val resx = (envelope.getMaximum(0) - envelope.getMinimum(0)) / width
  val resy = (envelope.getMaximum(1) - envelope.getMinimum(1)) / height
  val resolution: Double = if (resx == resy) { resx } else { paramsMap(GeoMesaCoverageFormat.RESOLUTION.getName.toString).asInstanceOf[Parameter[String]].getValue.toDouble }
  val min = Array(Math.max(envelope.getMinimum(0), -180) + .00000001, Math.max(envelope.getMinimum(1), -90) + .00000001)
  val max = Array(Math.min(envelope.getMaximum(0), 180) - .00000001, Math.min(envelope.getMaximum(1), 90) - .00000001)
  val bbox = BoundingBox(Bounds(min(0), max(0)), Bounds(min(1), max(1)))
}