package org.locationtech.geomesa.utils

import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS

import scala.util.Try

package object geotools {
  // use the epsg jar if it's available (e.g. in geoserver), otherwise use the less-rich constant
  val CRS_EPSG_4326          = Try(CRS.decode("EPSG:4326")).getOrElse(DefaultGeographicCRS.WGS84)
}
