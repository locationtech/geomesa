package org.locationtech.geomesa.utils

import org.geotools.data.FeatureReader
import org.geotools.data.collection.DelegateFeatureReader
import org.geotools.feature.collection.DelegateFeatureIterator
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

import scala.util.Try

package object geotools {
  // use the epsg jar if it's available (e.g. in geoserver), otherwise use the less-rich constant
  val CRS_EPSG_4326          = Try(CRS.decode("EPSG:4326")).getOrElse(DefaultGeographicCRS.WGS84)

  type FR = FeatureReader[SimpleFeatureType, SimpleFeature]
  type DFR = DelegateFeatureReader[SimpleFeatureType, SimpleFeature]
  type DFI = DelegateFeatureIterator[SimpleFeature]
}
