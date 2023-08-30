/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils

import com.typesafe.scalalogging.LazyLogging
import org.geotools.data.FeatureReader
import org.geotools.data.collection.DelegateFeatureReader
import org.geotools.feature.collection.DelegateFeatureIterator
import org.geotools.geometry.jts.ReferencedEnvelope
import org.geotools.referencing.CRS
import org.geotools.referencing.crs.DefaultGeographicCRS
import org.locationtech.geomesa.utils.text.WKTUtils
import org.locationtech.jts.geom.{Geometry, Polygon}
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}
import org.opengis.referencing.crs.CoordinateReferenceSystem

import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

package object geotools extends LazyLogging {

  // use the epsg jar if it's available (e.g. in geoserver), otherwise use the less-rich constant
  val CRS_EPSG_4326: CoordinateReferenceSystem = {
    // work-around for classloading bug in indriya: https://github.com/unitsofmeasurement/indriya/issues/383
    // by default, SPI use the context classloader, which can be incorrect if there are multiple classloaders
    // TODO should be able to remove this when geotools upgrades to indriya 2.2+
    val ccl = Thread.currentThread().getContextClassLoader
    try {
      val cl = getClass.getClassLoader
      logger.debug(s"Setting context classloader for CRS loading to: $cl")
      Thread.currentThread().setContextClassLoader(cl)
      CRS.decode("EPSG:4326", true)
    } catch {
      case _: Throwable => DefaultGeographicCRS.WGS84
    } finally {
      Thread.currentThread().setContextClassLoader(ccl)
    }
  }

  val CrsEpsg4326: CoordinateReferenceSystem = CRS_EPSG_4326

  // we make this a function, as envelopes are mutable
  def wholeWorldEnvelope = new ReferencedEnvelope(-180, 180, -90, 90, CRS_EPSG_4326)
  val WholeWorldPolygon: Polygon =
    WKTUtils.read("POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))").asInstanceOf[Polygon]
  val EmptyGeometry: Geometry = WKTUtils.read("POLYGON EMPTY")

  // date format with geotools pattern
  val GeoToolsDateFormat: DateTimeFormatter =
    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC)

  type FR = FeatureReader[SimpleFeatureType, SimpleFeature]
  type DFR = DelegateFeatureReader[SimpleFeatureType, SimpleFeature]
  type DFI = DelegateFeatureIterator[SimpleFeature]
}
