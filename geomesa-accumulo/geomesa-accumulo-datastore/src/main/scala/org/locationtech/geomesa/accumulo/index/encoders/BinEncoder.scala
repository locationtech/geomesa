/***********************************************************************
 * Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.accumulo.index.encoders

import java.util.Date

import com.vividsolutions.jts.geom.{Geometry, Point}
import org.locationtech.geomesa.filter.function.{BasicValues, Convert2ViewerFunction}
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType.RichSimpleFeatureType
import org.opengis.feature.simple.{SimpleFeature, SimpleFeatureType}

/**
 * Encodes features in 'bin' format
 */
class BinEncoder(sft: SimpleFeatureType, trackIdField: String) {

  private val trackIdIndex = sft.indexOf(trackIdField)
  private val geomIndex = sft.getGeomIndex
  private val dtgIndex = sft.getDtgIndex.getOrElse(-1)

  private val getLatLon = if (sft.isPoints) getLatLonPoints _ else getLatLonNonPoints _

  private val getDtg: (SimpleFeature) => Long =
    if (dtgIndex == -1) (sf: SimpleFeature) => 0L else getDtgWithIndex

  /**
   * Encode a feature to bytes
   */
  def encode(sf: SimpleFeature): Array[Byte] = {
    val (lat, lon) = getLatLon(sf)
    val dtg = getDtg(sf)
    val trackIdVal = sf.getAttribute(trackIdIndex)
    val trackId = if (trackIdVal == null) { 0 } else { trackIdVal.hashCode }
    Convert2ViewerFunction.encodeToByteArray(BasicValues(lat, lon, dtg, trackId))
  }

  private def getLatLonPoints(sf: SimpleFeature): (Float, Float) = {
    val geom = sf.getAttribute(geomIndex).asInstanceOf[Point]
    (geom.getY.toFloat, geom.getX.toFloat)
  }

  private def getLatLonNonPoints(sf: SimpleFeature): (Float, Float) = {
    import org.locationtech.geomesa.utils.geotools.Conversions.RichGeometry
    val geom = sf.getAttribute(geomIndex).asInstanceOf[Geometry].safeCentroid()
    (geom.getY.toFloat, geom.getX.toFloat)
  }

  private def getDtgWithIndex(sf: SimpleFeature): Long = {
    val dtg = sf.getAttribute(dtgIndex).asInstanceOf[Date]
    if (dtg == null) 0L else dtg.getTime
  }
}

object BinEncoder {
  def apply(sft: SimpleFeatureType): Option[BinEncoder] = sft.getBinTrackId.map(new BinEncoder(sft, _))
}
