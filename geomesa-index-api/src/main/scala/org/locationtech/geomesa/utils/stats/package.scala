/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils

import org.geotools.geometry.jts.JTSFactoryFinder
import org.locationtech.jts.geom.{Geometry, GeometryFactory, Point}

package object stats {

  val geoFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

  def safeCentroid(geom: Geometry): Point = {
    val centroid = geom.getCentroid
    if (java.lang.Double.isNaN(centroid.getCoordinate.x) || java.lang.Double.isNaN(centroid.getCoordinate.y)) {
      geom.getEnvelope.getCentroid
    } else {
      centroid
    }
  }
}
