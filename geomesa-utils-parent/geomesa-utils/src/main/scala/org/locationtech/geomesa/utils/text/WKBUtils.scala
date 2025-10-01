/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.utils.text

import org.locationtech.jts.geom.Geometry
import org.locationtech.jts.io.{WKBReader, WKBWriter}

object WKBUtils extends WKBUtils

trait WKBUtils {

  private[this] val readerPool = new ThreadLocal[WKBReader]{
    override def initialValue = new WKBReader
  }

  private[this] val writer2dPool = new ThreadLocal[WKBWriter]{
    override def initialValue = new WKBWriter(2)
  }

  private[this] val writer3dPool = new ThreadLocal[WKBWriter]{
    override def initialValue = new WKBWriter(3)
  }

  def read(s: String): Geometry = read(s.getBytes)
  def read(b: Array[Byte]): Geometry = readerPool.get.read(b)

  def write(g: Geometry): Array[Byte] = {
    val writer = if (is2d(g)) { writer2dPool } else { writer3dPool }
    writer.get.write(g)
  }

  private def is2d(geometry: Geometry): Boolean = {
    // don't trust coord.getDimensions - it always returns 3 in jts
    // instead, check for NaN for the z dimension
    // note that we only check the first coordinate - if a geometry is written with different
    // dimensions in each coordinate, some information may be lost
    if (geometry == null) { true } else {
      val coord = geometry.getCoordinate
      // check for dimensions
      // TODO WKBWriter still only supports z or m but not both
      coord == null || (java.lang.Double.isNaN(coord.getZ) && java.lang.Double.isNaN(coord.getM))
    }
  }
}
