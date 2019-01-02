/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.geomesa.curve.NormalizedDimension.{NormalizedLat, NormalizedLon}
import org.locationtech.sfcurve.IndexRange
import org.locationtech.sfcurve.zorder.{Z2, ZRange}

object Z2SFC extends Z2SFC(31)

/**
  * z2 space-filling curve
  *
  * @param precision number of bits used per dimension - note sum must be less than 64
  */
class Z2SFC(precision: Int) extends SpaceFillingCurve[Z2] {

  override val lon: NormalizedDimension = NormalizedLon(precision)
  override val lat: NormalizedDimension = NormalizedLat(precision)

  override def index(x: Double, y: Double, lenient: Boolean = false): Z2 = {
    try {
      require(x >= lon.min && x <= lon.max && y >= lat.min && y <= lat.max,
        s"Value(s) out of bounds ([${lon.min},${lon.max}], [${lat.min},${lat.max}]): $x, $y")
      Z2(lon.normalize(x), lat.normalize(y))
    } catch {
      case _: IllegalArgumentException if lenient => lenientIndex(x, y)
    }
  }

  protected def lenientIndex(x: Double, y: Double): Z2 = {
    val bx = if (x < lon.min) { lon.min } else if (x > lon.max) { lon.max } else { x }
    val by = if (y < lat.min) { lat.min } else if (y > lat.max) { lat.max } else { y }
    Z2(lon.normalize(bx), lat.normalize(by))
  }

  override def invert(z: Z2): (Double, Double) = {
    val (x, y) = z.decode
    (lon.denormalize(x), lat.denormalize(y))
  }

  override def ranges(xy: Seq[(Double, Double, Double, Double)],
                      precision: Int,
                      maxRanges: Option[Int]): Seq[IndexRange] = {
    val zbounds = xy.map { case (xmin, ymin, xmax, ymax) => ZRange(index(xmin, ymin).z, index(xmax, ymax).z) }
    Z2.zranges(zbounds.toArray, precision, maxRanges)
  }
}
