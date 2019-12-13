/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.utils.geometry

sealed trait GeometryPrecision

object GeometryPrecision {

  case object FullPrecision extends GeometryPrecision

  /**
    * Precision for serialized geometries
    *
    * Precision is the number of base-10 decimal places stored. A positive precision implies retaining
    * information to the right of the decimal place, while a negative precision implies rounding up to
    * the left of the decimal place
    *
    * See https://github.com/TWKB/Specification/blob/master/twkb.md#type--precision
    *
    * If precision exceeds the max precision for TWKB serialization, full double serialization will
    * be used instead
    *
    * @param xy 4-bit signed integer precision for the x and y dimensions
    *           for lat/lon, a precision of 6 is about 10cm
    * @param z 3-bit unsigned integer precision for the z dimension (if used)
    *          for meters, precision of 1 is 10cm
    * @param m 3-bit unsigned integer precision for the z dimension (if used)
    *          for milliseconds, precision of 0 is 1ms
    */
  case class TwkbPrecision(xy: Byte = 6, z: Byte = 1, m: Byte = 0) extends GeometryPrecision {
    require(xy < 8 && xy > -8 && z < 8 && z > -1 && m < 8 && m > -1, s"Invalid TWKB precision: $xy,$z,$m")
  }
}
