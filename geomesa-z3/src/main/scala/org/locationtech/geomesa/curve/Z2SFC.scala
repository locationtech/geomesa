/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.sfcurve.IndexRange
import org.locationtech.sfcurve.zorder.Z2

object Z2SFC extends SpaceFillingCurve[Z2] {

  private val xprec: Long = math.pow(2, 31).toLong - 1
  private val yprec: Long = math.pow(2, 31).toLong - 1

  override val lon  = NormalizedLon(xprec)
  override val lat  = NormalizedLat(yprec)

  override def index(x: Double, y: Double): Z2 = Z2(lon.normalize(x), lat.normalize(y))

  override def ranges(x: (Double, Double), y: (Double, Double), precision: Int): Seq[IndexRange] =
    Z2.zranges(index(x._1, y._1), index(x._2, y._2), precision)

  override def invert(z: Z2): (Double, Double) = {
    val (x, y) = z.decode
    (lon.denormalize(x), lat.denormalize(y))
  }
}
