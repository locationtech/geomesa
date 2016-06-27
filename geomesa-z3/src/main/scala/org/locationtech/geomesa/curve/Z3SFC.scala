/***********************************************************************
* Copyright (c) 2013-2016 Commonwealth Computer Research, Inc.
* All rights reserved. This program and the accompanying materials
* are made available under the terms of the Apache License, Version 2.0
* which accompanies this distribution and is available at
* http://www.opensource.org/licenses/apache2.0.php.
*************************************************************************/

package org.locationtech.geomesa.curve

import org.joda.time.Weeks
import org.locationtech.sfcurve.IndexRange
import org.locationtech.sfcurve.zorder.{Z3, ZRange}

object Z3SFC extends SpaceTimeFillingCurve[Z3] {

  private val xprec: Long = math.pow(2, 21).toLong - 1
  private val yprec: Long = math.pow(2, 21).toLong - 1
  private val tprec: Long = math.pow(2, 20).toLong - 1
  private val tmax: Double = Weeks.weeks(1).toStandardSeconds.getSeconds.toDouble

  override val lon  = NormalizedLon(xprec)
  override val lat  = NormalizedLat(yprec)
  override val time = NormalizedTime(tprec, tmax)

  override def index(x: Double, y: Double, t: Long): Z3 =
    Z3(lon.normalize(x), lat.normalize(y), time.normalize(t))

  override def invert(z: Z3): (Double, Double, Long) = {
    val (x, y, t) = z.decode
    (lon.denormalize(x), lat.denormalize(y), time.denormalize(t).toLong)
  }

  override def ranges(xy: Seq[(Double, Double, Double, Double)],
                      t: Seq[(Long, Long)],
                      precision: Int,
                      maxRanges: Option[Int]): Seq[IndexRange] = {
    val zbounds = for { (xmin, ymin, xmax, ymax) <- xy ; (tmin, tmax) <- t } yield {
      ZRange(index(xmin, ymin, tmin).z, index(xmax, ymax, tmax).z)
    }
    Z3.zranges(zbounds.toArray, precision, maxRanges)
  }
}
