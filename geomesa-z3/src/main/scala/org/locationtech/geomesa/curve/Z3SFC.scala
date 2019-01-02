/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.geomesa.curve.NormalizedDimension._
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.sfcurve.IndexRange
import org.locationtech.sfcurve.zorder.{Z3, ZRange}

/**
  * Z3 space filling curve
  *
  * @param period time period used to bin results
  * @param precision bits used per dimension - note all precisions must sum to less than 64
  */
class Z3SFC(period: TimePeriod, precision: Int = 21) extends SpaceTimeFillingCurve[Z3] {

  require(precision > 0 && precision < 22, "Precision (bits) per dimension must be in [1,21]")

  override val lon: NormalizedDimension  = NormalizedLon(precision)
  override val lat: NormalizedDimension  = NormalizedLat(precision)
  override val time: NormalizedDimension = NormalizedTime(precision, BinnedTime.maxOffset(period).toDouble)

  val wholePeriod = Seq((time.min.toLong, time.max.toLong))

  override def index(x: Double, y: Double, t: Long, lenient: Boolean = false): Z3 = {
    try {
      require(x >= lon.min && x <= lon.max && y >= lat.min && y <= lat.max && t >= time.min && t <= time.max,
        s"Value(s) out of bounds ([${lon.min},${lon.max}], [${lat.min},${lat.max}], [${time.min},${time.max}]): $x, $y, $t")
      Z3(lon.normalize(x), lat.normalize(y), time.normalize(t))
    } catch {
      case _: IllegalArgumentException if lenient => lenientIndex(x, y, t)
    }
  }

  protected def lenientIndex(x: Double, y: Double, t: Long): Z3 = {
    val bx = if (x < lon.min) { lon.min } else if (x > lon.max) { lon.max } else { x }
    val by = if (y < lat.min) { lat.min } else if (y > lat.max) { lat.max } else { y }
    val bt = if (t < time.min) { time.min } else if (t > time.max) { time.max } else { t }
    Z3(lon.normalize(bx), lat.normalize(by), time.normalize(bt))
  }

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

object Z3SFC {

  private val SfcDay   = new Z3SFC(TimePeriod.Day)
  private val SfcWeek  = new Z3SFC(TimePeriod.Week)
  private val SfcMonth = new Z3SFC(TimePeriod.Month)
  private val SfcYear  = new Z3SFC(TimePeriod.Year)

  def apply(period: TimePeriod): Z3SFC = period match {
    case TimePeriod.Day   => SfcDay
    case TimePeriod.Week  => SfcWeek
    case TimePeriod.Month => SfcMonth
    case TimePeriod.Year  => SfcYear
  }
}