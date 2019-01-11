/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.geomesa.curve.NormalizedDimension.{SemiNormalizedLat, SemiNormalizedLon, SemiNormalizedTime}
import org.locationtech.geomesa.curve.TimePeriod.TimePeriod
import org.locationtech.sfcurve.zorder.Z3

@deprecated("Z3SFC", "1.3.2")
class LegacyZ3SFC(period: TimePeriod) extends {
  // early initialization of lat/lon/time to allow for 'wholePeriod' val creation in Z3SFC
  override val lon  = SemiNormalizedLon(math.pow(2, 21).toLong - 1)
  override val lat  = SemiNormalizedLat(math.pow(2, 21).toLong - 1)
  override val time = SemiNormalizedTime(math.pow(2, 20).toLong - 1, BinnedTime.maxOffset(period).toDouble)
} with Z3SFC(period, 21) {
  // old impl required for deleting existing values that may have been written
  override protected def lenientIndex(x: Double, y: Double, t: Long): Z3 = {
    val nx = math.max(lon.min, math.ceil((x - lon.min) / (lon.max - lon.min) * lon.precision)).toInt
    val ny = math.max(lat.min, math.ceil((y - lat.min) / (lat.max - lat.min) * lat.precision)).toInt
    val nt = math.max(time.min, math.ceil((t - time.min) / (time.max - time.min) * time.precision)).toInt
    Z3(nx, ny, nt)
  }
}

@deprecated("Z3SFC", "1.3.2")
object LegacyZ3SFC {

  private val SfcDay   = new LegacyZ3SFC(TimePeriod.Day)
  private val SfcWeek  = new LegacyZ3SFC(TimePeriod.Week)
  private val SfcMonth = new LegacyZ3SFC(TimePeriod.Month)
  private val SfcYear  = new LegacyZ3SFC(TimePeriod.Year)

  def apply(period: TimePeriod): LegacyZ3SFC = period match {
    case TimePeriod.Day   => SfcDay
    case TimePeriod.Week  => SfcWeek
    case TimePeriod.Month => SfcMonth
    case TimePeriod.Year  => SfcYear
  }
}