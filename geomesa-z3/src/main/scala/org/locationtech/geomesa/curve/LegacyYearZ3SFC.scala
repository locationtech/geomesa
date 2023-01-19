/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.geomesa.curve.NormalizedDimension.NormalizedTime

import java.time.temporal.ChronoUnit

/**
 * Z3SFC with a legacy, incorrect max time value of 52 weeks. The max value is kept the same to ensure that
 * index keys and query ranges are consistent. Any dates that exceed the original max time will be dropped into
 * the last time bin, potentially degrading results for the last day or two of the year.
 *
 * @param precision bits used per dimension - note all precisions must sum to less than 64
 */
@deprecated("Z3SFC", "3.2.0")
class LegacyYearZ3SFC(precision: Int = 21) extends {
  // need to use early instantiation here to prevent errors in creating parent class
  // legacy incorrect time max duration
  override val time: NormalizedDimension =
    NormalizedTime(precision, ChronoUnit.WEEKS.getDuration.toMinutes * 52d)
  } with Z3SFC(TimePeriod.Year, precision) {

  // the correct max time duration
  private val maxTime = BinnedTime.maxOffset(TimePeriod.Year)

  override def index(x: Double, y: Double, t: Long, lenient: Boolean = false): Long = {
    if (t > time.max && t <= maxTime) {
      super.index(x, y, time.max.toLong, lenient)
    } else {
      super.index(x, y, t, lenient)
    }
  }
}
