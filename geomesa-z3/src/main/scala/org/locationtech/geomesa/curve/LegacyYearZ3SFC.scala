/***********************************************************************
 * Copyright (c) 2013-2024 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.geomesa.curve.LegacyYearZ3SFC.LegacyYearZ3Dimensions
import org.locationtech.geomesa.curve.NormalizedDimension.NormalizedTime
import org.locationtech.geomesa.curve.Z3SFC.{Z3Dimensions, StandardZ3Dimensions}

import java.time.temporal.ChronoUnit

/**
 * Z3SFC with a legacy, incorrect max time value of 52 weeks. The max value is kept the same to ensure that
 * index keys and query ranges are consistent. Any dates that exceed the original max time will be dropped into
 * the last time bin, potentially degrading results for the last day or two of the year.
 *
 * @param dims curve dimensions
 */
@deprecated("Z3SFC", "3.2.0")
class LegacyYearZ3SFC(dims: LegacyYearZ3Dimensions) extends Z3SFC(dims) {

  // the correct max time duration
  private val maxTime = BinnedTime.maxOffset(TimePeriod.Year)

  /**
   * Alternate constructor
   *
   * @param precision bits used per dimension - note all precisions must sum to less than 64
   */
  def this(precision: Int = 21) = this(LegacyYearZ3Dimensions(precision))

  override def index(x: Double, y: Double, t: Long, lenient: Boolean = false): Long = {
    if (t > time.max && t <= maxTime) {
      super.index(x, y, time.max.toLong, lenient)
    } else {
      super.index(x, y, t, lenient)
    }
  }
}

@deprecated("Z3SFC", "3.2.0")
object LegacyYearZ3SFC {

  case class LegacyYearZ3Dimensions(precision: Int = 21) extends Z3Dimensions {
    private val delegate = StandardZ3Dimensions(TimePeriod.Year, precision)

    override val lat: NormalizedDimension = delegate.lat
    override val lon: NormalizedDimension = delegate.lon
    // legacy incorrect time max duration
    override val time: NormalizedDimension = NormalizedTime(precision, ChronoUnit.WEEKS.getDuration.toMinutes * 52d)
  }
}
