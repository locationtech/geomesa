/***********************************************************************
 * Copyright (c) 2013-2023 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import java.time.temporal.ChronoUnit

/**
 * XZ3SFC with a legacy, incorrect max time value of 52 weeks. The max value is kept the same to ensure that
 * index keys and query ranges are consistent. Any dates that exceed the original max time will be dropped into
 * the last time bin, potentially degrading results for the last day or two of the year.
 *
 * @param g resolution level of the curve - i.e. how many times the space will be recursively split into eighths
 */
@deprecated("XZ3SFC", "3.2.0")
class LegacyYearXZ3SFC(g: Short)
    extends XZ3SFC(g, (-180.0, 180.0), (-90.0, 90.0), (0.0, ChronoUnit.WEEKS.getDuration.toMinutes * 52d)) {

  // the correct max time duration
  private val maxTime = BinnedTime.maxOffset(TimePeriod.Year).toDouble
  // the incorrect max time duration
  private val zHi = zBounds._2

  override protected def normalize(
      xmin: Double,
      ymin: Double,
      zmin: Double,
      xmax: Double,
      ymax: Double,
      zmax: Double,
      lenient: Boolean): (Double, Double, Double, Double, Double, Double) = {
    if (zmax > zHi && zmax <= maxTime) {
      super.normalize(xmin, ymin, math.min(zmin, zHi), xmax, ymax, zHi, lenient)
    } else {
      super.normalize(xmin, ymin, zmin, xmax, ymax, zmax, lenient)
    }
  }
}
