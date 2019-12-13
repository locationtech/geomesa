/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.geomesa.curve.NormalizedDimension.{SemiNormalizedLat, SemiNormalizedLon}
import org.locationtech.sfcurve.zorder.Z2

@deprecated("Z2SFC", "1.3.2")
object LegacyZ2SFC extends Z2SFC(31) {
  override val lon = SemiNormalizedLon(math.pow(2, 31).toLong - 1)
  override val lat = SemiNormalizedLat(math.pow(2, 31).toLong - 1)

  // old impl required for deleting existing values that may have been written
  override protected def lenientIndex(x: Double, y: Double): Z2 = {
    val nx = math.max(lon.min, math.ceil((x - lon.min) / (lon.max - lon.min) * lon.precision)).toInt
    val ny = math.max(lat.min, math.ceil((y - lat.min) / (lat.max - lat.min) * lat.precision)).toInt
    Z2(nx, ny)
  }
}
