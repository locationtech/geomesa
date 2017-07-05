/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.curve

import org.locationtech.geomesa.curve.NormalizedDimension.{SemiNormalizedLat, SemiNormalizedLon}

@deprecated("Z2SFC", "1.3.2")
object LegacyZ2SFC extends Z2SFC(31) {
  override val lon = SemiNormalizedLon(math.pow(2, 31).toLong - 1)
  override val lat = SemiNormalizedLat(math.pow(2, 31).toLong - 1)
}
