/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import org.locationtech.geomesa.curve.S2SFC
import org.locationtech.geomesa.filter.FilterValues
import org.locationtech.jts.geom.Geometry

package object s2 {

  case class S2IndexValues(
                            sfc: S2SFC,
                            geometries: FilterValues[Geometry],
                            @deprecated("Use spatialBounds instead.")
                            bounds: Seq[(Double, Double, Double, Double)]) extends SpatialIndexValues {
    override def spatialBounds: Seq[(Double, Double, Double, Double)] = bounds
  }
}
