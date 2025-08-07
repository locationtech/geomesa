/***********************************************************************
 * Copyright (c) 2013-2025 General Atomics Integrated Intelligence, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import org.locationtech.geomesa.curve.S2SFC
import org.locationtech.geomesa.filter.FilterValues
import org.locationtech.jts.geom.Geometry

package object s2 {

  case class S2IndexValues(
      sfc: S2SFC,
      geometries: FilterValues[Geometry],
      spatialBounds: Seq[(Double, Double, Double, Double)]
    ) extends SpatialIndexValues
}
