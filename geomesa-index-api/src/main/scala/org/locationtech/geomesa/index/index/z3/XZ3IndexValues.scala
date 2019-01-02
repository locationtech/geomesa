/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z3

import java.time.ZonedDateTime

import org.locationtech.jts.geom.Geometry
import org.locationtech.geomesa.curve.XZ3SFC
import org.locationtech.geomesa.filter.{Bounds, FilterValues}

case class XZ3IndexValues(sfc: XZ3SFC,
                          geometries: FilterValues[Geometry],
                          spatialBounds: Seq[(Double, Double, Double, Double)],
                          intervals: FilterValues[Bounds[ZonedDateTime]],
                          temporalBounds: Map[Short, (Double, Double)])
