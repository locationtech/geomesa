/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import org.locationtech.jts.geom.Geometry
import org.locationtech.geomesa.curve.{XZ2SFC, Z2SFC}
import org.locationtech.geomesa.filter.FilterValues

package object z2 {

  case class Z2IndexValues(sfc: Z2SFC,
                           geometries: FilterValues[Geometry],
                           bounds: Seq[(Double, Double, Double, Double)])

  case class XZ2IndexValues(sfc: XZ2SFC,
                            geometries: FilterValues[Geometry],
                            bounds: Seq[(Double, Double, Double, Double)])
}
