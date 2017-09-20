/***********************************************************************
 * Copyright (c) 2013-2017 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index.z2

import com.vividsolutions.jts.geom.Geometry
import org.locationtech.geomesa.curve.Z2SFC
import org.locationtech.geomesa.filter.FilterValues

case class Z2ProcessingValues(sfc: Z2SFC,
                              geometries: FilterValues[Geometry],
                              bounds: Seq[(Double, Double, Double, Double)])
