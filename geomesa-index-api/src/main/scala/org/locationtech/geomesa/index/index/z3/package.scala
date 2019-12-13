/***********************************************************************
 * Copyright (c) 2013-2019 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import java.time.ZonedDateTime

import org.locationtech.geomesa.curve.{XZ3SFC, Z3SFC}
import org.locationtech.geomesa.filter.{Bounds, FilterValues}
import org.locationtech.jts.geom.Geometry

package object z3 {

  case class Z3IndexKey(bin: Short, z: Long) extends Ordered[Z3IndexKey] {
    override def compare(that: Z3IndexKey): Int = {
      val b = Ordering.Short.compare(bin, that.bin)
      if (b != 0) { b } else {
        Ordering.Long.compare(z, that.z)
      }
    }
  }

  case class Z3IndexValues(sfc: Z3SFC,
                           geometries: FilterValues[Geometry],
                           spatialBounds: Seq[(Double, Double, Double, Double)],
                           intervals: FilterValues[Bounds[ZonedDateTime]],
                           temporalBounds: Map[Short, Seq[(Long, Long)]],
                           temporalUnbounded: Seq[(Short, Short)])

  case class XZ3IndexValues(sfc: XZ3SFC,
                            geometries: FilterValues[Geometry],
                            spatialBounds: Seq[(Double, Double, Double, Double)],
                            intervals: FilterValues[Bounds[ZonedDateTime]],
                            temporalBounds: Map[Short, (Double, Double)],
                            temporalUnbounded: Seq[(Short, Short)])
}
