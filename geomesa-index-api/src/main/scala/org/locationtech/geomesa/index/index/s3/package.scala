/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index.index

import java.time.ZonedDateTime

import org.locationtech.geomesa.curve.S2SFC
import org.locationtech.geomesa.filter.{Bounds, FilterValues}
import org.locationtech.jts.geom.Geometry

package object s3 {

  case class S3IndexKey(bin: Short, s: Long, offset: Int) extends Ordered[S3IndexKey] {
    override def compare(that: S3IndexKey): Int = {
      val b = Ordering.Short.compare(bin, that.bin)
      if (b != 0) { b } else {
        Ordering.Long.compare(s, that.s)
      }
    }
  }

  case class S3IndexValues(
      sfc: S2SFC,
      maxTime: Int,
      geometries: FilterValues[Geometry],
      spatialBounds: Seq[(Double, Double, Double, Double)],
      intervals: FilterValues[Bounds[ZonedDateTime]],
      temporalBounds: Map[Short, Seq[(Int, Int)]],
      temporalUnbounded: Seq[(Short, Short)])
}
