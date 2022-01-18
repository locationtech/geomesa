/***********************************************************************
 * Copyright (c) 2013-2021 Commonwealth Computer Research, Inc.
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

  /**
   * Index key for z3 values
   *
   * @param bin date epoch
   * @param z z3 value within the epoch
   */
  case class Z3IndexKey(bin: Short, z: Long) extends Ordered[Z3IndexKey] {
    override def compare(that: Z3IndexKey): Int = {
      val b = Ordering.Short.compare(bin, that.bin)
      if (b != 0) { b } else {
        Ordering.Long.compare(z, that.z)
      }
    }
  }

  /**
   * Index values extracted from a filter for z3 queries
   *
   * @param sfc specific curve being used
   * @param geometries extracted geometries
   * @param spatialBounds the spatial bounds from the extracted geometries, as bounding boxes
   * @param intervals extracted dates
   * @param temporalBounds the temporal bounds from the extracted dates, as time units (depending on the sfc),
   *                       keyed by epoch
   * @param temporalUnbounded unbounded temporal epochs, i.e. all time values are covered. will be either
   *                          `(0, t)`, `(t, Short.MaxValue)` or `(0, Short.MaxValue)` for upper, lower,
   *                          and unbounded queries, respectively
   */
  case class Z3IndexValues(
      sfc: Z3SFC,
      geometries: FilterValues[Geometry],
      spatialBounds: Seq[(Double, Double, Double, Double)],
      intervals: FilterValues[Bounds[ZonedDateTime]],
      temporalBounds: Map[Short, Seq[(Long, Long)]],
      temporalUnbounded: Seq[(Short, Short)]
    ) extends TemporalIndexValues with SpatialIndexValues

  /**
   * Index values extracted from a filter for xz3 queries
   *
   * @param sfc specific curve being used
   * @param geometries extracted geometries
   * @param spatialBounds the spatial bounds from the extracted geometries, as bounding boxes
   * @param intervals extracted dates
   * @param temporalBounds the temporal bounds from the extracted dates, as time units (depending on the sfc),
   *                       keyed by epoch
   * @param temporalUnbounded unbounded temporal epochs, i.e. all time values are covered. will be either
   *                          `(0, t)`, `(t, Short.MaxValue)` or `(0, Short.MaxValue)` for upper, lower,
   *                          and unbounded queries, respectively
   */
  case class XZ3IndexValues(
      sfc: XZ3SFC,
      geometries: FilterValues[Geometry],
      spatialBounds: Seq[(Double, Double, Double, Double)],
      intervals: FilterValues[Bounds[ZonedDateTime]],
      temporalBounds: Map[Short, (Double, Double)],
      temporalUnbounded: Seq[(Short, Short)]
    ) extends TemporalIndexValues with SpatialIndexValues
}
