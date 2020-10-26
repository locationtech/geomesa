/***********************************************************************
 * Copyright (c) 2013-2020 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0
 * which accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 ***********************************************************************/

package org.locationtech.geomesa.index

import java.time.ZonedDateTime

import org.locationtech.geomesa.filter.{Bounds, FilterValues}
import org.locationtech.geomesa.index.api.GeoMesaFeatureIndex
import org.opengis.feature.simple.SimpleFeatureType

package object index {
  /**
   * Marker trait for spatial indices
   */
  trait SpatialIndex //[T <: SpatialIndexValues, U] extends GeoMesaFeatureIndex[T, U]

  /**
   * Index values with a spatial component
   */
  trait SpatialIndexValues {
    def spatialBounds: Seq[(Double, Double, Double, Double)]
  }

  /**
   * Marker trait for temporal indices
   */
  trait TemporalIndex //[T <: TemporalIndexValues, U] extends GeoMesaFeatureIndex[T, U]

  /**
   * Index values with a temporal component
   */
  trait TemporalIndexValues {
    def intervals: FilterValues[Bounds[ZonedDateTime]]
  }

  /**
   * Marker trait for spatio-temporal indices
   */
  trait SpatioTemporalIndex //[T <: SpatialIndexValues with TemporalIndexValues, U]
      extends SpatialIndex /* [T, U] */ with TemporalIndex /* [T, U] */
}
